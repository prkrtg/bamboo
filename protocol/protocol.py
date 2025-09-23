from __future__ import annotations
from typing import Dict, Optional, Callable, List, Set
from datetime import datetime, timezone
import queue, threading

from .transport import Transport
from .message import Message, Envelope, MsgType
from .wire import pack_frame, unpack_frame
from .keys import KeyRegistry
from .discovery import PeerTable, DISCOVERY_TOPIC
from .builder import MessageBuilder

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Protocol:
    
    # Notes:
    # - Send once; wait up to ttl for ACK (direct messages only)
    # - Keys gate direct messages (destid != None). If unknown/invalid -> receiver stays silent
    # - Broadcast = destid None (publish); no ACKs for broadcast
    # - For REQ, after ACK we briefly wait for RESP
    # Designed to be transport-agnostic; the Transport just moves frames

    def __init__(self, transport: Transport, self_id: str,
                 keys: Optional[KeyRegistry] = None):
        self.t = transport
        self.self_id = self_id
        self.keys = keys or KeyRegistry()

        # Handlers
        self._req_handlers: Dict[str, Callable[[Message], Optional[dict]]] = {}
        self._evt_handlers: Dict[str, Callable[[Message], None]] = {}

        # Waiters
        self._ack_wait: Dict[str, "queue.Queue[Message]"] = {}
        self._resp_wait: Dict[str, "queue.Queue[Message]"] = {}
        self._lock = threading.Lock()

        # Subscriptions
        self._local_subs: Set[str] = set()
        self._remote_subs: Dict[str, Set[str]] = {}

        self.t.on_receive(self._on_frame)

    def on(self, key: str, handler: Callable[[Message], Optional[dict]]) -> None:
        """Register a raw-message handler for a key (also advertises the key locally)."""
        self.keys.local.add(key)
        self._req_handlers[key] = handler

    def listen(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Listen for PUB on a topic; also auto-subscribe."""
        self._evt_handlers[topic] = handler
        if topic not in self._local_subs:
            self._local_subs.add(topic)
            self._send_subscribe(add=[topic])

    def send(self, msg: Message) -> None:
        """Send a message without waiting (direct or broadcast)."""
        dest = f"peer:{msg.env.destid}" if msg.env.destid else "broadcast:*"
        self.t.send(dest, pack_frame(msg))

    def request(self, msg: Message) -> dict:
        """
        For direct messages, wait for ACK and if REQ  wait for RESP
        For broadcast, just send.

        Returns:
          {'status': 'sent'|'delivered'|'timeout'|'no_key' ,
           'transid': <id>,
           'resp': {...}|None (only for REQ)}
        """
        # Broadcast path (no ACKs)
        if msg.env.destid is None:
            self.send(msg)
            return {"status": "sent", "transid": msg.env.transid}

        # Sender-side key check (if known)
        if msg.env.key and not self.keys.peer_supports(msg.env.destid, msg.env.key):
            return {"status": "no_key", "transid": msg.env.transid}

        qa: "queue.Queue[Message]" = queue.Queue()
        qr: "queue.Queue[Message]" = queue.Queue()
        with self._lock:
            self._ack_wait[msg.env.transid] = qa
            self._resp_wait[msg.env.transid] = qr

        self.send(msg)

        ttl = msg.env.payload.get("ttl_ms", None)
        wait_seconds = (ttl / 1000.0) if isinstance(ttl, (int, float)) else 8.0

        try:
            try:
                _ = qa.get(timeout=wait_seconds)
            except queue.Empty:
                return {"status": "timeout", "transid": msg.env.transid}

            if msg.env.type == MsgType.REQ:
                try:
                    resp_msg = qr.get(timeout=max(0.0, wait_seconds / 2.0))
                    return {"status": "delivered", "transid": msg.env.transid, "resp": resp_msg.env.payload}
                except queue.Empty:
                    return {"status": "delivered", "transid": msg.env.transid, "resp": None}

            # CONFIG direct: delivered once ACKed
            return {"status": "delivered", "transid": msg.env.transid}
        finally:
            with self._lock:
                self._ack_wait.pop(msg.env.transid, None)
                self._resp_wait.pop(msg.env.transid, None)

    def _on_frame(self, source: str, frame: bytes) -> None:
        msg = unpack_frame(frame)
        e = msg.env

        # Route ACK/RESP
        if e.type == MsgType.ACK:
            with self._lock:
                q = self._ack_wait.get(e.transid)
            if q: q.put(msg)
            return

        if e.type == MsgType.RESP:
            with self._lock:
                q = self._resp_wait.get(e.transid)
            if q: q.put(msg)
            return

        # HELLO (discovery): update peers+keys+subs (no ACK if broadcast)
        if e.type == MsgType.HELLO:
            p = e.payload or {}
            self.keys.learn(
                e.sourceid,
                p.get("keys", []),
                caps=p.get("caps"),
                rev=p.get("rev"),
                ts=p.get("ts", time.time()),
            )
            subs = set(p.get("subs", []))
            if subs:
                self._remote_subs[e.sourceid] = subs
            return

        # SUBSCRIBE: update remote subscription table; ACK if direct
        if e.type == MsgType.SUBSCRIBE:
            p = e.payload or {}
            add = set(p.get("add", []))
            remove = set(p.get("remove", []))
            current = self._remote_subs.setdefault(e.sourceid, set())
            current |= add
            current -= remove
            if e.destid is not None:     # direct subscription command
                self._fast_ack(e)
            return

        # Keys-gated direct messages (only REQ/CONFIG). PUB is not key-gated.
        if e.destid is not None and e.type in (MsgType.REQ, MsgType.CONFIG):
            if not e.key or e.key not in self.keys.local:
                return  # silent drop
            self._fast_ack(e)

        # Dispatch:
        if e.type == MsgType.REQ:
            handler = self._req_handlers.get(e.key or "")
            if handler:
                try:
                    result = handler(msg)
                    if isinstance(result, dict) and ("ok" in result or "error" in result):
                        resp_payload = result
                    else:
                        resp_payload = {"ok": True, "data": result}
                except Exception as ex:
                    resp_payload = {"ok": False, "error": str(ex)}

                noresp = bool(msg.env.payload.get("noresp") or msg.env.payload.get("_noresp"))
                if not noresp:
                    resp = Message(
                        env=Envelope(
                            version=e.version,
                            type=MsgType.RESP,
                            transid=e.transid,
                            key=e.key,
                            payload=resp_payload,
                            time=_iso_now(),
                            destid=e.sourceid,
                            sourceid=self.self_id,
                        ),
                        binary=None,
                    )
                    self.send(resp)
            return

        if e.type == MsgType.CONFIG and e.destid is not None:
            handler = self._req_handlers.get(e.key or "")
            if handler:
                try: _ = handler(msg)
                except Exception: pass
            return

        # PUB: deliver to evt handler if present (ignore if none)
        if e.type == MsgType.PUB:
            h = self._evt_handlers.get(e.key or "")
            if h:
                try: h(msg)
                except Exception: pass
            return

    def _fast_ack(self, e: Envelope) -> None:
        """Emit a fast ACK confirming message was recieved."""
        ack = Message(
            env=Envelope(
                version=e.version,
                type=MsgType.ACK,
                transid=e.transid,
                key="sys.ack",
                payload={"ack": e.transid},
                time=_iso_now(),
                destid=e.sourceid,
                sourceid=self.self_id,
            ),
            binary=None,
        )
        self.send(ack)

    def serve(self, key: str, callback: Callable[[dict, dict], Optional[dict]]) -> None:
        """
        Register a handler that receives (payload: dict, ctx: dict) and returns dict | None.
        ctx = {"key","source","transid","time","binary","raw_msg"}
        """
        def _wrap(msg: Message):
            payload = msg.env.payload
            ctx = {
                "key": msg.env.key,
                "source": msg.env.sourceid,
                "transid": msg.env.transid,
                "time": msg.env.time,
                "binary": None,
                "raw_msg": msg,
            }
            try:
                return callback(payload, ctx)
            except Exception as ex:
                return {"ok": False, "error": str(ex)}
        self.on(key, _wrap)

    def publish(self, topic: str, payload: dict) -> int:
        """
        Publish an event on `topic`.
        If we have known subscribers, fan out direct PUB to them (fast-ACKs happen, but we don't wait).
        Otherwise, broadcast a single PUB.
        Returns the number of direct recipients (0 means broadcast fallback).
        """
        subs = [pid for pid, topics in self._remote_subs.items() if topic in topics]
        if subs:
            for pid in subs:
                msg = (MessageBuilder(self.self_id).pub(topic, payload).to(pid).build())
                self.send(msg)  # no waiting
            return len(subs)
        # fallback broadcast
        msg = (MessageBuilder(self.self_id).pub(topic, payload).to(None).build())
        self.send(msg)
        return 0

    def subscribe_topics(self, add: List[str] = None, remove: List[str] = None) -> None:
        add = add or []
        remove = remove or []
        changed = False
        for t in add:
            if t not in self._local_subs:
                self._local_subs.add(t); changed = True
        for t in remove:
            if t in self._local_subs:
                self._local_subs.remove(t); changed = True
        if changed:
            self._send_subscribe(add=add, remove=remove)

    def announce_hello(self) -> None:
        """Broadcast a HELLO (caps, keys, subs, rev, ts)."""
        payload = self.keys.advertise()
        msg = (MessageBuilder(self.self_id)
               .hello(caps=self.keys.local_caps, keys=payload.get("keys", []),
                      subs=self._local_subs, rev=payload.get("rev", 0),
                      ts=payload.get("ts", time.time()))
               .to(None)
               .build())
        self.send(msg)   

    def request_peer(self, peer_id: str, key: str, payload: dict, timeout_s: float = 8.0) -> dict:
        """Build+send a keyed REQ to a peer"""
        msg = (
            MessageBuilder(sourceid=self.self_id)
            .req(key, payload)
            .to(peer_id)
            .build()
        )
        return self._request_with_timeout(msg, timeout_s)
    
    def publish_group(self, topic: str, payload: dict) -> None:
        """Broadcast a topic to everyone; no ACKs."""
        msg = (
            MessageBuilder(sourceid=self.self_id)
            .set(topic, payload)   # topic as key
            .to(None)              # broadcast
            .build()
        )
        self.send(msg)

    def learn_peer_keys(self, peer_id: str, keys: List[str]) -> None:
        """Teach the local registry which keys a remote peer supports (if not using discovery)."""
        self.keys.learn(peer_id, keys)

    def _request_with_timeout(self, msg: Message, timeout_s: float) -> dict:
        """Same as request(), but with an explicit timeout window in seconds."""
        # Optional MTU guard
        try:
            frame = pack_frame(msg)
            mtu = getattr(self.t, "mtu", None)
            if mtu is not None and len(frame) > int(mtu):
                return {
                    "status": "too_large",
                    "transid": msg.env.transid,
                    "mtu": int(mtu),
                    "size": len(frame),
                }
        except Exception:
            # TODO
            pass

        qa: "queue.Queue[Message]" = queue.Queue()
        qr: "queue.Queue[Message]" = queue.Queue()
        with self._lock:
            self._ack_wait[msg.env.transid] = qa
            self._resp_wait[msg.env.transid] = qr

        self.send(msg)

        try:
            # ACK wait
            try:
                _ = qa.get(timeout=timeout_s)
            except queue.Empty:
                return {"status": "timeout", "transid": msg.env.transid}

            # If REQ, wait briefly for RESP
            if msg.env.type == MsgType.REQ:
                try:
                    resp = qr.get(timeout=max(0.0, timeout_s / 2.0))
                    return {"status": "delivered", "transid": msg.env.transid, "resp": resp.env.payload}
                except queue.Empty:
                    return {"status": "delivered", "transid": msg.env.transid, "resp": None}

            # For direct GET/SET/CONFIG we’re done once ACKed
            return {"status": "delivered", "transid": msg.env.transid}

        finally:
            with self._lock:
                self._ack_wait.pop(msg.env.transid, None)
                self._resp_wait.pop(msg.env.transid, None)

    def _send_subscribe(self, *, add: List[str] = (), remove: List[str] = ()):
        msg = (MessageBuilder(self.self_id)
               .subscribe(add=add, remove=remove)
               .to(None)  # broadcast our (un)subscriptions
               .build())
        self.send(msg)
