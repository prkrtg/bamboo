
from __future__ import annotations
import json, time, uuid, threading
from queue import Queue
from typing import Any, Callable, Dict, Optional

from .envelope import Envelope
from .codecs import Codecs, JSONCodec, Codec

Handler = Callable[[Envelope], Any]

class ProtocolRuntime:
    def __init__(self, transport, node_id: Optional[str] = None, *, codec: Codec = JSONCodec()):
        self.transport = transport
        self.node_id = node_id or transport.peer_id
        self.handlers: Dict[str, Handler] = {}
        self.pending: Dict[str, Queue] = {}
        self.running = False
        self.rx_thread: Optional[threading.Thread] = None
        self.codec = codec  # controls wire format

    def on(self, key: str, handler: Handler) -> None:
        self.handlers[key] = handler

    # ---- serialization ----
    def _serialize(self, env: Envelope) -> bytes:
        return self.codec.dumps(env.__dict__)

    def _deserialize(self, data: bytes) -> Envelope:
        obj = self.codec.loads(data)
        return Envelope(**obj)

    def start(self):
        if self.running:
            return
        self.running = True
        self.rx_thread = threading.Thread(target=self._rx_loop, daemon=True)
        self.rx_thread.start()

    def stop(self):
        self.running = False
        if self.rx_thread:
            self.rx_thread.join(timeout=1)

    # ---- API ----
    def request(self, dest_peer: str, key: str, body: Any, *, timeout_s: float = 8.0) -> Envelope:
        req = Envelope.new(src=self.node_id, dest=dest_peer, kind="req", key=key, body=body)
        reply_q = Queue(maxsize=1)
        self.pending[req.id] = reply_q
        self.transport.send(req.dest, self._serialize(req), {"kind": "req"})
        try:
            resp: Envelope = reply_q.get(timeout=timeout_s)
            return resp
        finally:
            self.pending.pop(req.id, None)

    def respond(self, req: Envelope, body: Any) -> None:
        resp = Envelope.new(src=self.node_id, dest=req.src, kind="resp", key=req.key, body=body, corr=req.id)
        self.transport.send(resp.dest, self._serialize(resp), {"kind": "resp"})

    def publish(self, topic: str, key: str, body: Any) -> None:
        env = Envelope.new(src=self.node_id, dest=topic, kind="pub", key=key, body=body)
        self.transport.send(env.dest, self._serialize(env), {"kind": "pub"})

    # ---- RX loop ----
    def _rx_loop(self):
        while self.running:
            pkt = self.transport.recv(timeout=0.1)
            if pkt is None:
                continue
            src, payload, headers = pkt
            try:
                env = self._deserialize(payload)
            except Exception:
                continue

            if env.kind == "resp" and env.corr and env.corr in self.pending:
                self.pending[env.corr].put(env)
                continue

            if env.kind == "req":
                handler = self.handlers.get(env.key)
                if not handler:
                    self.respond(env, {"ok": False, "error": "no_route", "key": env.key})
                    continue
                try:
                    result = handler(env)
                except Exception as e:
                    self.respond(env, {"ok": False, "error": repr(e)})
                else:
                    self.respond(env, {"ok": True, "data": result})

            if env.kind == "pub":
                handler = self.handlers.get(env.key)
                if handler:
                    try:
                        handler(env)
                    except Exception:
                        pass
