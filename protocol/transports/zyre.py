
from __future__ import annotations
import json
import threading
from queue import Queue, Empty
from typing import Dict, Optional, Tuple

try:
    from zyre import Zyre, ZyreEvent
except Exception as e:
    raise RuntimeError("Zyre Python bindings are required. Error: %r" % (e,))

class ZyreTransport:
    """Transport over Zyre.

    Mapping:
    - req/resp -> WHISPER to a peer (by *name* or *uuid*). Zyre maintains name<->uuid mapping from ENTER events.
    - pub -> SHOUT to a group named by the topic. Use .subscribe(topic) to JOIN the group.

    Frames on the wire (Zmsg):
    [0] JSON-encoded headers
    [1] payload bytes

    """

    def __init__(self, peer_id: Optional[str] = None, **kwargs):
        self.peer_id = peer_id or ""
        self.node = Zyre()

        if self.peer_id:

            try:
                self.node.set_name(self.peer_id)

            except Exception:
                pass

        self.node.start()
        self._inbox: Queue = Queue()
        self._running = True
        self._rx_thread = threading.Thread(target=self._rx_loop, daemon=True)
        self._rx_thread.start()

        self._peers_by_uuid: Dict[str, str] = {}
        self._uuid_by_name: Dict[str, str] = {}

    def subscribe(self, topic: str):
        try:
            self.node.join(topic)
        except Exception:
            pass

    def leave(self, topic: str):
        try:
            self.node.leave(topic)
        except Exception:
            pass

    def send(self, dest: str, payload: bytes, headers: Dict[str, object]) -> None:
        kind = headers.get("kind") if isinstance(headers, dict) else None
        hdr = headers if isinstance(headers, dict) else {}
        header_bytes = json.dumps(hdr, separators=(",", ":")).encode("utf-8")

        frames = [header_bytes, payload]

        if kind == "pub":
            # dest is a topic (group)
            self.node.shout(dest, frames)
            return

        # req/resp -> WHISPER to a peer
        uuid = self._uuid_by_name.get(dest, dest)
        self.node.whisper(uuid, frames)

    def recv(self, timeout: Optional[float] = None) -> Optional[Tuple[str, bytes, Dict[str, object]]]:
        try:
            return self._inbox.get(timeout=timeout)
        except Empty:
            return None

    def _rx_loop(self):
        while self._running:
            try:
                event = ZyreEvent(self.node)
            except Exception:
                continue
            if not event:
                continue
            etype = event.type()
            if isinstance(etype, bytes):
                etype = etype.decode()

            if etype == "ENTER":
                try:
                    uuid = event.peer_uuid()
                    name = event.peer_name()
                    if isinstance(uuid, bytes): uuid = uuid.decode()
                    if isinstance(name, bytes): name = name.decode()
                    self._peers_by_uuid[uuid] = name
                    self._uuid_by_name[name] = uuid
                except Exception:
                    pass
                continue

            if etype in ("EXIT", "LEAVE"):
                try:
                    uuid = event.peer_uuid()
                    if isinstance(uuid, bytes): uuid = uuid.decode()
                    name = self._peers_by_uuid.pop(uuid, None)
                    if name:
                        self._uuid_by_name.pop(name, None)
                except Exception:
                    pass
                continue

            if etype in ("WHISPER", "SHOUT"):
                try:
                    uuid = event.peer_uuid()
                    if isinstance(uuid, bytes): uuid = uuid.decode()
                    src_name = self._peers_by_uuid.get(uuid, uuid)
                    zmsg = event.msg()

                    headers, payload = self._parse_frames(zmsg)
                    self._inbox.put((src_name, payload, headers))
                except Exception:
                    # swallow malformed frames
                    pass
                continue

    def _parse_frames(self, zmsg):
        # Attempt to read frames from Zmsg
        frames = []
        try:
            # Some bindings expose iteration or popmem
            while True:
                try:
                    data = zmsg.popmem()
                except AttributeError:
                    try:
                        data = zmsg.pop()
                    except Exception:
                        data = None
                if not data:
                    break
                if not isinstance(data, (bytes, bytearray)):
                    try:
                        data = bytes(data)
                    except Exception:
                        try:
                            data = str(data).encode("utf-8")
                        except Exception:
                            data = b""
                frames.append(data)
        except Exception:
            pass
        headers = {}
        payload = b""
        if frames:
            try:
                headers = json.loads(frames[0].decode("utf-8"))
            except Exception:
                headers = {}
        if len(frames) >= 2:
            payload = frames[1]
        elif frames:
            payload = frames[0]
        return headers, payload

    def close(self):
        self._running = False
        try:
            self.node.stop()
        except Exception:
            pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass