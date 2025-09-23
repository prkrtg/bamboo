from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict
import time, threading

from .builder import MessageBuilder
from .keys import KeyRegistry

DISCOVERY_TOPIC = "bamboo.discovery"
KEYS_TOPIC = "bamboo.keys"

@dataclass
class PeerTable:
    last_seen: Dict[str, float] = field(default_factory=dict)
    def touch(self, peer_id: str) -> None:
        self.last_seen[peer_id] = time.time()
    def alive(self, within: int = 30) -> Dict[str, float]:
        now = time.time()
        return {p: t for p, t in self.last_seen.items() if now - t < within}

class Discovery:
    """
    Periodically broadcast hello + keys via broadcast REQ (noresp),
    so receivers update PeerTable and KeyRegistry.
    """

    def __init__(self, send_msg, self_id: str, keys: KeyRegistry, every_seconds: int = 5):
        """
        send_msg: callable(Message) -> None (typically Protocol.send)
        """
        self._send = send_msg
        self._self_id = self_id
        self._keys = keys
        self._every = max(1, int(every_seconds))
        self._stop = False
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self._stop = False
        if not self._thread.is_alive():
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def announce_now(self) -> None:
        """Fire one hello+keys immediately."""
        self._send(self._hello_msg())
        self._send(self._keys_msg())

    def _loop(self) -> None:
        # Send an immediate hello to speed up first contact
        self.announce_now()
        while not self._stop:
            time.sleep(self._every)
            self._send(self._hello_msg())
            self._send(self._keys_msg())

    def _hello_msg(self):
        payload = {"peer": self._self_id, "caps": sorted(self._keys.local_caps), "v": 1, "ts": time.time(), "noresp": True}
        return (MessageBuilder(self._self_id)
                .req(DISCOVERY_TOPIC, payload)
                .to(None)  # broadcast
                .build())

    def _keys_msg(self):
        payload = dict(self._keys.advertise())
        payload["noresp"] = True
        return (MessageBuilder(self._self_id)
                .req(KEYS_TOPIC, payload)
                .to(None)  # broadcast
                .build())
