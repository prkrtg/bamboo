from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Set, Iterable, Optional, List
import time

@dataclass
class KeyRegistry:
    # keys we serve
    local: Set[str] = field(default_factory=set)
    # our capabilities (advertised)
    local_caps: Set[str] = field(default_factory=lambda: {"json"})
    # remote peer -> keys they serve
    remote: Dict[str, Set[str]] = field(default_factory=dict)

    # parallel metadata maps (flat)
    remote_caps: Dict[str, Set[str]] = field(default_factory=dict)   # peer -> caps
    remote_rev: Dict[str, int]       = field(default_factory=dict)   # peer -> revision
    last_seen: Dict[str, float]      = field(default_factory=dict)   # peer -> ts

    _rev: int = 0  # our local advertisement rev

    def add(self, *keys: str) -> None:
        self.local.update(keys); self._rev += 1
    def remove(self, *keys: str) -> None:
        for k in keys: self.local.discard(k); self._rev += 1
    def set_caps(self, *caps: str) -> None:
        self.local_caps = set(caps) if caps else set(); self._rev += 1

    def advertise(self) -> dict:
        return {
            "keys": sorted(self.local),
            "caps": sorted(self.local_caps),
            "rev": self._rev,
            "ts": time.time(),
        }

    def learn(
        self,
        peer_id: str,
        keys: Iterable[str],
        *,
        caps: Optional[Iterable[str]] = None,
        rev: Optional[int] = None,
        ts: Optional[float] = None,
        replace: bool = True,
    ) -> None:
        # ignore stale revs if provided
        if rev is not None and peer_id in self.remote_rev and rev < self.remote_rev[peer_id]:
            self.last_seen[peer_id] = ts or time.time()
            return

        new_keys = set(keys)
        if replace or peer_id not in self.remote:
            self.remote[peer_id] = new_keys
        else:
            self.remote[peer_id] |= new_keys

        if caps is not None:
            self.remote_caps[peer_id] = set(caps)
        if rev is not None:
            self.remote_rev[peer_id] = int(rev)

        self.last_seen[peer_id] = ts or time.time()

    def peer_supports(self, peer_id: str, key: str) -> bool:
        return key in self.remote.get(peer_id, set())

    def peers_supporting(self, key: str) -> Set[str]:
        return {pid for pid, ks in self.remote.items() if key in ks}

    def get_peer_caps(self, peer_id: str) -> Set[str]:
        return set(self.remote_caps.get(peer_id, set()))

    def prune(self, stale_after_s: float = 60.0) -> List[str]:
        cutoff = time.time() - max(0.0, stale_after_s)
        removed: List[str] = []
        for pid, ts in list(self.last_seen.items()):
            if ts and ts < cutoff:
                removed.append(pid)
                self.last_seen.pop(pid, None)
                self.remote.pop(pid, None)
                self.remote_caps.pop(pid, None)
                self.remote_rev.pop(pid, None)
        return removed
