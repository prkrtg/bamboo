
from __future__ import annotations
from typing import Optional, Tuple, Dict, Protocol

class Transport(Protocol):
    peer_id: str
    def send(self, dest: str, payload: bytes, headers: Dict[str, object]) -> None: ...
    def recv(self, timeout: Optional[float] = None) -> Optional[Tuple[str, bytes, Dict[str, object]]]: ...
