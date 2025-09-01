
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
import uuid

@dataclass
class Envelope:
    id: str           # unique message id
    src: str          # sender peer id
    dest: str         # destination (peer id for req/resp, topic for pub)
    kind: str         # "req" | "resp" | "pub"
    key: str          # route key, e.g. "camera.focus"
    body: Any         # freeform payload
    corr: Optional[str] = None  # correlation id for responses

    @staticmethod
    def new(src: str, dest: str, kind: str, key: str, body: Any, corr: Optional[str] = None) -> "Envelope":
        return Envelope(
            id=str(uuid.uuid4()),
            src=src,
            dest=dest,
            kind=kind,
            key=key,
            body=body,
            corr=corr,
        )
