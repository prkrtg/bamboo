
from __future__ import annotations
from typing import Any, Dict, Protocol as TypingProtocol

import json

class Codec(TypingProtocol):
    name: str
    def dumps(self, obj: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...

class JSONCodec:
    name = "json"
    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")
    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

try:
    import msgpack
    class MsgPackCodec:
        name = "msgpack"
        def dumps(self, obj: Any) -> bytes:
            return msgpack.packb(obj, use_bin_type=True)
        def loads(self, data: bytes) -> Any:
            return msgpack.unpackb(data, raw=False)
except Exception:
    MsgPackCodec = None

class Codecs:
    _registry: Dict[str, Codec] = {"json": JSONCodec()}
    if MsgPackCodec:
        _registry["msgpack"] = MsgPackCodec()

    @classmethod
    def get(cls, name: str) -> 'Codec':
        if name not in cls._registry:
            raise ValueError(f"Unknown codec: {name}")
        return cls._registry[name]
