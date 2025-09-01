
from __future__ import annotations
from typing import Any, Iterable, Mapping, Optional, Union, Callable

from .runtime import ProtocolRuntime
from .codecs import Codecs
from .transport import Transport

def Bamboo(peer_id: str,
          *,
          transport: Union[str, Transport] = "inmemory",
          codec: Union[str, Any] = "json",
          keys: Optional[Union[Iterable[str], Mapping[str, Callable]]] = None,
          auto_start: bool = True,
          **transport_kwargs) -> ProtocolRuntime:
    """
    One-liner factory:
      Bamboo("PeerA", transport="zyre", keys=["camera.scan","camera.off"], codec="json")
      Bamboo("PeerB", transport=my_transport_instance, codec=my_codec_instance, keys={"camera.scan": handler})

    - peer_id: node identifier
    - transport: "inmemory" | "zmq" | "zyre" | Transport instance
    - codec: "json" | "msgpack" | Codec instance
    - keys: sequence of route keys (registered with unimplemented stubs) or dict key->handler
    - auto_start: start the runtime receive loop immediately
    - **transport_kwargs: passed to transport constructor
    """
    # Resolve codec
    if isinstance(codec, str):
        codec_obj = Codecs.get(codec)
    else:
        codec_obj = codec

    # Resolve transport
    if isinstance(transport, str):
        tlabel = transport.lower()
        if tlabel == "zyre":
            from .transports.zyre import ZyreTransport
            t = ZyreTransport(peer_id=peer_id, **transport_kwargs)
        elif tlabel == "zmq":
            from .transports.zmq import ZmqTransport
            t = ZmqTransport(peer_id=peer_id, **transport_kwargs)
        else:
            raise ValueError(f"Unknown transport label: {transport}")
    else:
        t = transport
        # Trust caller to have set peer_id inside transport if needed

    rt = ProtocolRuntime(t, node_id=peer_id, codec=codec_obj)

    # Register keys
    if keys:
        if isinstance(keys, dict):
            for k, handler in keys.items():
                rt.on(k, handler)
        else:
            # Assume iterable of strings -> stub handlers
            for k in keys:
                def _stub(env, _k=k):
                    return {"ok": False, "error": "unimplemented", "key": _k}
                rt.on(k, _stub)

    if auto_start:
        rt.start()

    return rt
