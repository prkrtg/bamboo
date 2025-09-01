
from __future__ import annotations
class ZmqTransport:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("ZmqTransport is not implemented in this MVP. Use 'inmemory' or implement proto.transports.zmq.ZmqTransport.")
