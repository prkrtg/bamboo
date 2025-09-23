"""
Public API:
- Protocol: core runtime (send once, wait for ACK; optional RESP for REQ)
- MessageBuilder: fluent builder that conforms to the bamboo envelope
- Envelope, Message: wire-level types (JSON header + optional binary)
- Transport: abstract class transports must implement
- KeyRegistry: track local/remote keys
- Discovery, PeerTable: optional, for transports without native discovery
- pack_frame, unpack_frame: simple framing (JSON + b"\\n\\n" + optional binary)
"""

# Core runtime
from .protocol import Protocol

# Builder & wire types
from .builder import MessageBuilder
from .message import (
    Envelope,
    Message,
    MsgType,
)

# Transport contract
from .transport import Transport

# Framing helpers
from .wire import pack_frame, unpack_frame

# Keys & discovery
from .keys import KeyRegistry
from .discovery import Discovery, PeerTable

__all__ = [
    "Protocol",
    "MessageBuilder",
    "Envelope",
    "Message",
    "MsgType",
    "Transport",
    "KeyRegistry",
    "Discovery",
    "PeerTable",
    "pack_frame",
    "unpack_frame",
]

# Optional version string; keep simple if vendored
__version__ = "0.1.0"
