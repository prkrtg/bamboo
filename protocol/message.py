from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import StrEnum

# Allowed message types
class MsgType(StrEnum):
    REQ        = "REQ"
    RESP       = "RESP"
    ACK        = "ACK"
    CONFIG     = "CONFIG"
    HELLO      = "HELLO"       
    PUB        = "PUB"         
    SUBSCRIBE  = "SUBSCRIBE" 

@dataclass(frozen=True)
class Envelope:
    """
    Envelope fields, 'payload' is JSON, optional 'binary' 
    """
    version: int                 # strictly incrementing protocol version
    type: MsgType                # GET | SET | REQ | RESP | ACK | CONFIG
    transid: str                 # transaction id (uuid string). Use the same transid for ACK/RESP
    key: Optional[str]           # keys topic/handler
    payload: Dict[str, Any]      # JSON object
    time: str                    # Sender's message time
    destid: Optional[str]        # UUID of destination peer. None => broadcast/publish
    sourceid: str                # UUID of source peer

@dataclass(frozen=True)
class Message:
    env: Envelope
    binary: Optional[bytes] = None   # raw bytes (e.g., blosc-compressed chunk)
