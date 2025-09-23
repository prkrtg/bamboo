from __future__ import annotations
import json
from .message import Envelope, Message, MsgType

_SEP = b"\n\n"

def pack_frame(msg: Message) -> bytes:
    env_dict = {
        "version":  msg.env.version,
        "type":     str(msg.env.type),
        "transid":  msg.env.transid,
        "key":      msg.env.key,
        "payload":  msg.env.payload,
        "time":     msg.env.time,
        "destid":   msg.env.destid,
        "sourceid": msg.env.sourceid,
    }
    header = json.dumps(env_dict, separators=(",", ":")).encode("utf-8")
    if msg.binary is None:
        return header
    return header + _SEP + msg.binary

def unpack_frame(frame: bytes) -> Message:
    try:
        header_bytes, binary = frame.split(_SEP, 1)
    except ValueError:
        header_bytes, binary = frame, None

    env = json.loads(header_bytes.decode("utf-8"))
    envelope = Envelope(
        version=env["version"],
        type=MsgType(env["type"]),
        transid=env["transid"],
        key=env.get("key"),
        payload=env.get("payload", {}),
        time=env["time"],
        destid=env.get("destid"),
        sourceid=env["sourceid"],
    )
    return Message(envelope, binary)
