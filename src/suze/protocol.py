"""Wire protocol for the daemon <-> CLI socket."""

import json
import uuid
from typing import Any, Optional

PROTOCOL_VERSION = 1


def new_envelope(cmd: str, args: Optional[dict] = None) -> dict:
    return {"id": str(uuid.uuid4()), "protocol": PROTOCOL_VERSION,
            "cmd": cmd, "args": args or {}}


def encode(msg: dict) -> bytes:
    return (json.dumps(msg, default=str) + "\n").encode()


def decode(line: bytes) -> dict:
    return json.loads(line.decode())


class ProtocolError(Exception):
    pass


class RemoteError(Exception):
    """Raised on the client when the daemon returns ok=false."""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
