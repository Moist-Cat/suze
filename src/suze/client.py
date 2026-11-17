"""Synchronous CLI-side client for the daemon socket."""

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Optional

from suze.protocol import encode, decode, new_envelope, RemoteError


def default_socket() -> Path:
    if "SUZE_SOCKET" in os.environ:
        return Path(os.environ["SUZE_SOCKET"])
    dev = Path.cwd() / ".suze" / "suze.sock"
    if dev.exists():
        return dev
    return Path("/run/suze/suze.sock")


class Client:
    def __init__(self, socket_path: Optional[Path] = None, timeout: float = 10.0):
        self.socket_path = Path(socket_path) if socket_path else default_socket()
        self.timeout = timeout

    def call(self, cmd: str, **args) -> Any:
        return asyncio.run(self._call(cmd, args))

    async def _call(self, cmd: str, args: dict) -> Any:
        msg = new_envelope(cmd, args)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self.socket_path)),
                timeout=self.timeout,
            )
        except (FileNotFoundError, ConnectionRefusedError) as e:
            raise ConnectionError(
                f"cannot reach daemon at {self.socket_path} ({e}). "
                f"Try `systemctl status suze` or set $SUZE_SOCKET."
            ) from e

        try:
            writer.write(encode(msg))
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            if not line:
                raise ConnectionError("daemon closed connection without reply")
            resp = decode(line)
            if not resp.get("ok"):
                raise RemoteError(resp.get("error", "unknown error"))
            return resp.get("result")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
