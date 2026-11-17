"""Suze daemon: owns PipelineManager, Scheduler, and the admin socket.

Run modes:
  - `suze daemon --config a.toml`  (dev; foreground)
  - systemd `Type=notify` with $NOTIFY_SOCKET (prod; watchdog enabled)

Lifecycle:
  startup -> import factories -> bind socket -> start scheduler -> notify READY
  run     -> await stop_event; serve CLI commands concurrently
  shutdown-> stop scheduler -> cancel active -> close socket -> exit
"""

import asyncio
import errno
import fcntl
import json
import os
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    import tomllib  # py3.11+
except ImportError:
    import tomli as tomllib  # type: ignore

from suze.manage import PipelineManager, PipelineStatus
from suze.scheduler import Scheduler, Trigger
from suze.request import DataRequest
from suze.protocol import encode, decode, new_envelope, PROTOCOL_VERSION
from suze.log import get_logger

logger = get_logger("daemon")


# ---------- config ----------

@dataclass
class DaemonConfig:
    socket: Path
    pidfile: Path
    socket_mode: int
    credits: int
    max_retained_pipelines: int
    report_interval: int
    pipelines: List["PipelineEntry"]


@dataclass
class PipelineEntry:
    id: str
    factory: Callable[[], DataRequest]
    trigger: Trigger
    interval: Optional[float] = None
    cron: Optional[str] = None
    ttl_threshold: float = 60.0
    cost: int = 1
    timeout_seconds: Optional[float] = None
    max_wait: Optional[float] = None


def _resolve_factory(spec: str) -> Callable[[], DataRequest]:
    if ":" not in spec:
        raise ValueError(f"factory {spec!r} must be 'module:callable'")
    mod_name, fn_name = spec.split(":", 1)
    import importlib
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name)
    if not callable(fn):
        raise TypeError(f"{spec} is not callable")
    return fn


def load_config(paths: List[Path]) -> DaemonConfig:
    merged: dict = {"pipelines": []}
    seen_ids: set = set()
    for p in paths:
        with p.open("rb") as f:
            data = tomllib.load(f)
        for key, val in data.items():
            if key == "pipelines":
                for entry in val:
                    if entry["id"] in seen_ids:
                        raise ValueError(f"duplicate pipeline id: {entry['id']}")
                    seen_ids.add(entry["id"])
                    merged["pipelines"].append(entry)
            else:
                merged[key] = {**merged.get(key, {}), **val}

    daemon = merged.get("daemon", {})
    socket_path = Path(os.environ.get("SUZE_SOCKET",
                                     daemon.get("socket", "/tmp/suze.sock")))
    pidfile = Path(os.environ.get("SUZE_PIDFILE",
                                  daemon.get("pidfile", str(socket_path) + ".pid")))

    entries: List[PipelineEntry] = []
    for raw in merged["pipelines"]:
        trigger = Trigger(raw.get("trigger", "ttl"))
        if trigger == Trigger.CRON:
            try:
                import croniter  # noqa: F401
            except ImportError:
                raise RuntimeError(
                    f"pipeline {raw['id']} uses cron but `croniter` is not installed"
                )
        entries.append(PipelineEntry(
            id=raw["id"],
            factory=_resolve_factory(raw["factory"]),
            trigger=trigger,
            interval=raw.get("interval"),
            cron=raw.get("cron"),
            ttl_threshold=float(raw.get("ttl_threshold", 60.0)),
            cost=int(raw.get("cost", 1)),
            timeout_seconds=raw.get("timeout_seconds"),
            max_wait=raw.get("max_wait"),
        ))

    return DaemonConfig(
        socket=socket_path,
        pidfile=pidfile,
        socket_mode=int(daemon.get("socket_mode", "0600"), 8),
        credits=int(daemon.get("credits", 4)),
        max_retained_pipelines=int(daemon.get("max_retained_pipelines", 100)),
        report_interval=int(daemon.get("report_interval", 0)),
        pipelines=entries,
    )


# ---------- sd_notify shim ----------

def _sd_notify(state: str) -> bool:
    """Notify systemd. Returns True if a notification was delivered."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False
    if addr[0] == "@":
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.sendall(state.encode())
        return True
    except OSError:
        return False


# ---------- pidfile ----------

class PidFile:
    def __init__(self, path: Path):
        self.path = path
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                raise RuntimeError(
                    f"another daemon holds {self.path} (pid "
                    f"{os.read(self._fd, 32).decode().strip()})"
                )
            raise
        os.ftruncate(self._fd, 0)
        os.write(self._fd, f"{os.getpid()}\n".encode())

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            finally:
                self._fd = None


# ---------- the daemon ----------

class Daemon:
    def __init__(self, cfg: DaemonConfig):
        self.cfg = cfg
        self.manager = PipelineManager()
        self.scheduler = Scheduler(self.manager, total_credits=cfg.credits)
        self.stop_event = asyncio.Event()
        self._server: Optional[asyncio.AbstractServer] = None
        self._pidfile = PidFile(cfg.pidfile)
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._report_task: Optional[asyncio.Task] = None
        self._entry_by_id: Dict[str, PipelineEntry] = {}

    # ----- lifecycle -----

    async def serve(self) -> None:
        self._pidfile.acquire()
        try:
            self._install_signal_handlers()
            await self._start_socket()
            self._register_pipelines()
            await self.scheduler.start()
            self._start_heartbeat()
            self._start_report_loop()
            _sd_notify("READY=1")
            logger.info(
                "suze daemon ready: %d pipelines, %d credits, socket=%s",
                len(self.cfg.pipelines), self.cfg.credits, self.cfg.socket,
            )
            await self.stop_event.wait()
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        logger.info("shutting down")
        _sd_notify("STOPPING=1")
        if self._report_task:
            self._report_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        await self.scheduler.stop()
        # Cancel still-running pipelines
        for pid, task in list(self.scheduler._active.items()):
            if not task.done():
                try:
                    await self.manager.cancel_pipeline(pid, "daemon shutdown")
                except Exception as e:
                    logger.warning("cancel %s failed: %s", pid, e)
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        try:
            self.cfg.socket.unlink(missing_ok=True)
        except OSError:
            pass
        self._pidfile.release()
        logger.info("shutdown complete")

    # ----- signals -----

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_stop_signal, sig)
        loop.add_signal_handler(signal.SIGUSR1, self._on_report_signal)

    def _on_stop_signal(self, sig: signal.Signals) -> None:
        logger.info("received %s; stopping", sig.name)
        self.stop_event.set()

    def _on_report_signal(self) -> None:
        self._dump_report(reason="SIGUSR1")

    # ----- socket -----

    async def _start_socket(self) -> None:
        path = self.cfg.socket
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if await self._probe_socket(path):
                raise RuntimeError(f"another daemon is listening on {path}")
            path.unlink(missing_ok=True)
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(path)
        )
        os.chmod(path, self.cfg.socket_mode)

    @staticmethod
    async def _probe_socket(path: Path) -> bool:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(path)), timeout=0.5
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (ConnectionRefusedError, FileNotFoundError, OSError, asyncio.TimeoutError):
            return False

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not line:
                return
            msg = decode(line)
            response = await self._dispatch(msg)
            writer.write(encode(response))
            await writer.drain()
        except asyncio.TimeoutError:
            writer.write(encode({"ok": False, "protocol": PROTOCOL_VERSION,
                                 "error": "read timeout"}))
        except Exception as e:
            logger.exception("client handler failed")
            try:
                writer.write(encode({"ok": False, "protocol": PROTOCOL_VERSION,
                                     "error": str(e)}))
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ----- command dispatch -----

    async def _dispatch(self, msg: dict) -> dict:
        cmd = msg.get("cmd", "")
        args = msg.get("args", {})
        env = {"id": msg.get("id"), "protocol": PROTOCOL_VERSION}
        handler = getattr(self, f"_cmd_{cmd}", None)
        if handler is None:
            return {**env, "ok": False, "error": f"unknown cmd: {cmd}"}
        try:
            result = await handler(**args)
            return {**env, "ok": True, "result": result}
        except Exception as e:
            logger.warning("cmd %s failed: %s", cmd, e)
            return {**env, "ok": False, "error": str(e)}

    async def _cmd_ping(self) -> dict:
        return {"status": "ok", "pid": os.getpid(), "uptime": time.time() - self._start_time}

    async def _cmd_list(self) -> dict:
        return {
            "pipelines": self._pipeline_summaries(),
            "scheduler": self._scheduler_summaries(),
            "credits": {
                "total": self.scheduler.pool.total,
                "available": self.scheduler.pool.available,
            },
        }

    async def _cmd_status(self, pipeline_id: str) -> dict:
        if pipeline_id in self.manager.pipelines:
            return self.manager.get_pipeline_status(pipeline_id)
        if pipeline_id in self._entry_by_id:
            return self._scheduler_summaries()[pipeline_id]
        raise KeyError(f"no pipeline or entry: {pipeline_id}")

    async def _cmd_view(self, pipeline_id: str, format: str = "mermaid") -> dict:
        from suze.view import render
        state = self.manager.pipelines.get(pipeline_id)
        if state is None:
            raise KeyError(f"no pipeline: {pipeline_id}")
        request = self.manager.request_map.get(state.tail_request_id)
        if request is None:
            raise KeyError(f"request for {pipeline_id} not available")
        out = render(request, format=format)
        return {"format": format, "content": str(out)}

    async def _cmd_pause(self, pipeline_id: str) -> dict:
        self.manager.pause_pipeline(pipeline_id)
        return {"paused": pipeline_id}

    async def _cmd_resume(self, pipeline_id: str) -> dict:
        self.manager.resume_pipeline(pipeline_id)
        return {"resumed": pipeline_id}

    async def _cmd_cancel(self, pipeline_id: str, reason: str = "cli") -> dict:
        await self.manager.cancel_pipeline(pipeline_id, reason)
        return {"cancelled": pipeline_id}

    async def _cmd_trigger(self, pipeline_id: str) -> dict:
        entry = self._entry_by_id.get(pipeline_id)
        if entry is None:
            raise KeyError(f"no scheduler entry: {pipeline_id}")
        await self._run_entry(entry, manual=True)
        return {"triggered": pipeline_id,
                "status": entry.id and self._scheduler_summaries()[pipeline_id]}

    async def _cmd_enable(self, pipeline_id: str) -> dict:
        self.scheduler._entries[pipeline_id].enabled = True
        return {"enabled": pipeline_id}

    async def _cmd_disable(self, pipeline_id: str) -> dict:
        self.scheduler._entries[pipeline_id].enabled = False
        return {"disabled": pipeline_id}

    async def _cmd_report(self) -> dict:
        return self._build_report()

    async def _cmd_shutdown(self) -> dict:
        asyncio.get_running_loop().call_later(0.2, self.stop_event.set)
        return {"shutting_down": True}

    # ----- scheduler integration -----

    def _register_pipelines(self) -> None:
        for entry in self.cfg.pipelines:
            self.scheduler.add(
                pipeline_id=entry.id,
                factory=entry.factory,
                trigger=entry.trigger,
                interval=entry.interval,
                cron=entry.cron,
                ttl_threshold=entry.ttl_threshold,
                cost=entry.cost,
            )
            self._entry_by_id[entry.id] = entry
        # Hook per-entry timeouts by wrapping the scheduler's dispatch.
        orig_dispatch = self.scheduler._dispatch

        async def dispatch(entry):
            timeout = self._entry_by_id[entry.pipeline_id].timeout_seconds
            if timeout is None:
                return await orig_dispatch(entry)
            try:
                await asyncio.wait_for(orig_dispatch(entry), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "pipeline %s exceeded %ss; cancelling", entry.pipeline_id, timeout
                )
                await self.manager.cancel_pipeline(
                    entry.pipeline_id, f"timeout {timeout}s"
                )
                self.scheduler._reschedule(entry, ran=False)

        self.scheduler._dispatch = dispatch

    async def _run_entry(self, entry: PipelineEntry, manual: bool = False) -> None:
        # Used by `trigger`; the scheduler uses its own dispatch path.
        acquired = await self.scheduler.pool.acquire(entry.cost)
        try:
            request = entry.factory()
            if not isinstance(request, DataRequest):
                raise TypeError(
                    f"factory for {entry.id} returned {type(request).__name__}"
                )
            pid = self.manager.register_pipeline(request, name=entry.id)
            await self.manager.start_pipeline(pid)
        finally:
            await self.scheduler.pool.release(acquired)

    # ----- reporting -----

    def _pipeline_summaries(self) -> dict:
        out = {}
        for pid, state in self.manager.pipelines.items():
            out[pid] = {
                "status": state.status.value,
                "started_at": state.started_at.isoformat() if state.started_at else None,
                "completed_at": state.completed_at.isoformat() if state.completed_at else None,
                "total": state.total_nodes,
                "completed": state.completed_nodes,
                "failed": state.failed_nodes,
                "cancelled": state.cancelled_nodes,
                "error": state.error,
            }
        return out

    def _scheduler_summaries(self) -> dict:
        out = {}
        for pid, entry in self.scheduler._entries.items():
            out[pid] = {
                "trigger": entry.trigger.value,
                "enabled": entry.enabled,
                "cost": entry.cost,
                "next_check": entry.next_check,
                "last_run": entry.last_run,
                "last_status": entry.last_status,
                "interval": entry.interval,
                "cron": entry.cron_expr,
                "ttl_threshold": entry.ttl_threshold,
            }
        return out

    def _build_report(self) -> dict:
        return {
            "timestamp": datetime.now().isoformat(),
            "pid": os.getpid(),
            "uptime": time.time() - self._start_time,
            "credits": {
                "total": self.scheduler.pool.total,
                "available": self.scheduler.pool.available,
                "active": list(self.scheduler._active.keys()),
            },
            "pipelines": self._pipeline_summaries(),
            "scheduler": self._scheduler_summaries(),
        }

    def _dump_report(self, reason: str) -> None:
        try:
            logger.info("report (%s): %s", reason, json.dumps(self._build_report()))
        except Exception:
            logger.exception("report dump failed")

    def _start_report_loop(self) -> None:
        if self.cfg.report_interval <= 0:
            return

        async def loop():
            while True:
                await asyncio.sleep(self.cfg.report_interval)
                self._dump_report("interval")

        self._report_task = asyncio.create_task(loop(), name="Report")

    def _start_heartbeat(self) -> None:
        interval = int(os.environ.get("WATCHDOG_USEC", "0")) / 1_000_000 / 3
        if interval <= 0:
            return

        async def beat():
            while True:
                _sd_notify("WATCHDOG=1")
                await asyncio.sleep(interval)

        self._heartbeat_task = asyncio.create_task(beat(), name="Heartbeat")


# ---------- entry point ----------

async def _amain(cfg: DaemonConfig) -> int:
    daemon = Daemon(cfg)
    daemon._start_time = time.time()
    try:
        await daemon.serve()
        return 0
    except Exception as e:
        logger.error("daemon failed: %s", e)
        _sd_notify(f"STATUS=fatal: {e}")
        return 1


def main(config_paths: List[str]) -> int:
    paths = [Path(p) for p in config_paths]
    try:
        cfg = load_config(paths)
    except Exception as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    try:
        return asyncio.run(_amain(cfg))
    except KeyboardInterrupt:
        return 0
