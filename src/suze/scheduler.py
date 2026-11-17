# suze/scheduler.py
import asyncio
import heapq
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional

try:
    from croniter import croniter
except ImportError:
    croniter = None

from suze.manage import PipelineManager, PipelineStatus
from suze.log import get_logger

logger = get_logger("scheduler")


class Trigger(Enum):
    TTL = "ttl"           # refresh when cache TTL is within threshold
    INTERVAL = "interval"
    CRON = "cron"


@dataclass(order=True)
class Entry:
    next_check: float                       # monotonic; heap key
    pipeline_id: str = field(compare=False)
    factory: Callable[[], Any] = field(compare=False)  # () -> DataRequest
    trigger: Trigger = field(compare=False, default=Trigger.TTL)
    interval: Optional[float] = field(compare=False, default=None)
    cron_expr: Optional[str] = field(compare=False, default=None)
    ttl_threshold: float = field(compare=False, default=60.0)
    cost: int = field(compare=False, default=1)
    enabled: bool = field(compare=False, default=True)
    last_run: Optional[float] = field(compare=False, default=None)
    last_status: Optional[str] = field(compare=False, default=None)


class CreditPool:
    """Cost-weighted semaphore. `cost` slots per pipeline, `total` overall.

    Pipelines with cost > total are clamped to total so a single heavy
    pipeline can run alone but never deadlocks.
    """
    def __init__(self, total: int):
        self.total = total
        self.available = total
        self._cond = asyncio.Condition()

    async def acquire(self, cost: int, timeout: Optional[float] = None) -> int:
        cost = min(cost, self.total)
        async with self._cond:
            if timeout is None:
                while self.available < cost:
                    await self._cond.wait()
            else:
                deadline = time.monotonic() + timeout
                while self.available < cost:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise asyncio.TimeoutError(f"credit acquire({cost}) timed out")
                    try:
                        await asyncio.wait_for(self._cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        raise
            self.available -= cost
            return cost

    async def release(self, cost: int):
        async with self._cond:
            self.available += cost
            self._cond.notify_all()


class Scheduler:
    def __init__(self, manager: PipelineManager, total_credits: int = 4):
        self.manager = manager
        self.pool = CreditPool(total_credits)
        self._heap: list[Entry] = []
        self._entries: dict[str, Entry] = {}
        self._active: dict[str, asyncio.Task] = {}
        self._wakeup = asyncio.Event()
        self._stopped = False
        self._task: Optional[asyncio.Task] = None

    # ---------- registration ----------

    def add(
        self,
        pipeline_id: str,
        factory: Callable[[], Any],
        *,
        trigger: Trigger = Trigger.TTL,
        interval: Optional[float] = None,
        cron: Optional[str] = None,
        ttl_threshold: float = 60.0,
        cost: int = 1,
        first_run_at: Optional[float] = None,
    ) -> Entry:
        if trigger == Trigger.CRON and croniter is None:
            raise RuntimeError("cron trigger requires `croniter`")
        entry = Entry(
            next_check=first_run_at if first_run_at is not None else time.monotonic(),
            pipeline_id=pipeline_id,
            factory=factory,
            trigger=trigger,
            interval=interval,
            cron_expr=cron,
            ttl_threshold=ttl_threshold,
            cost=cost,
        )
        self._entries[pipeline_id] = entry
        heapq.heappush(self._heap, entry)
        self._wakeup.set()
        return entry

    def remove(self, pipeline_id: str):
        self._entries.pop(pipeline_id, None)
        if pipeline_id in self._active:
            self._active[pipeline_id].cancel()

    # ---------- lifecycle ----------

    async def start(self):
        if self._task is not None:
            raise RuntimeError("scheduler already started")
        self._stopped = False
        self._task = asyncio.create_task(self._loop(), name="Scheduler")

    async def stop(self, *, cancel_active: bool = True):
        self._stopped = True
        self._wakeup.set()
        if cancel_active:
            for task in self._active.values():
                task.cancel()
        if self._task is not None:
            await self._task
            self._task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()

    # ---------- main loop ----------

    async def _loop(self):
        while not self._stopped:
            if not self._heap:
                self._wakeup.clear()
                try:
                    await asyncio.wait_for(self._wakeup.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            now = time.monotonic()
            entry = self._heap[0]
            if entry.next_check <= now:
                heapq.heappop(self._heap)
                if not entry.enabled:
                    continue
                # fire and forget — _dispatch does the credit-gated work
                asyncio.create_task(self._dispatch(entry))
                continue

            wait = entry.next_check - now
            self._wakeup.clear()
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    async def _dispatch(self, entry: Entry):
        # 1. Should we run at all? For TTL entries, check the cache first.
        if entry.trigger == Trigger.TTL:
            request = entry.factory()
            if request.source and request.source.cache.exists(hash(request)):
                ttl = _remaining_ttl(request.source.cache, hash(request))
                if ttl is not None and ttl > entry.ttl_threshold:
                    self._reschedule(entry, ran=False)
                    return

        # 2. Acquire credit (may block; that's backpressure, not failure).
        try:
            acquired = await self.pool.acquire(entry.cost)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            logger.warning(f"Scheduler: credit timeout for {entry.pipeline_id}")
            self._reschedule(entry, ran=False)
            return

        # 3. Run the pipeline.
        try:
            request = entry.factory()
            pid = self.manager.register_pipeline(request, name=entry.pipeline_id)
            task = self.manager.start_pipeline(pid)
            self._active[entry.pipeline_id] = task
            entry.last_run = time.time()
            try:
                await task
                entry.last_status = self.manager.pipelines[pid].status.value
            except asyncio.CancelledError:
                entry.last_status = "cancelled"
                raise
            except Exception as e:
                entry.last_status = "failed"
                logger.error(f"Scheduler: pipeline {entry.pipeline_id} raised: {e}")
        finally:
            self._active.pop(entry.pipeline_id, None)
            await self.pool.release(acquired)
            self._reschedule(entry, ran=True)

    def _reschedule(self, entry: Entry, *, ran: bool):
        if self._stopped:
            return
        now = time.monotonic()
        if entry.trigger == Trigger.CRON:
            if croniter is None:
                return
            base = datetime.now()
            entry.next_check = now + (
                croniter(entry.cron_expr, base).get_next(datetime) - base
            ).total_seconds()
        elif entry.trigger == Trigger.INTERVAL:
            interval = entry.interval or 60.0
            entry.next_check = now + interval
        else:  # TTL
            # Re-check periodically; the cache TTL is the real trigger.
            entry.next_check = now + max(entry.ttl_threshold / 2, 10.0)
        heapq.heappush(self._heap, entry)
        self._wakeup.set()


def _remaining_ttl(cache, key) -> Optional[float]:
    """Best-effort. Override per cache backend if TTL isn't exposed."""
    if hasattr(cache, "ttl_remaining"):
        return cache.ttl_remaining(key)
    return None
