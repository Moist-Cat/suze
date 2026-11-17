"""Per-event-loop single-flight map.

Two concurrent `fetch` calls for the same cache key share one execution:
the first to arrive starts a task, the rest await it. The map is keyed by
the running event loop so a task can never be observed from a different
loop (asyncio primitives are loop-bound, so that would be a bug anyway).
"""

import asyncio
from typing import Dict
from weakref import WeakKeyDictionary

_by_loop: WeakKeyDictionary = WeakKeyDictionary()


def get_inflight() -> Dict[int, asyncio.Task]:
    loop = asyncio.get_running_loop()
    inflight = _by_loop.get(loop)
    if inflight is None:
        inflight = {}
        _by_loop[loop] = inflight
    return inflight
