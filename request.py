import asyncio
import uuid
from dataclasses import dataclass, field
import inspect
import hashlib
import json
import functools
from typing import Callable, Dict, Any, Optional, List

from suze.log import get_logger

request_logger = get_logger("request")


def get_function_identity(func):
    """Get a consistent identity for a function"""
    # Try to get a unique but consistent identifier
    identity_parts = []

    # For partials, include the base function and arguments
    if isinstance(func, functools.partial):
        identity_parts.append(f"partial({get_function_identity(func.func)})")
        if func.args:
            identity_parts.append(f"args:{repr(func.args)}")
        if func.keywords:
            keywords = {}
            for key, value in func.keywords.items():
                if isinstance(value, Callable):
                    value = get_function_identity(value)
                keywords[key] = value
            identity_parts.append(f"kwargs:{keywords}")
    else:
        identity_parts.append(f"signature:{func.__name__}")

    return "|".join(identity_parts)


class Encoder(json.JSONEncoder):
    """
    To encode other Python objects like functions or instances
    """

    def default(self, o):
        if isinstance(o, Callable):
            return get_function_identity(o)
        elif hasattr(o, "_use_hash"):  # user hints hash() is safe
            return hash(o)
        return str(o)


@dataclass
class DataRequest:
    """Represents a request for data from a source"""

    source: "DataSource" = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    # Stage overrides
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    dependency_requirements: List["DataRequest"] = field(default_factory=dict)
    cache_key: Optional[str] = None

    # per-request lock to use gather to gather (duh) dependencies
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    enable_telemetry: bool = False

    _use_hash = True

    # important for cache
    def __str__(self):
        return (
            f"<DataRequest(source={self.source.source_id if self.source else 'None'}, "
            f"request_id={self.request_id}, stages={list(self.stages.keys())}, "
            f"num_dependencies={len(self.dependency_requirements)}, "
        )

    def __hash__(self):
        """
        Hash functions are computed dynamically and not cached
        to allow state changes (e.g. from I/O) unless the user
        explicitly defines it.

        This doesn't include convenience transformations like str -> int.
        """
        # some cache keys need lazy computing (e.g. stat a file)
        # we have to re-compute every time
        if isinstance(self.cache_key, Callable):
            return self.cache_key()
        if isinstance(self.cache_key, str):
            self.cache_key = int(
                hashlib.md5(self.cache_key.encode()).hexdigest(), base=16
            )
        if isinstance(self.cache_key, list):
            self.cache_key = int(
                hashlib.md5(str(tuple(self.cache_key)).encode()).hexdigest(), base=16
            )

        if not self.cache_key:
            key_data = {
                "source": self.source.source_id,
                "stages": self.stages,
                "dependencies": sorted(
                    [hash(dep_req) for dep_req in self.dependency_requirements]
                ),
            }
            key = json.dumps(key_data, sort_keys=True, cls=Encoder).encode()
            if b"0x" in key:
                request_logger.warning(
                    "Cache key %s might be invalid (contains memory address)", key
                )
            return int(hashlib.md5(key).hexdigest(), base=16) % (2**64)
        return self.cache_key % (2**64)

    async def fetch(self):
        async with self.lock:
            return await self.source.fetch(self)
