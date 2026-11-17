import asyncio
import uuid
from dataclasses import dataclass, field
import inspect
import hashlib
import json
import functools
from typing import Any, Callable, Dict, Optional, List

from suze.stages import StageConfig, build_stage_config
from suze.log import get_logger

request_logger = get_logger("request")

def dummy_key():
    """
    Dummy key that always invalidates cache.
    """
    return str(uuid.uuid4())

def _types_compatible(out_t: Optional[type], in_t: Optional[type]) -> bool:
    """Fail only on clear mismatches; allow Any/object through."""
    if out_t is None or in_t is None:
        return True
    if out_t is Any or in_t is Any:
        return True
    if out_t is object or in_t is object:
        return True
    try:
        return issubclass(out_t, in_t)
    except TypeError:
        return True


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
        # hashing the code is an alternative
        # but it could easily fail silently
        # a value that changes randomly
        # will create a hard-to-catch bug
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
        candidate = str(o)
        if "at 0x" in candidate:
            raise Exception(
                f"{candidate} is part of a cache key and it contains a memory address."
                "This effectively invalidates the cache every time the code runs."
                "Either explicitly set a `dummy_key` or define the object's __str__ method"
                "with a valid signature."
            )
        return candidate



@dataclass
class DataRequest:
    source: "DataSource" = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    stages: Dict[str, StageConfig] = field(default_factory=dict)
    dependency_requirements: List["DataRequest"] = field(default_factory=list)
    cache_key: Optional[str] = None
    enable_telemetry: bool = False
    _use_hash = True

    def __post_init__(self):
        if self.stages:
            self.stages = {
                k: build_stage_config(k, v) if isinstance(v, dict) else v
                for k, v in self.stages.items()
            }

    # ---- validation against a source ----
    def validate(self) -> None:
        """Check order, strategy, params, and type chain vs. the source."""
        if self.source is None or not self.stages:
            return
        source = self.source
        names = list(self.stages.keys())

        # 1. order: request names must be a subsequence of source.stages
        it = iter(source.stages)
        n_set = set(source.stages)
        for name in names:
            if name not in n_set:
                continue
            if not any(s == name for s in it):
                raise ValueError(
                    f"Stage '{name}' is not in source order {source.stages} "
                    f"(or request stages {names} are reordered)"
                )

        # 2. strategy + parameter schema
        for name in names:
            if name not in n_set:
                continue
            cfg = self.stages[name]
            strategy = cfg.strategy or "default"
            spec = source.regs[name].spec(strategy)
            if spec is None:
                continue
                raise ValueError(
                    f"({self.request_id}) Unknown strategy '{strategy}' for stage '{name}' "
                    f"in source {source.source_id}"
                )
            if spec.params_model is not None:
                validated = spec.params_model(**cfg.parameters)
                cfg.parameters = validated.model_dump()

        # 3. type chain for consecutive present stages
        for prev, curr in zip(names, names[1:]):
            if prev not in n_set or curr not in n_set:
                continue
            prev_spec = source.regs[prev].spec(self.stages[prev].strategy or "default")
            curr_spec = source.regs[curr].spec(self.stages[curr].strategy or "default")
            if prev_spec is None or curr_spec is None:
                continue
            if not _types_compatible(prev_spec.output_type, curr_spec.input_type):
                raise TypeError(
                    f"Stage '{prev}' outputs {prev_spec.output_type.__name__}, "
                    f"but stage '{curr}' expects {curr_spec.input_type.__name__}"
                )

    # important for cache
    def __str__(self):
        return (
            f"<DataRequest(source={self.source.source_id if self.source else 'None'}, "
            f"request_id={self.request_id}, stages={list(self.stages.keys())}, "
            f"num_dependencies={len(self.dependency_requirements)}"
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
                "stages": {k: v.model_dump() for k, v in self.stages.items()},
                "dependencies": sorted(
                    [hash(dep_req) for dep_req in self.dependency_requirements]
                ),
            }
            key = json.dumps(key_data, sort_keys=True, cls=Encoder).encode()
            return int(hashlib.md5(key).hexdigest(), base=16) % (2**64)
        return self.cache_key % (2**64)

    async def fetch(self):
        return await self.source.fetch(self)

    __repr__ = __str__
