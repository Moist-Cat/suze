from dataclasses import dataclass, field
import inspect
import hashlib
import json
import functools
from typing import Callable, Dict, Any, Optional

from suze.log import root_logger


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
            identity_parts.append(f"kwargs:{repr(func.keywords)}")

    # For lambdas and other callables, try to get source
    else:
        code = inspect.getsource(func)
        code_hash = int(hashlib.md5(code.encode()).hexdigest(), base=16)
        identity_parts.append(f"code:{code_hash}")

    identity_parts.append(f"signature:{repr(inspect.signature(func))}")

    return "|".join(identity_parts)


class Encoder(json.JSONEncoder):
    """
    To encode other Python objects like functions or instances
    """

    def default(self, obj):
        if isinstance(obj, Callable):
            return get_function_identity(obj)
        return str(obj)


@dataclass
class DataRequest:
    """Represents a request for data from a source"""

    source: "DataSource"
    request_id: str = None
    # Stage overrides
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    dependency_requirements: Dict[str, "DataRequest"] = field(default_factory=dict)
    # evaluated dependencies
    # injected
    dependencies: Dict[str, Any] = field(default_factory=dict)
    cache_key: Optional[str] = None

    def __hash__(self):
        if not self.cache_key:
            key_data = {
                "source": self.source.source_id,
                "stages": self.stages,
                "dependencies": sorted(
                    [hash(dep_req) for dep_req in self.dependency_requirements]
                ),
            }
            self.cache_key = json.dumps(key_data, sort_keys=True, cls=Encoder).encode()
        else:
            if isinstance(self.cache_key, str):
                self.cache_key = self.cache_key.encode()

        root_logger.info("Cache key for %s: %s", self.request_id, self.cache_key)

        return int(hashlib.md5(self.cache_key).hexdigest(), base=16)

    async def fetch(self):
        return await self.source.fetch(self)
