from abc import ABC, abstractmethod
import json
import operator
from pathlib import Path
import pickle
from typing import Optional, Any, Dict, Iterator
import os
import time
from threading import Lock
from functools import wraps

from suze.log import get_logger, logged
from suze.config import BASE_DIR

# OpenTelemetry imports
from opentelemetry import trace, metrics
from opentelemetry.trace import Status, StatusCode


@logged
class CacheBackend(ABC):
    """Abstract cache storage backend with logging and OpenTelemetry"""

    def __init__(
        self,
        default_ttl: int,
        max_size_bytes: int,
        enable_telemetry: bool = True,
    ):
        self.default_ttl = default_ttl
        self.max_size_bytes = max_size_bytes
        self._lock = Lock()

        self._tracer = trace.get_tracer(self.__class__.__name__)
        self._meter = metrics.get_meter(self.__class__.__name__)
        self._init_otel_metrics()

        metadata = self._load_metadata()
        self.access_count = metadata.get("access", {})
        self.key_ttl = metadata.get("ttl", {})

        self.logger.info(
            f"Initialized cache backend: {self.__class__.__name__}",
            extra={
                "cache.default_ttl": default_ttl,
                "cache.max_size": max_size_bytes,
            },
        )

    def _init_otel_metrics(self):
        """Initialize OpenTelemetry metrics"""
        # Cache operation counters
        self.cache_operations = self._meter.create_counter(
            name="cache.operations",
            description="Total number of cache operations",
            unit="1",
        )

        # Initialize operation counters
        self._operation_stats = {
            "hits": 0,
            "misses": 0,
            "expirations": 0,
            "evictions": 0,
            "sets": 0,
            "deletes": 0,
        }

    def _record_operation(
        self, operation: str, key: int = None, latency: float = None, attributes=None
    ):
        """Record cache operation metrics"""
        attributes = attributes or {}
        # Update operation stats
        if operation in self._operation_stats:
            self._operation_stats[operation] += 1

        # Record operation counter
        base_attrs = {
            "cache.backend": self.__class__.__name__,
            "cache.operation": operation,
        }
        if key is not None:
            base_attrs["cache.key"] = str(key)
        base_attrs.update(attributes)

        self.cache_operations.add(1, base_attrs)

    @abstractmethod
    def _load_metadata(self) -> dict:
        pass

    @abstractmethod
    def _save_metadata(self):
        pass

    @abstractmethod
    def _get(self, key: int) -> Optional[Any]:
        pass

    @abstractmethod
    def _set(self, key: int, value: Any):
        pass

    @abstractmethod
    def _delete(self, key: int):
        pass

    def get(self, key: int) -> Optional[Any]:
        start_time = time.time()

        with self._lock:
            metadata = self._load_metadata()
            self.access_count = metadata.get("access", {})
            self.key_ttl = metadata.get("ttl", {})

            if key not in self:
                self.logger.debug("Cache MISS (%s)", key)
                self._record_operation("misses", key, time.time() - start_time)
                return None

            if self.expire(key):
                self.logger.debug("Cache EXPIRED (%s)", key)
                self._record_operation("expirations", key, time.time() - start_time)
                return None

            if key not in self.access_count:
                self.logger.warning("Key without access count key=%s", key)
                self.access_count[key] = 0
            self.access_count[key] += 1
            self.logger.debug("Cache HIT (%s)", key)

            self._save_metadata()

            latency = time.time() - start_time
            self._record_operation("hits", key, latency)

            return self._get(key)

    def set(self, key: int, value: Any, ttl: Optional[float] = None):
        start_time = time.time()

        with self._lock:
            metadata = self._load_metadata()
            self.key_ttl = metadata.get("ttl", {})

            if self.total_size >= self.max_size_bytes:
                self.evict()
            if key in self:
                self.logger.warning("Double SET detected (%s)", key)
                return None

            actual_ttl = ttl if ttl is not None else self.default_ttl
            self.access_count[key] = 0
            self.key_ttl[key] = time.time() + actual_ttl

            self._save_metadata()
            self._set(key, value)

            latency = time.time() - start_time
            value_size = self._estimate_size(key)
            self._record_operation(
                "sets", key, latency, attributes={"cache.value_size": value_size}
            )

            self.logger.debug("Cache SET (%s)", key)

    def delete(self, key: int):
        start_time = time.time()

        if key not in self:
            self.logger.warning("Double DELETE detected (%s)", key)
            return

        # Safely remove from dictionaries
        self.access_count.pop(key, None)
        self.key_ttl.pop(key, None)
        self._delete(key)

        latency = time.time() - start_time
        self._record_operation("deletes", key, latency)

    @abstractmethod
    def exists(self, key: int) -> bool:
        pass

    def expire(self, key: int) -> bool:
        """Expire entries if TTL has passed"""
        start_time = time.time()

        now = time.time()
        ttl = self.key_ttl.get(key)
        if ttl is None:
            self.logger.warning("No TTL set for key=%s", key)
            self.key_ttl[key] = now + self.default_ttl
            self._save_metadata()
            self._record_operation("ttl_missing", key, time.time() - start_time)
            return False

        if now > ttl:
            self.delete(key)
            self._record_operation("expirations", key, time.time() - start_time)
            return True

        self._record_operation("ttl_check", key, time.time() - start_time)
        return False

    def evict(self):
        """Evict using LFU policy."""
        start_time = time.time()

        evict_count = max(1, len(self) // 2)
        self.logger.debug(f"Evicting {evict_count} entries from cache")

        entries_to_evict = sorted(
            self.access_count.items(), key=operator.itemgetter(1)
        )[:evict_count]

        evicted_keys = []
        for key, _ in entries_to_evict:
            self.logger.debug("Cache EVICT (%s)", key)
            self.delete(key)
            evicted_keys.append(key)

        latency = time.time() - start_time
        self._record_operation(
            "evictions",
            latency=latency,
            **{
                "cache.evicted_count": len(evicted_keys),
                "cache.evicted_keys": str(evicted_keys),
            },
        )

    @property
    def total_size(self) -> int:
        return sum(self._estimate_size(k) for k in list(self))

    def _estimate_size(self, key: int) -> int:
        """Roughly estimate the size of an object in bytes."""
        value = self._get(key)
        if value is None:
            return 0
        return value.__sizeof__()

    def __contains__(self, key: int) -> bool:
        if not self.exists(key):
            return False
        return True

    @abstractmethod
    def __iter__(self):
        pass

    def __len__(self) -> int:
        count = 0
        for _ in self:
            count += 1
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring"""
        return {
            "backend": self.__class__.__name__,
            "total_entries": len(self),
            "total_size_bytes": self.total_size,
            "max_size_bytes": self.max_size_bytes,
            "default_ttl": self.default_ttl,
            "operation_stats": self._operation_stats.copy(),
            "hit_ratio": (
                self._get_hit_ratio_callback().__next__().value
                if (self._operation_stats["hits"] + self._operation_stats["misses"]) > 0
                else 0.0
            ),
        }


@logged
class FileCache(CacheBackend):
    """File system based cache with TTL support and logging"""

    def __init__(
        self,
        cache_dir: Path = BASE_DIR / "cache",
        default_ttl: int = 30 * 24 * 3600,
        max_size_bytes: int = 10 * 1024 * 1024 * 1024,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.cache_dir / ".meta"

        super().__init__(
            default_ttl=default_ttl,
            max_size_bytes=max_size_bytes,
        )

    def _get_cache_path(self, key: int) -> Path:
        return self.cache_dir / f"{key}.cache"

    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata from the central metadata file."""
        try:
            with open(self.meta_path, "r", encoding="utf8") as f:
                data = json.load(f)
            access = {int(key): value for key, value in data["access"].items()}
            ttl = {int(key): value for key, value in data["ttl"].items()}
            return {"access": access, "ttl": ttl}
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning(f"Failed to load metadata: {e}")
        except FileNotFoundError:
            self.logger.error(f"No meta file {self.meta_path}")

        # Return empty metadata if file doesn't exist or can't be read
        return {"access": {}, "ttl": {}}

    def _save_metadata(self):
        """Save metadata to the central metadata file."""
        try:
            metadata = {"access": self.access_count, "ttl": self.key_ttl}
            with open(self.meta_path, "w", encoding="utf8") as f:
                json.dump(metadata, f)
        except OSError as e:
            self.logger.error(f"Failed to save metadata: {e}")

    def _get(self, key: int) -> Optional[Any]:
        """Internal method to get value from cache file."""
        cache_path = self._get_cache_path(key)

        try:
            with open(cache_path, "rb") as f:
                data = pickle.load(f)
                return data
        except (pickle.PickleError, OSError, FileNotFoundError) as e:
            self.logger.warning(f"Cache corruption detected for {key}: {e}")
            self._record_operation("corruption", key, attributes={"error": str(e)})
            self._delete_key(key)
            return None

    def _set(self, key: int, value: Any):
        """Internal method to set value to cache file."""
        cache_path = self._get_cache_path(key)

        self.logger.debug("Cache SET at %s. Value: %s", cache_path, str(value)[:100])

        try:
            data = pickle.dumps(value)
            with open(cache_path, "wb") as f:
                f.write(data)
            self._record_operation(
                "file_write", key, attributes={"file_size": len(data)}
            )
        except (pickle.PickleError, OSError) as e:
            self.logger.error(f"Failed to cache {key}: {e}")
            self._record_operation("write_error", key, attributes={"error": str(e)})
            self._delete(key)
            raise

    def _delete(self, key: int):
        """Helper to delete all files associated with a key."""
        cache_path = self._get_cache_path(key)

        try:
            os.remove(cache_path)
            self._record_operation("file_delete", key)
        except (OSError, FileNotFoundError) as e:
            self.logger.error(f"Failed to delete cache file for {key}: {e}")
            self._record_operation("delete_error", key, attributes={"error": str(e)})

    def exists(self, key: int) -> bool:
        """Check if key exists in cache (does not check TTL)."""
        cache_path = self._get_cache_path(key)
        return cache_path.exists()

    def __iter__(self) -> Iterator[int]:
        """Iterate over all cache keys."""
        for file_path in self.cache_dir.glob("*.cache"):
            try:
                key_str = file_path.stem
                key = int(key_str)
                yield key
            except ValueError:
                continue

    def _estimate_size(self, key: int) -> int:
        """Estimate size of cached item."""
        cache_path = self._get_cache_path(key)
        try:
            return os.path.getsize(cache_path)
        except (OSError, FileNotFoundError) as e:
            self.logger.error("Error getting filesize (%s)", str(cache_path))
            self._record_operation("size_error", key, attributes={"error": str(e)})
            return 0

    def _estimate_size(self, key: int) -> int:
        """Estimate size of cached item."""
        cache_path = self._get_cache_path(key)
        try:
            return os.path.getsize(cache_path)
        except (OSError, FileNotFoundError) as e:
            self.logger.error("Error getting filesize (%s)", str(cache_path))
            return 0
        return 0


@logged
class MemCache(CacheBackend):
    """
    In-memory cache backend for partial computations with TTL support and thread safety.
    Optimized for fast read/write operations and minimal overhead.
    """

    def __init__(
        self, default_ttl: float = 3600, max_size_bytes: int = 10 * 1024 * 1024 * 1024
    ):
        """
        Initialize in-memory cache.

        Args:
            default_ttl: Default time-to-live in seconds for cache entries
            max_size_bytes: Maximum size in bytes (converted from original max_size parameter)
        """
        super().__init__(
            default_ttl=int(default_ttl),
            max_size_bytes=max_size_bytes,
        )
        self._cache: Dict[int, Any] = {}

        self.logger.info(
            f"Initialized in-memory cache with TTL: {default_ttl}s, "
            f"max_size: {max_size_bytes} bytes"
        )

    def _load_metadata(self) -> Dict[str, Any]:
        """Memory cache doesn't persist metadata across sessions."""
        return {"access": {}, "ttl": {}}

    def _save_metadata(self):
        """Memory cache doesn't persist metadata."""
        pass

    def _get(self, key: int) -> Optional[Any]:
        """Internal method to get value from memory."""
        return self._cache.get(key)

    def _set(self, key: int, value: Any):
        """Internal method to set value in memory."""
        self._cache[key] = value

    def _delete(self, key: int):
        """Internal method to delete value from memory."""
        self._cache.pop(key, None)

    def exists(self, key: int) -> bool:
        """Check if key exists in cache (does not check TTL)."""
        return key in self._cache

    def __iter__(self) -> Iterator[int]:
        """Iterate over all cache keys."""
        return iter(self._cache.keys())

    def _estimate_size(self, key: int) -> int:
        """Estimate size of cached item in memory."""
        value = self._cache.get(key)
        if value is None:
            return 0
        return value.__sizeof__()
