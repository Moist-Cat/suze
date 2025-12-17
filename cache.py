from abc import ABC, abstractmethod
import hashlib
import json
from pathlib import Path
import pickle
from typing import Optional, Any, Dict
import os
import time
from threading import Lock

from suze.log import log_cache_operations, get_logger
from suze.config import BASE_DIR


class CacheBackend(ABC):
    """Abstract cache storage backend with logging"""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    def __contains__(self, key: str) -> bool:
        """Support 'in' operator for cache existence checks."""
        return self.exists(key)


class FileCache(CacheBackend):
    """File system based cache with TTL support and logging"""

    def __init__(
        self, cache_dir: Path = BASE_DIR / "cache", default_ttl: float = 30 * 24 * 3600
    ):
        self.cache_dir = Path(cache_dir)
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True)
        self.default_ttl = default_ttl
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("cache.filesystem")

        self.logger.info(f"Initialized filesystem cache at {cache_dir}")

    def _get_cache_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.cache"

    @log_cache_operations
    def get(self, key: str) -> Optional[Any]:
        cache_path = self._get_cache_path(key)
        meta_path = cache_path.with_suffix(".meta")

        if key not in self:
            return None

        try:
            # Check TTL
            if os.path.exists(meta_path):
                with open(meta_path, "r") as f:
                    meta = json.loads(f.read())
                    expires_at = meta.get("expires_at", float("inf"))

                    if time.time() > expires_at:
                        self.logger.debug(f"Cache entry expired for {key}...")
                        self._delete_key(key)
                        return None

            with open(cache_path, "rb") as f:
                data = pickle.loads(f.read())
                self.logger.debug(
                    f"Cache hit for {key}..., size: {len(pickle.dumps(data))} bytes"
                )
                return data

        except (pickle.PickleError, json.JSONDecodeError, OSError) as e:
            self.logger.warning(
                f"Cache corruption detected for {key}..., deleting: {e}"
            )
            self._delete_key(key)
            return None

    @log_cache_operations
    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        # we allow to cache empty values (e.g. "[]") because if we segment
        # the pipeline for partial cache, some results might yield empty sequences
        cache_path = self._get_cache_path(key)
        meta_path = cache_path.with_suffix(".meta")

        ttl = ttl or self.default_ttl
        meta = {"created_at": time.time(), "expires_at": time.time() + ttl, "key": key}

        try:
            with open(cache_path, "wb") as f:
                f.write(pickle.dumps(value))
            with open(meta_path, "w") as f:
                f.write(json.dumps(meta))

            data_size = len(pickle.dumps(value))
            self.logger.debug(
                f"Cache set for {key}..., size: {data_size} bytes, TTL: {ttl}s"
            )

        except (pickle.PickleError, OSError) as e:
            self.logger.error(f"Failed to cache {key}...: {e}")
            self._delete_key(key)
            raise

    @log_cache_operations
    def exists(self, key: str) -> bool:
        cache_path = self._get_cache_path(key)
        if not os.path.exists(cache_path):
            return False
        return True

    def _delete_key(self, key: str):
        cache_path = self._get_cache_path(key)
        meta_path = cache_path.with_suffix(".meta")

        deleted_count = 0
        for path in [cache_path, meta_path]:
            if os.path.exists(path):
                os.remove(path)
                deleted_count += 1

        if deleted_count > 0:
            self.logger.debug(f"Deleted {deleted_count} cache files for {key}...")


class MemCache(CacheBackend):
    """
    In-memory cache backend for partial computations with TTL support and thread safety.
    Optimized for fast read/write operations and minimal overhead.
    """

    def __init__(
        self, default_ttl: float = 3600, max_size: Optional[int] = 1024 * 1024 * 1024
    ):
        """
        Initialize in-memory cache.

        Args:
            default_ttl: Default time-to-live in seconds for cache entries
            max_size: Maximum number of entries to store (None for unlimited)
        """
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._cache: Dict[str, Any] = {}
        self._metadata: Dict[str, Dict[str, float]] = (
            {}
        )  # key -> {created_at, expires_at}
        self._lock = Lock()  # For thread safety
        self._access_count: Dict[str, int] = {}  # For potential LRU eviction
        self.logger = get_logger("cache.memory")

        self.logger.info(
            f"Initialized in-memory cache with TTL: {default_ttl}s, "
            f"max_size: {max_size or 'unlimited'}"
        )

    @log_cache_operations
    def get(self, key: int) -> Optional[Any]:
        """Retrieve value from cache if it exists and hasn't expired."""

        with self._lock:
            # Check if key exists and hasn't expired
            if key not in self._cache:
                return None

            # Check TTL
            meta = self._metadata.get(key, {})
            expires_at = meta.get("expires_at", float("inf"))

            if time.time() > expires_at:
                self.logger.debug(f"Cache entry expired for {key}...")
                self._delete_key(key)
                return None

            # Update access count for LRU tracking
            self._access_count[key] = self._access_count.get(key, 0) + 1

            value = self._cache[key]
            self.logger.debug(
                f"Memory cache hit for {key}..., "
                f"size: {self._estimate_size(value)} bytes"
            )
            return value

    @log_cache_operations
    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        """Store value in cache with optional TTL."""
        ttl = ttl or self.default_ttl

        with self._lock:
            # Check if we need to evict due to size limits
            size_estimate = self._estimate_size(value)
            if self.max_size and size_estimate >= self.max_size:
                self._evict_if_needed()

            # Store the value
            self._cache[key] = value
            self._metadata[key] = {
                "created_at": time.time(),
                "expires_at": time.time() + ttl,
                "original_key": key,
            }
            self._access_count[key] = 0

            self.logger.debug(
                f"Memory cache set for {key}..., "
                f"size: ~{size_estimate} bytes, TTL: {ttl}s, "
                f"total entries: {len(self._cache)}"
            )

    @log_cache_operations
    def exists(self, key: str) -> bool:
        """Check if key exists in cache and hasn't expired."""
        with self._lock:
            if key not in self._cache:
                return False

            # Check expiration
            meta = self._metadata.get(key, {})
            expires_at = meta.get("expires_at", float("inf"))

            if time.time() > expires_at:
                self._delete_key(key)
                return False

            return True

    def _delete_key(self, key: str):
        """Delete a key from all internal dictionaries."""
        for dict_ref in [self._cache, self._metadata, self._access_count]:
            dict_ref.pop(key, None)

    def _evict(self):
        """Evict entries if cache exceeds max_size using LRU policy."""
        # Calculate how many entries to evict (evict 10% or at least 1)
        evict_count = max(1, len(self._cache) // 10)
        self.logger.debug(f"Evicting {evict_count} entries from memory cache")

        # Use LRU policy: remove least frequently accessed entries
        entries_to_evict = sorted(self._access_count.items(), key=lambda x: x[1])[
            :evict_count
        ]

        for key, _ in entries_to_evict:
            original_key = self._metadata.get(key, {}).get("original_key", "unknown")
            self.logger.debug(f"Evicting {original_key}... from memory cache")
            self._delete_key(key)

    def _estimate_size(self, obj: Any) -> int:
        """Roughly estimate the size of an object in bytes."""
        return obj.__sizeof__()

    # Additional utility methods for memory cache management

    def clear(self):
        """Clear all entries from the cache."""
        with self._lock:
            entry_count = len(self._cache)
            self._cache.clear()
            self._metadata.clear()
            self._access_count.clear()
            self.logger.info(f"Cleared all {entry_count} entries from memory cache")

    def cleanup_expired(self) -> int:
        """Remove all expired entries and return count cleaned."""
        with self._lock:
            current_time = time.time()
            expired_keys = [
                key
                for key, meta in self._metadata.items()
                if current_time > meta.get("expires_at", float("inf"))
            ]

            for key in expired_keys:
                self._delete_key(key)

            if expired_keys:
                self.logger.debug(
                    f"Cleaned up {len(expired_keys)} expired memory cache entries"
                )

            return len(expired_keys)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_size = sum(self._estimate_size(v) for v in self._cache.values())
            avg_ttl_remaining = (
                sum(
                    meta.get("expires_at", 0) - time.time()
                    for meta in self._metadata.values()
                )
                / len(self._metadata)
                if self._metadata
                else 0
            )

            return {
                "entries": len(self._cache),
                "total_size_bytes": total_size,
                "max_size": self.max_size,
                "avg_ttl_remaining": avg_ttl_remaining,
                "most_accessed": sorted(
                    self._access_count.items(), key=lambda x: x[1], reverse=True
                )[
                    :5
                ],  # Top 5 most accessed
            }
