from abc import ABC
from typing import Any, Dict, List, Optional, Union, Callable
from pathlib import Path
import aiohttp
import asyncio
import time

from suze.cache import CacheBackend, FileCache, MemCache
from suze.search import (
    binary_search,
    linear_search,
    html_search,
)
from suze.loaders import (
    merge_loader,
    split_loader,
    text_loader,
    json_loader,
    tsv_loader,
    csv_loader,
    log_loader,
    http_loader,
)
from suze.transform import drop_transform, rename_transform
from suze.rate import WindowRateLimiter
from suze.registries import Registry
from suze.log import log_execution_time, get_logger
from suze.config import CACHE_DIR
from suze.request import DataRequest


class DataSource(ABC):
    """Abstract base class for all data sources with comprehensive logging"""

    # global cache for everyone to handle memory fingerprint
    cache = MemCache()

    def __init__(
        self,
        source_id: str,
        cache: Optional[CacheBackend] = None,
        rate_limiter=None,
    ):
        self.source_id = source_id
        self.cache = cache or self.cache
        # only one instance requesting data at the time
        # if multiple
        self.rate_limiter = rate_limiter or asyncio.Semaphore(1)
        self.lock = asyncio.Lock()
        self.logger = get_logger(f"source.{source_id}")

        self.stages = ["load", "search", "validate", "transform", "drop", "rename"]

        # Initialize stage definitions
        self.regs: Dict[str, StageDefinition] = {}
        for stage_name in self.stages:
            self.regs[stage_name] = Registry()

        self.regs["drop"].register("default", drop_transform)
        self.regs["rename"].register("default", rename_transform)

        self.metrics = {
            "stage_times": {stage: 0.0 for stage in self.stages},
            "requests_total": 0,
            "requests_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "total_execution_time": 0.0,
        }

        self.logger.info(f"Initialized data source: {source_id}")

    def __str__(self):
        # avoid cache misses
        return f"<{self.__class__.__name__}>"

    def register_stage(self, stage_name, after=None):
        if after is None:
            idx = len(self.stages)
        else:
            idx = self.stage.index(after) + 1
        self.stages.insert(idx, stage_name)
        self.metrics["stage_times"][stage_name] = 0.0
        self.regs[stage_name] = Registry()

    async def _execute_stage(
        self, stage_name: str, data: Optional[Any], request: DataRequest
    ) -> Any:
        """Execute a single stage"""
        if stage_name not in request.stages:
            return data
        stage = request.stages[stage_name]
        func = None
        kwargs = {}
        data_format = "default"
        if stage:
            func = stage.get("strategy", func)
            kwargs = stage.get("parameters", kwargs)

        # Class-level overrides
        # The class overrides the parameters
        get_type = f"get_{stage_name}_type"
        get_params = f"get_{stage_name}_params"
        if hasattr(self, get_type):
            data_format = await getattr(self, get_type)(kwargs, request.dependencies)
        if hasattr(self, get_params):
            kwargs = await getattr(self, get_params)(kwargs, request.dependencies)

        # Request can override the executor
        if func is None:
            func = self.regs[stage_name].get(data_format)

        # If it's still None there is
        # nothing we can do but but echo
        if func is None:
            self.logger.debug(
                "No executor defined for stage %s of %s",
                stage_name,
                self.source_id,
            )
            return data

        # Prepare arguments
        # For load stage, no input data, and await
        if stage_name == self.stages[0]:  # First stage (load)
            return await func(**kwargs)
        else:
            # All other stages receive data from previous stage
            # and dependencies, and operations are done in-memory
            return func(data, request.dependencies, **kwargs)

    @log_execution_time("data_fetch")
    async def fetch(self, request: DataRequest = None) -> Any:
        """Fetch data with caching support and detailed logging"""
        if isinstance(request, dict):
            request = DataRequest(stages=request)
        elif request is None:
            request = DataRequest(stages={})

        self.metrics["requests_total"] += 1

        cache_key = hash(request)

        log_context = {
            "source": self.source_id,
            "cache_key": cache_key,
            "parameters": {
                k: v
                for k, v in request.stages.items()
                if k not in {"password", "api_key"}
            },
        }

        # Try cache first
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached:
                self.metrics["cache_hits"] += 1
                self.logger.debug("Cache hit", extra=log_context)
                return cached
            else:
                self.metrics["cache_misses"] += 1
                self.logger.debug("Cache miss", extra=log_context)

        # Fetch dependencies after checking our own cache
        # to avoid unecessary loads
        for dep_req in request.dependency_requirements:
            rid = dep_req.request_id
            data = await dep_req.fetch()
            if rid in request.dependencies:
                self.logger.warning("Duplicate request %s on %s", rid, self.source_id)
                rid = rid + "_"
            request.dependencies[rid] = data

        # Start time after deps have been resolved
        # To measure our performance in isolation
        start_time = time.time()

        # Fetch from source with rate limiting
        async with self.rate_limiter:
            try:
                stage_name = None
                self.logger.info(
                    f"Fetching data from {self.source_id} for {request.request_id}",
                    extra=log_context,
                )
                data = None
                for stage_name in self.stages:
                    stage_start = time.time()
                    data = await self._execute_stage(stage_name, data, request)
                    stage_time = time.time() - stage_start
                    self.metrics["stage_times"][stage_name] += stage_time

                # Cache the result
                if self.cache:
                    self.cache.set(cache_key, data)

                execution_time = time.time() - start_time
                self.metrics["total_execution_time"] += execution_time

                data_size = len(str(data)) if data else 0
                self.logger.info(
                    f"Successfully fetched {data_size} bytes from {self.source_id} "
                    f"in {execution_time:.2f}s for {request.request_id}",
                    extra={
                        **log_context,
                        "data_size": data_size,
                        "execution_time": execution_time,
                    },
                )

                return data

            except Exception as e:
                self.metrics["requests_failed"] += 1
                execution_time = time.time() - start_time

                self.logger.error(
                    f"Data fetch for {self.source_id} failed after {execution_time:.2f}s:"
                    f"{str(e)} at {stage_name} in {request.request_id}",
                    extra={
                        **log_context,
                        "execution_time": execution_time,
                        "stage_name": stage_name,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for this data source"""
        avg_time = (
            self.metrics["total_execution_time"] / self.metrics["requests_total"]
            if self.metrics["requests_total"] > 0
            else 0
        )

        cache_hit_rate = (
            self.metrics["cache_hits"]
            / (self.metrics["cache_hits"] + self.metrics["cache_misses"])
            if (self.metrics["cache_hits"] + self.metrics["cache_misses"]) > 0
            else 0
        )

        success_rate = (
            1 - (self.metrics["requests_failed"] / self.metrics["requests_total"])
            if self.metrics["requests_total"] > 0
            else 1
        )

        return {
            **self.metrics,
            "average_execution_time": avg_time,
            "cache_hit_rate": cache_hit_rate,
            "success_rate": success_rate,
        }

    def log_metrics(self):
        """Log current metrics for this data source"""
        metrics = self.get_metrics()
        self.logger.info(
            "Data source metrics", extra={"metrics": metrics, "source": self.source_id}
        )

    # necessary to avoid cache misses
    __repr__ = __str__


class Multiplexer(DataSource):
    """Data source that merges data from multiple dependencies"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.regs["load"].register("default", merge_loader)

        self.regs["search"].register("html", html_search)
        self.regs["search"].register("default", linear_search)

    async def get_load_params(self, params, dependencies):
        return {"data_dict": dependencies} | params


class Demultiplexer(DataSource):
    """
    Data source that splits one data request in many.
    Useful for making many HTTP request with equal search params and rate limit
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.regs["load"].register("default", split_loader)

        self.regs["search"].register("default", linear_search)
        self.regs["search"].register("html", html_search)

    async def get_load_params(self, params, dependencies):
        reqs = params.get("requests", [])
        if isinstance(reqs, Callable):
            reqs = reqs(dependencies)

        return {
            "requests": reqs,
        }


class WebDataSource(DataSource):
    """
    HTTP/HTTPS data source with async session management and logging

    You usually want to download the whole thing and parse it elsewhere.
    """

    def __init__(
        self,
        source_id: str,
        cache: Optional[CacheBackend] = True,
        rate_limiter=WindowRateLimiter(1 / 5),
        base_headers: Dict = None,
    ):
        # hack to configure the cache with the source_id
        if cache is True:
            cache = FileCache(cache_dir=CACHE_DIR / source_id)
        super().__init__(source_id, cache, rate_limiter)
        self.base_headers = base_headers or {}

        self._session = None
        self._session_lock = asyncio.Lock()

        self.regs["load"].register("default", http_loader)

        self.regs["search"].register("default", html_search)

        self.logger.info(f"Web data source configured with rate limit: {rate_limiter}")

    async def _ensure_session(self):
        """Ensure session exists (async version)"""
        async with self._session_lock:
            if self._session is None:
                self._session = aiohttp.ClientSession(
                    headers=self.base_headers, timeout=aiohttp.ClientTimeout(total=30)
                )
                self.logger.debug("Created new async HTTP session")
        return self._session

    async def get_load_params(self, params, dependencies):
        data = {}
        request_data = {}
        for key, value in params.items():
            if key not in {"url", "session", "method", "headers", "raw"}:
                request_data[key] = value

        session = params.get("session")
        if not session:
            session = await self._ensure_session()

        return {
            "session": session,
            "url": params["url"],
            "method": params.get("method", "GET"),
            "request_data": request_data,
            "headers": {**self.base_headers, **params.get("headers", {})},
            "raw": params.get("raw", False),
        }

    async def close(self):
        """Close the session when done"""
        if self._session:
            await self._session.close()
            self._session = None
            self.logger.debug("Closed HTTP session")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class FileDataSource(DataSource):
    """Local file data source with pluggable search strategies"""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.regs["load"].register("txt", text_loader)
        self.regs["load"].register("json", json_loader)
        self.regs["load"].register("tsv", tsv_loader)
        self.regs["load"].register("csv", csv_loader)
        self.regs["load"].register("log", log_loader)
        self.regs["load"].register("default", text_loader)

        self.regs["search"].register("default", linear_search)
        self.regs["search"].register("linear", linear_search)
        self.regs["search"].register("binary", binary_search)

    async def get_load_type(self, params, dependencies) -> str:
        """Auto-detect file type from extension"""
        ext = params["file_path"].suffix.lower().lstrip(".")
        return ext if ext else "txt"

    async def get_search_type(self, params, dependencies):
        return "binary"
