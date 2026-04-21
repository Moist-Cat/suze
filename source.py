"""Data Source Abstraction Framework for Asynchronous Data Processing.

This module provides a flexible, extensible framework for defining data sources
with support for caching, rate limiting, transformation pipelines, and multiple
data formats. It implements a stage-based execution model where data flows through
configurable processing stages.

Key Components:
    - DataSource: Abstract base class defining the core data fetching pipeline
    - Multiplexer: Merges data from multiple dependencies
    - Demultiplexer: Splits one request into multiple parallel requests
    - WebDataSource: HTTP/HTTPS data source with session management
    - FileDataSource: Local file data source with format detection

The framework supports:
    - Asynchronous operations with asyncio
    - Multi-stage data processing pipelines
    - Pluggable caching backends (memory, filesystem)
    - Rate limiting and concurrency control
    - Comprehensive metrics collection
    - Extensible stage registries
"""

from abc import ABC
from typing import Any, Dict, Optional, Callable
import aiohttp
import asyncio
import time
import functools
import logging
import traceback

from opentelemetry import trace, metrics
from opentelemetry.trace import StatusCode, NoOpTracerProvider
from opentelemetry.metrics import Counter, Histogram, ObservableGauge, NoOpMeterProvider

from suze.cache import CacheBackend, FileCache, MemCache
from suze.search import (
    binary_search,
    linear_search,
    html_search,
)
from suze.config import CACHE_DIR
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
from suze.log import get_logger, logged
from suze.rate import WindowRateLimiter
from suze.registries import Registry
from suze.request import DataRequest
from suze.utils import rolling_hash
from suze.metrics import start_noop_span


# Decorators for instrumentation
@logged
class DataSource(ABC):
    """Abstract base class for all data sources with comprehensive logging.

    This class implements a stage-based data processing pipeline with built-in
    caching, rate limiting, and metrics collection. It provides a flexible
    framework for fetching and processing data from various sources.

    Attributes:
        cache: Shared memory cache instance for all data sources
        source_id: Unique identifier for this data source
        rate_limiter: Controls request concurrency and rate limiting
        lock: Asynchronous lock for thread-safe operations
        logger: Logger instance for this data source
        stages: List of stage names in the processing pipeline
        regs: Dictionary of stage registries for pluggable functionality
        metrics: Dictionary containing performance and operational metrics

    Example:
        >>> class MyDataSource(DataSource):
        ...     def __init__(self, source_id: str):
        ...         super().__init__(source_id)
        ...         # Register custom loaders, searchers, etc.
        ...
        >>> ds = MyDataSource("my-api")
        >>> result = await ds.fetch(request)
    """

    # global cache for everyone to handle memory fingerprint
    cache = MemCache()

    def __init__(
        self,
        source_id: str,
        cache: Optional[CacheBackend] = None,
        rate_limiter=None,
    ):
        """Initialize a data source with caching and rate limiting support.

        Args:
            source_id: Unique identifier for this data source (used in logging
                       and cache paths)
            cache: Cache backend instance. If None, uses the shared memory cache.
                   Can be FileCache for persistent storage or MemCache for
                   in-memory caching.
            rate_limiter: Rate limiter instance controlling request frequency.
                         Defaults to asyncio.Semaphore(1) for serial execution.

        Raises:
            ValueError: If source_id is empty or invalid
        """
        self.source_id = source_id
        if cache is not None:
            self.cache = cache
        # only one instance requesting data at the time
        self.rate_limiter = rate_limiter or asyncio.Semaphore(1)
        self.lock = asyncio.Lock()

        self.stages = ["load", "search", "validate", "transform", "drop", "rename"]

        # Initialize stage definitions
        self.regs = {}
        for stage_name in self.stages:
            self.regs[stage_name] = Registry()

        self.regs["drop"].register("default", drop_transform)
        self.regs["rename"].register("default", rename_transform)

        # Initialize metrics collection
        self.metrics = {
            "stage_times": {stage: 0.0 for stage in self.stages},
            "requests_total": 0,
            "requests_failed": 0,
            "total_execution_time": 0.0,
            "stage_counts": {stage: 0 for stage in self.stages},
            "dependency_resolution_time": 0.0,
            "rate_limit_wait_time": 0.0,
        }

        self._tracer = trace.get_tracer(self.__class__.__name__)
        self._meter = metrics.get_meter(self.__class__.__name__)
        self._init_metrics()

        # Log initialization with telemetry context
        self.logger.info(
            f"Initialized data source: {source_id}",
            extra={
                "datasource_id": source_id,
                "cache_type": self.cache.__class__.__name__,
                "stages": self.stages,
            },
        )

    def _init_metrics(self):
        """Initialize OpenTelemetry metrics for this datasource"""
        # Request counters
        self._requests_total_counter = self._meter.create_counter(
            name="datasource.requests.total",
            description="Total number of data source requests",
            unit="1",
        )

        self._requests_failed_counter = self._meter.create_counter(
            name="datasource.requests.failed",
            description="Number of failed data source requests",
            unit="1",
        )

        # Execution time histograms
        self._total_execution_time_histogram = self._meter.create_histogram(
            name="datasource.execution.time",
            description="Total execution time per request",
            unit="s",
        )

        self._total_result_size_bytes = self._meter.create_histogram(
            name="datasource.result.size",
            description="Total result size per request",
            unit="bytes",
        )

        self._total_result_count = self._meter.create_histogram(
            name="datasource.result.count",
            description="Total number of items per request",
            unit="1",
        )

        self._stage_time_histogram = self._meter.create_histogram(
            name="datasource.stage.time",
            description="Execution time per stage",
            unit="s",
        )

        self._dependency_resolution_histogram = self._meter.create_histogram(
            name="datasource.dependency.resolution.time",
            description="Time spent resolving dependencies",
            unit="s",
        )

        self._rate_limit_wait_histogram = self._meter.create_histogram(
            name="datasource.rate_limit.wait.time",
            description="Time spent waiting due to rate limiting",
            unit="s",
        )

    def _record_metric(
        self, metric_name: str, value: float = 1, attributes: Dict[str, Any] = None
    ):
        """Record a metric with OpenTelemetry"""
        metric_attr = {"datasource.id": self.source_id, "component": "datasource"}
        if attributes:
            metric_attr.update(attributes)

        metric = getattr(self, f"_{metric_name}")
        if isinstance(metric, Counter):
            metric.add(value, metric_attr)
        elif isinstance(metric, Histogram):
            metric.record(value, metric_attr)

    def __str__(self):
        """Return string representation of the data source.

        Returns:
            String in format "<ClassName>" to avoid cache key collisions
            with detailed string representations.
        """
        return f"<{self.__class__.__name__}>"

    def __hash__(self):
        return rolling_hash(self.source_id)

    def register_stage(self, stage_name, after=None):
        """Register a new stage in the processing pipeline.

        Args:
            stage_name: Name of the stage to register
            after: Name of the stage after which to insert the new stage.
                   If None, appends to the end of the pipeline.

        Example:
            >>> ds = DataSource("test")
            >>> ds.register_stage("enrich", after="load")
            >>> # Now stages are: ["load", "enrich", "search", ...]
        """
        if after is None:
            idx = len(self.stages)
        else:
            idx = self.stages.index(after) + 1
        self.stages.insert(idx, stage_name)
        self.metrics["stage_times"][stage_name] = 0.0
        self.metrics["stage_counts"][stage_name] = 0
        self.regs[stage_name] = Registry()

    async def _execute_stage(
        self,
        stage_name: str,
        data: Optional[Any],
        request: DataRequest,
        dependencies: dict,
    ) -> Any:
        """Execute a single stage in the processing pipeline.

        This method orchestrates the execution of a specific stage, handling
        parameter resolution, function lookup, and execution. It supports
        class-level overrides and request-specific configurations.

        Args:
            stage_name: Name of the stage to execute
            data: Input data from previous stage (None for first stage)
            request: DataRequest containing stage configurations
            dependencies: Dictionary of resolved dependencies from other
                         data sources

        Returns:
            Processed data from this stage

        Raises:
            KeyError: If required parameters are missing
            TypeError: If the stage function is not callable
            RuntimeError: If stage execution fails
        """
        stage_start = time.time()

        # Add stage-specific attributes to span
        current_span = trace.get_current_span()
        if current_span:
            current_span.set_attribute("datasource.stage", stage_name)
            current_span.set_attribute(
                "datasource.stage_number", self.stages.index(stage_name)
            )

        if stage_name not in request.stages:
            return data
        stage = request.stages[stage_name]
        func = None
        kwargs = {}
        data_format = "default"
        if stage:
            func = stage.get("strategy", func)
            kwargs = stage.get("parameters", kwargs)
            if stage and (not "parameters" in stage and not "strategy" in stage):
                # should be a model/static class
                raise ValueError(
                    f"Invalid stage definition. Expected a 'parameters' field, found {stage}."
                )

        # Class-level overrides
        # The class overrides the parameters
        get_type = f"get_{stage_name}_type"
        get_params = f"get_{stage_name}_params"
        if hasattr(self, get_type):
            data_format = await getattr(self, get_type)(kwargs, dependencies)
        if hasattr(self, get_params):
            kwargs = await getattr(self, get_params)(kwargs, dependencies)

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
        if hasattr(func, "__name__") and func.__name__ != "<lambda>":
            current_span.set_attribute("datasource.stage.strategy", func.__name__)

        if stage_name == self.stages[0]:  # First stage (load)
            result = await func(**kwargs)
        else:
            # All other stages receive data from previous stage
            # and dependencies, and operations are done in-memory
            result = func(data, dependencies, **kwargs)

        # Record stage metrics
        stage_time = time.time() - stage_start
        self.metrics["stage_times"][stage_name] += stage_time
        self.metrics["stage_counts"][stage_name] += 1

        self._record_metric(
            "stage_time_histogram",
            stage_time,
            {"stage": stage_name, "status": "success"},
        )

        return result

    async def fetch(self, request: DataRequest = None) -> Any:
        """Fetch data with caching support and detailed logging.

        This is the main entry point for data retrieval. It implements:
        1. Cache lookup with cache key based on request hash
        2. Dependency resolution
        3. Stage-by-stage pipeline execution
        4. Result caching
        5. Comprehensive metrics collection

        Args:
            request: DataRequest object containing stage configurations and
                    dependencies. Can also be a dict which will be converted
                    to a DataRequest.

        Returns:
            The processed data from the pipeline

        Raises:
            Exception: Any exception raised during data fetching, with
                      detailed logging context

        Example:
            >>> ds = DataSource("test")
            >>> request = DataRequest(stages={
            ...     "load": {"strategy": "default", "parameters": {...}},
            ...     "search": {"strategy": "linear", "parameters": {...}}
            ... })
            >>> result = await ds.fetch(request)
        """
        if isinstance(request, dict):
            request = DataRequest(stages=request)
        elif request is None:
            request = DataRequest(stages={})

        request_id = request.request_id

        # Record request start
        fetch_start = time.time()
        self.metrics["requests_total"] += 1

        self._record_metric("requests_total_counter", 1)

        cache_key = hash(request)

        log_context = {
            "datasource_id": self.source_id,
            "cache_key": cache_key,
            "request_id": request_id,
            "parameters": {
                k: v
                for k, v in request.stages.items()
                if k not in {"password", "api_key"}
            },
        }

        # Try cache first
        if self.cache is not None:
            cached = self.cache.get(cache_key)
            if cached is not None:
                if request.enable_telemetry:
                    self._record_metric(
                        "total_execution_time_histogram",
                        time.time() - fetch_start,
                        {"status": "cache_hit", "request.id": request_id},
                    )
                return cached

        # Fetch dependencies after checking our own cache
        # to avoid unecessary loads
        dependencies = {}
        if request.dependency_requirements:
            dep_resolution_start = time.time()

            for dep_req in request.dependency_requirements:
                dep_start = time.time()

                rid = dep_req.request_id
                data = await dep_req.fetch()
                if rid in dependencies:
                    self.logger.warning(
                        f"Duplicate request {rid} on {self.source_id}",
                        extra={"datasource_id": self.source_id, "request_id": rid},
                    )
                    rid = rid + "_"
                dependencies[rid] = data

            dep_resolution_time = time.time() - dep_resolution_start
            self.metrics["dependency_resolution_time"] += dep_resolution_time

            if request.enable_telemetry:
                self._record_metric(
                    "dependency_resolution_histogram",
                    dep_resolution_time,
                    {
                        "num_dependencies": len(request.dependency_requirements),
                        "request.id": request_id,
                    },
                )

        rate_limit_start = time.time()

        # Fetch from source with rate limiting
        async with self.rate_limiter:
            # Start time after deps have been resolved
            # And we are not blocked by the rate limiter
            # To measure our performance in isolation
            rate_limit_wait = time.time() - rate_limit_start
            self.metrics["rate_limit_wait_time"] += rate_limit_wait

            self._record_metric("rate_limit_wait_histogram", rate_limit_wait, {})

            execution_start = time.time()
            stage_name = None
            try:
                self.logger.info(
                    f"Fetching data from {self.source_id} for {request.request_id}",
                    extra=log_context,
                )

                data = None
                span_instance = start_noop_span()
                if request.enable_telemetry:
                    span_instance = self._tracer.start_as_current_span(
                        f"DataSource.fetch"
                    )
                with span_instance as span:
                    span.set_attribute("datasource.id", self.source_id)
                    span.set_attribute("datasource.request.id", str(request.request_id))
                    span.set_attribute(
                        "datasource.request.num_dependencies",
                        len(request.dependency_requirements),
                    )

                    for stage_name in self.stages:
                        stage_span_instance = start_noop_span()
                        if request.enable_telemetry:
                            stage_span_instance = self._tracer.start_as_current_span(
                                f"DataSource.stage.{stage_name}"
                            )

                        with stage_span_instance as stage_span:
                            stage_span.set_attribute("datasource.stage", stage_name)
                            stage_span.set_attribute(
                                "datasource.stage_number", self.stages.index(stage_name)
                            )

                            data = await self._execute_stage(
                                stage_name, data, request, dependencies
                            )
                            if data is not None:
                                try:
                                    data_size = len(str(data))
                                    stage_span.set_attribute("result.size", data_size)
                                except:
                                    pass

                # Cache the result
                if self.cache is not None:
                    self.cache.set(cache_key, data)
                else:
                    self.logger.warning("No cache set for %s", self.source_id)

                execution_time = time.time() - execution_start
                total_time = time.time() - fetch_start

                self.metrics["total_execution_time"] += execution_time

                if request.enable_telemetry:
                    self._record_metric(
                        "total_execution_time_histogram",
                        execution_time,
                        {
                            "status": "success",
                            "cache_hit": False,
                            "request.id": request_id,
                        },
                    )

                # final data_size not stage data_size
                data_size = len(str(data)) if data else 0

                if request.enable_telemetry:
                    self._record_metric(
                        "total_result_size_bytes",
                        data.__sizeof__(),
                        {"request.id": request_id},
                    )

                if hasattr(data, "__len__"):
                    self._record_metric(
                        "total_result_count",
                        len(data),
                        {"request.id": request_id},
                    )

                self.logger.info(
                    f"Successfully fetched {data_size} bytes from {self.source_id} "
                    f"in {total_time:.2f}s (execution: {execution_time:.2f}s)",
                    extra={
                        **log_context,
                        "data_size": data_size,
                        "total_time": total_time,
                        "execution_time": execution_time,
                        "cache_hit": False,
                        "num_stages": len(self.stages),
                        "dependency_resolution_time": (
                            dep_resolution_time
                            if "dep_resolution_time" in locals()
                            else 0
                        ),
                        "rate_limit_wait": rate_limit_wait,
                    },
                )

                return data

            except Exception as e:
                self.metrics["requests_failed"] += 1
                execution_time = time.time() - execution_start
                total_time = time.time() - fetch_start

                self._record_metric(
                    "requests_failed_counter",
                    1,
                    {"error.type": e.__class__.__name__},
                )
                self._record_metric(
                    "total_execution_time_histogram",
                    execution_time,
                    {"status": "error", "error.type": e.__class__.__name__},
                )

                self.logger.error(
                    f"Data fetch for {self.source_id} failed after {total_time:.2f}s: '{str(e)}' at {stage_name}",
                    extra={
                        **log_context,
                        "total_time": total_time,
                        "execution_time": execution_time,
                        "stage_name": stage_name,
                        "error": str(e),
                        "error_type": e.__class__.__name__,
                        "stack_trace": (traceback.format_exc()),
                    },
                    exc_info=True,
                )
                raise

    # necessary to avoid cache misses
    __repr__ = __str__


class Multiplexer(DataSource):
    """Data source that merges data from multiple dependencies.

    This data source specializes in combining data from multiple upstream
    sources using the merge_loader. It's useful for scenarios where data
    needs to be aggregated from multiple APIs, databases, or files.

    Inherits all capabilities from DataSource and adds:
    - Default merge_loader for the load stage
    - Support for HTML and linear search strategies
    - Automatic dependency parameter handling

    Example:
        >>> mux = Multiplexer("combined-data")
        >>> # Configure with multiple dependency requests
        >>> result = await mux.fetch(request_with_dependencies)
    """

    def __init__(self, *args, **kwargs):
        """Initialize a Multiplexer data source."""
        super().__init__(*args, **kwargs)

        self.regs["load"].register("default", merge_loader)

        self.regs["search"].register("html", html_search)
        self.regs["search"].register("default", linear_search)

    async def get_load_params(self, params, dependencies):
        """Prepare parameters for the merge loader.

        Combines the resolved dependencies with any additional parameters
        specified in the request.

        Args:
            params: Original parameters from the request
            dependencies: Resolved dependencies from upstream sources

        Returns:
            Dictionary of parameters for merge_loader
        """
        return {"data_dict": dependencies} | params


class Demultiplexer(DataSource):
    """
    Data source that splits one data request into many parallel requests.

    This is useful for making many similar HTTP requests (e.g., paginated APIs,
    batch processing) with shared search parameters and rate limiting. It
    parallelizes the execution of multiple requests.

    Inherits all capabilities from DataSource and adds:
    - Default split_loader for the load stage
    - Support for HTML and linear search strategies
    - Dynamic request generation from dependencies

    Example:
        >>> demux = Demultiplexer("batch-processor")
        >>> # Configure with a list of requests or a request generator
        >>> result = await demux.fetch({
        ...     "load": {
        ...         "parameters": {
        ...             "requests": lambda deps: [
        ...                 DataRequest(url=f"https://api.example.com/item/{i}")
        ...                 for i in range(10)
        ...             ]
        ...         }
        ...     }
        ... })
    """

    def __init__(self, *args, **kwargs):
        """Initialize a Demultiplexer data source."""
        super().__init__(*args, **kwargs)

        self.regs["load"].register("default", split_loader)

        self.regs["search"].register("default", linear_search)
        self.regs["search"].register("html", html_search)

    async def get_load_params(self, params, dependencies):
        """Prepare parameters for the split loader.

        Extracts the requests from parameters, which can be either a static
        list or a callable that generates requests based on dependencies.

        Args:
            params: Original parameters from the request
            dependencies: Resolved dependencies from upstream sources

        Returns:
            Dictionary of parameters for split_loader
        """
        if isinstance(params["requests"], Callable):
            params["requests"] = params["requests"](dependencies)

        for req in params["requests"]:
            req.enable_telemetry = False

        return {
            **params,
        }


class WebDataSource(DataSource):
    """
    HTTP/HTTPS data source with async session management and logging.

    This specialized data source handles HTTP operations with built-in:
    - Connection pooling and session management
    - Automatic retry and timeout handling
    - Configurable rate limiting
    - Persistent caching (FileCache by default)
    - HTML content parsing support

    You typically want to download the entire content and parse it in
    subsequent stages (search, transform, etc.).

    Attributes:
        base_headers: Default HTTP headers for all requests
        _session: Internal aiohttp.ClientSession instance

    Example:
        >>> web_ds = WebDataSource("api-service")
        >>> result = await web_ds.fetch({
        ...     "load": {
        ...         "parameters": {
        ...             "url": "https://api.example.com/data",
        ...             "method": "GET",
        ...             "headers": {"Authorization": "Bearer token"}
        ...         }
        ...     }
        ... })
    """

    def __init__(
        self,
        source_id: str,
        cache: Optional[CacheBackend] = True,
        rate_limiter=WindowRateLimiter(1 / 5),
        base_headers: Dict = None,
    ):
        """Initialize a WebDataSource.

        Args:
            source_id: Unique identifier for this web data source
            cache: Cache backend. If True (default), creates a FileCache
                   in CACHE_DIR/source_id. If False, disables caching.
                   Can also be a custom CacheBackend instance.
            rate_limiter: Rate limiter for controlling request frequency.
                         Defaults to WindowRateLimiter(1/5) = 1 request every 5 seconds.
            base_headers: Default HTTP headers applied to all requests

        Note:
            The default rate limiter allows 1 request
            every 5 seconds. Adjust based on API rate limits.
        """
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
        """Ensure session exists (async version).

        Creates a new aiohttp.ClientSession if one doesn't exist, with
        connection pooling and timeout configuration.

        Returns:
            Existing or newly created aiohttp.ClientSession

        Note:
            Sessions are created lazily on first request to avoid unnecessary
            connection overhead.
        """
        async with self._session_lock:
            if self._session is None:
                self._session = aiohttp.ClientSession(
                    headers=self.base_headers, timeout=aiohttp.ClientTimeout(total=30)
                )
                self.logger.debug("Created new async HTTP session")
        return self._session

    async def get_load_params(self, params, dependencies):
        """Prepare parameters for the HTTP loader.

        Extracts HTTP-specific parameters and ensures a session exists.
        Combines base headers with request-specific headers.

        Args:
            params: Original parameters from the request
            dependencies: Unused in WebDataSource (kept for API consistency)

        Returns:
            Dictionary of parameters for http_loader

        Raises:
            KeyError: If 'url' parameter is missing
        """
        del dependencies
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
        """Close the session when done.

        Properly closes the aiohttp session to free resources. Should be
        called when the data source is no longer needed.

        Example:
            >>> web_ds = WebDataSource("api")
            >>> try:
            ...     result = await web_ds.fetch(request)
            ... finally:
            ...     await web_ds.close()
        """
        if self._session:
            await self._session.close()
            self._session = None
            self.logger.debug("Closed HTTP session")

    async def __aenter__(self):
        """Support async context manager protocol.

        Returns:
            Self, for use in async with statements
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Support async context manager protocol.

        Automatically closes the session when exiting the context.
        """
        await self.close()


class FileDataSource(DataSource):
    """Local file data source with pluggable search strategies.

    This data source handles local file operations with automatic format
    detection based on file extensions. It supports multiple file formats
    and search strategies optimized for file content.

    Supported formats:
        - txt: Plain text files (default)
        - json: JSON structured data
        - tsv: Tab-separated values
        - csv: Comma-separated values
        - log: Log files (line-based)

    Search strategies:
        - linear: Sequential scanning (default for most formats)
        - binary: Binary search for sorted files

    Example:
        >>> file_ds = FileDataSource("local-files")
        >>> result = await file_ds.fetch({
        ...     "load": {
        ...         "parameters": {"file_path": Path("data.json")}
        ...     },
        ...     "search": {
        ...         "parameters": {"pattern": "search_term"}
        ...     }
        ... })
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Initialize a FileDataSource with format-specific loaders."""
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
        """Auto-detect file type from extension.

        Args:
            params: Request parameters containing 'file_path'
            dependencies: Unused (kept for API consistency)

        Returns:
            File extension as string (e.g., 'json', 'csv', 'txt')

        Note:
            If no extension is found or the extension is not recognized,
            defaults to 'txt' for plain text processing.
        """
        del dependencies
        ext = params["file_path"].suffix.lower().lstrip(".")
        return ext if ext else "txt"

    async def get_search_type(self, params, dependencies):
        """Determine the appropriate search strategy.

        By default, uses binary search for file operations as it's generally
        more efficient for sorted data.

        Args:
            params: Request parameters
            dependencies: Unused (kept for API consistency)

        Returns:
            Search strategy identifier ('binary' by default)
        """
        del dependencies, params
        return "binary"
