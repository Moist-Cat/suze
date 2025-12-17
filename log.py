"""
Logging
"""

import asyncio
from pathlib import Path
import time
import logging
import logging.handlers
import functools
import sys
from datetime import datetime

from suze.config import BASE_DIR


def setup_logging(log_dir: Path = BASE_DIR / "logs", level: str = "INFO"):
    """Setup comprehensive logging configuration"""
    log_dir.mkdir(exist_ok=True)

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
        "[%(filename)s:%(lineno)d]"
    )
    simple_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(simple_formatter)
    console_handler.setLevel(logging.INFO)

    # File handler - detailed
    log_file = log_dir / "dataset.log"
    file_handler = logging.handlers.RotatingFileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(detailed_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Error file handler
    error_file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "errors.log", encoding="utf-8"
    )
    error_file_handler.setFormatter(detailed_formatter)
    error_file_handler.setLevel(logging.ERROR)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_file_handler)

    return root_logger


# Create module-specific loggers
def get_logger(name: str) -> logging.Logger:
    """Get a named logger for specific components"""
    return logging.getLogger(f"dataset.{name}")


# Decorators for logging
def log_execution_time(operation_name: str = None):
    """Decorator to log execution time of methods"""

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            start_time = time.time()
            operation = operation_name or func.__name__

            # Extract context for logging
            context = {}
            if args and hasattr(args[0], "source_id"):
                context["source"] = args[0].source_id
            if "item_id" in kwargs:
                context["item_id"] = kwargs["item_id"]
            if "field_name" in kwargs:
                context["field"] = kwargs["field_name"]

            logger.debug(f"Starting {operation}", extra=context)

            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time

                logger.info(
                    f"Completed {operation} in {execution_time:.2f}s",
                    extra={**context, "execution_time": execution_time},
                )
                return result

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"Failed {operation} after {execution_time:.2f}s: {str(e)}",
                    extra={
                        **context,
                        "execution_time": execution_time,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            start_time = time.time()
            operation = operation_name or func.__name__

            context = {}
            if args and hasattr(args[0], "source_id"):
                context["source"] = args[0].source_id

            logger.debug(f"Starting {operation}", extra=context)

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                logger.info(
                    f"Completed {operation} in {execution_time:.2f}s",
                    extra={**context, "execution_time": execution_time},
                )
                return result

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"Failed {operation} after {execution_time:.2f}s: {str(e)}",
                    extra={
                        **context,
                        "execution_time": execution_time,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


def log_cache_operations(func):
    """Decorator to log cache operations"""

    @functools.wraps(func)
    def wrapper(self, key: str, *args, **kwargs):
        logger = get_logger("cache")
        operation = func.__name__

        try:
            result = func(self, key, *args, **kwargs)

            if operation == "get":
                status = "HIT" if result is not None else "MISS"
                logger.debug(f"Cache {status} for key: {key}...")
            elif operation == "set":
                logger.debug(f"Cache SET for key: {key}...")
            elif operation == "exists":
                logger.debug(f"Cache EXISTS check for key: {key}...")

            return result

        except Exception as e:
            logger.error(f"Cache {operation} failed for key {key}...: {e}")
            raise

    return wrapper


root_logger = setup_logging()
