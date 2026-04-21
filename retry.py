"""
Retry policy implementation for DataSource using higher-order functions.
This follows the same pattern as other components in the system.
"""

import asyncio
import time
import random
from typing import Callable, Type, Optional, List, Any
from functools import wraps
from dataclasses import dataclass
from enum import Enum

from suze.log import get_logger

logger = get_logger("retry")


class RetryStrategy(Enum):
    """Strategy for handling retries."""

    NONE = "none"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIXED = "fixed"


@dataclass
class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts (including initial)
        strategy: Retry strategy to use
        base_delay: Base delay in seconds for retries
        max_delay: Maximum delay in seconds (for exponential backoff)
        jitter: Whether to add random jitter to delays
        retry_on: List of exception types to retry on
        backoff_factor: Factor for exponential backoff (default 2.0)
    """

    max_attempts: int = 3
    strategy: RetryStrategy = RetryStrategy.NONE
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter: bool = True
    retry_on: Optional[List[Type[Exception]]] = None
    backoff_factor: float = 2.0

    def __post_init__(self):
        if self.retry_on is None:
            # Default to retrying on any exception except KeyboardInterrupt
            self.retry_on = [Exception]


def with_retry(config: RetryConfig = None):
    """Create a retry policy decorator.

    Args:
        config: RetryConfig instance or dictionary of config options

    Returns:
        Decorator function that adds retry logic to the decorated function

    Example:
        >>> @with_retry(RetryConfig(max_attempts=3))
        >>> async def fetch_data(url):
        ...     return await http_get(url)
    """
    if config is None:
        config = RetryConfig()
    elif isinstance(config, dict):
        config = RetryConfig(**config)

    def decorator(func: Callable) -> Callable:
        """Add retry logic to the given function."""

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            """Async wrapper for retry logic."""
            last_exception = None

            for attempt in range(config.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if we should retry this exception
                    should_retry = any(
                        isinstance(e, exc_type) for exc_type in config.retry_on
                    ) and not isinstance(e, KeyboardInterrupt)

                    if not should_retry or attempt == config.max_attempts - 1:
                        raise

                    # Calculate delay
                    delay = _calculate_delay(config, attempt)

                    # Add jitter if enabled
                    if config.jitter:
                        delay = delay * (0.5 + random.random())

                    # Wait before retry
                    logger.info(
                        "Waiting %s seconds before retrying (attempt %d)",
                        str(delay),
                        attempt,
                    )
                    await asyncio.sleep(delay)

            # This should never be reached due to the raise in the loop
            raise last_exception or RuntimeError("Retry policy failed")

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            """Sync wrapper for retry logic."""
            last_exception = None

            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if we should retry this exception
                    should_retry = any(
                        isinstance(e, exc_type) for exc_type in config.retry_on
                    ) and not isinstance(e, KeyboardInterrupt)

                    if not should_retry or attempt == config.max_attempts - 1:
                        raise

                    # Calculate delay
                    delay = _calculate_delay(config, attempt)

                    # Add jitter if enabled
                    if config.jitter:
                        delay = delay * (0.5 + random.random())

                    # Wait before retry
                    time.sleep(delay)

            # This should never be reached due to the raise in the loop
            raise last_exception or RuntimeError("Retry policy failed")

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def _calculate_delay(config: RetryConfig, attempt: int) -> float:
    """Calculate delay for retry based on strategy.

    Args:
        config: Retry configuration
        attempt: Current attempt number (0-indexed)

    Returns:
        Delay in seconds
    """
    if config.strategy == RetryStrategy.NONE:
        return 0.0
    elif config.strategy == RetryStrategy.FIXED:
        return config.base_delay
    elif config.strategy == RetryStrategy.LINEAR:
        return config.base_delay * (attempt + 1)
    elif config.strategy == RetryStrategy.EXPONENTIAL:
        delay = config.base_delay * (config.backoff_factor**attempt)
        return min(delay, config.max_delay)
    else:
        return config.base_delay


def exponential_backoff(max_attempts: int = 3, base_delay: float = 1.0):
    """Exponential backoff retry policy.

    Args:
        max_attempts: Maximum number of attempts
        base_delay: Base delay for exponential backoff

    Returns:
        Decorator with exponential backoff
    """
    return with_retry(
        RetryConfig(
            max_attempts=max_attempts,
            strategy=RetryStrategy.EXPONENTIAL,
            base_delay=base_delay,
            max_delay=60.0,
            jitter=True,
        )
    )


def linear_backoff(max_attempts: int = 3, base_delay: float = 1.0):
    """Linear backoff retry policy.

    Args:
        max_attempts: Maximum number of attempts
        base_delay: Base delay for linear backoff

    Returns:
        Decorator with linear backoff
    """
    return with_retry(
        RetryConfig(
            max_attempts=max_attempts,
            strategy=RetryStrategy.LINEAR,
            base_delay=base_delay,
            jitter=True,
        )
    )


# Factory function for creating retry policies
def create_retry_policy(**kwargs) -> Callable:
    """Create a retry policy with the given configuration.

    Args:
        **kwargs: Configuration options for RetryConfig

    Returns:
        Decorator function

    Example:
        >>> retry_policy = create_retry_policy(
        ...     max_attempts=5,
        ...     strategy="exponential",
        ...     base_delay=2.0
        ... )
        >>> decorated_func = retry_policy(my_func)
    """
    if "strategy" in kwargs and isinstance(kwargs["strategy"], str):
        kwargs["strategy"] = RetryStrategy(kwargs["strategy"])

    return with_retry(RetryConfig(**kwargs))
