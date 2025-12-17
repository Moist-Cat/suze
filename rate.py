"""
Logic for rate limiting
"""

from abc import ABC, abstractmethod
import asyncio
import time


# Rate Limiter Classes
class RateLimiter(ABC):
    """Abstract base class for rate limiters"""

    @abstractmethod
    async def acquire(self) -> None:
        """Acquire permission to make a request"""
        pass

    @abstractmethod
    def release(self) -> None:
        """Release the rate limiter (for connection pool management)"""
        pass


class WindowRateLimiter(RateLimiter):
    """Fixed window rate limiter for simple use cases"""

    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / requests_per_second
        self._last_request = 0
        self._lock = asyncio.Lock()
        # only one concurrent fetch as well

    async def acquire(self) -> None:
        async with self._lock:
            current_time = time.time()
            time_since_last = current_time - self._last_request

            if time_since_last < self.interval:
                wait_time = self.interval - time_since_last
                await asyncio.sleep(wait_time)

            self._last_request = time.time()

    def release(self) -> None:
        pass

    def __str__(self):
        return f"<{self.__class__.__name__} ({self.interval=})>"

    async def __aenter__(self):
        return await self.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.release()
