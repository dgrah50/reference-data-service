"""
Rate limiter implementation using token bucket algorithm.
"""

import asyncio
import time


class RateLimiter:
    """
    Simple asynchronous token bucket rate limiter.
    Allows at most `max_calls` within a given `period` (in seconds).
    """

    def __init__(self, max_calls: int, period: float) -> None:
        """
        Initialize the rate limiter.

        Args:
            max_calls: Maximum number of calls allowed within the period.
            period: Time period in seconds.
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = 0
        self.last_reset = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        """
        Acquire a token from the rate limiter.
        If the call limit has been reached, wait until the period resets.
        """
        async with self.lock:
            now = time.monotonic()
            # Reset counter if the period has elapsed
            if now - self.last_reset >= self.period:
                self.calls = 0
                self.last_reset = now

            if self.calls >= self.max_calls:
                self.last_reset = now
                self.calls = 0
                await asyncio.sleep(self.period)  # Wait for a full period

            self.calls += 1
