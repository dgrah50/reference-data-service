"""
Circuit breaker implementation for exchange adapters.
"""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests allowed
    OPEN = "open"          # Failing, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5        # Failures before opening
    success_threshold: int = 2        # Successes to close from half-open
    timeout: float = 60.0             # Seconds before trying half-open
    half_open_max_calls: int = 1      # Max concurrent calls in half-open


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    States:
    - CLOSED: Normal operation, tracking failures
    - OPEN: Too many failures, rejecting calls, waiting for timeout
    - HALF_OPEN: Testing if service recovered with limited calls
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def is_available(self) -> bool:
        """Check if circuit allows requests."""
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.OPEN:
            # Check if timeout elapsed, transition to half-open
            if self._last_failure_time and (time.monotonic() - self._last_failure_time) >= self.config.timeout:
                return True  # Will transition to half-open on next call
            return False
        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls
        return False

    async def __aenter__(self):
        """Async context manager entry - check if call is allowed."""
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and (time.monotonic() - self._last_failure_time) >= self.config.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._success_count = 0
                else:
                    raise CircuitOpenError(f"Circuit breaker '{self.name}' is open")

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit breaker '{self.name}' is half-open, max calls reached")
                self._half_open_calls += 1

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - record success or failure."""
        async with self._lock:
            if exc_type is None:
                await self._record_success()
            else:
                await self._record_failure()
        return False  # Don't suppress exceptions

    async def _record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0

    async def _record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            # Any failure in half-open goes back to open
            self._state = CircuitState.OPEN
            self._half_open_calls = 0
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open and call is rejected."""
    pass
