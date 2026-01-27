"""
Base adapter class for exchange adapters.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional

import aiohttp

from ..api.models import StandardExchangeInfo
from ..core.circuit_breaker import CircuitBreaker
from ..core.event_bus import EventBus


class BaseExchangeAdapter(ABC):
    """Base class for exchange adapters."""

    def __init__(self) -> None:
        """Initialize the adapter with an event bus."""
        self._bus: EventBus[StandardExchangeInfo] = EventBus()
        self._running: bool = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None

    @property
    def session(self) -> aiohttp.ClientSession:
        """Get the aiohttp session, creating it if necessary."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    @property
    def circuit_breaker(self) -> Optional[CircuitBreaker]:
        """Get the circuit breaker if available. Subclasses should override."""
        return getattr(self, '_circuit_breaker', None)

    @abstractmethod
    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw exchange data to StandardExchangeInfo format.

        Args:
            raw: Raw JSON data from the exchange.

        Returns:
            A StandardExchangeInfo instance with normalized data.
        """
        pass

    @abstractmethod
    async def _listen(self) -> None:
        """
        Listen to the exchange's WebSocket endpoint.
        Applies rate limiting and retry logic with exponential backoff.
        Publishes normalized update messages.
        """
        pass

    def subscribe(
        self, callback: Callable[[StandardExchangeInfo], None]
    ) -> Callable[[], None]:
        """
        Subscribe to exchange info updates.

        Starts the listener if not already running.

        Args:
            callback: Function to call when new exchange info is available.

        Returns:
            An unsubscribe function that removes the callback.
        """
        if not self._running:
            self._running = True
            self._listen_task = asyncio.create_task(self._listen())
        return self._bus.subscribe(callback)

    async def close(self) -> None:
        """Cleanly close the aiohttp session and stop the listener."""
        if self._listen_task is not None:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._running = False
