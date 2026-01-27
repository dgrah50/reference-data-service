"""
Simple event bus for pub/sub pattern - replaces RxPY Subject.
"""

from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class EventBus(Generic[T]):
    """Simple pub/sub for broadcasting updates to multiple subscribers."""

    def __init__(self) -> None:
        """Initialize with empty subscriber list."""
        self._subscribers: list[Callable[[T], None]] = []

    def subscribe(self, callback: Callable[[T], None]) -> Callable[[], None]:
        """
        Subscribe to updates.

        Args:
            callback: Function to call when a value is published.

        Returns:
            An unsubscribe function that removes the callback.
        """
        self._subscribers.append(callback)
        return lambda: self._subscribers.remove(callback) if callback in self._subscribers else None

    def publish(self, value: T) -> None:
        """
        Publish value to all subscribers.

        Args:
            value: The value to broadcast to all subscribers.
        """
        for callback in self._subscribers:
            callback(value)
