"""
State management for exchange adapters.
"""

import logging
from typing import Dict, Any, Optional, Callable

from deepdiff import DeepDiff

from ..api.models import StandardExchangeInfo


Converter = Callable[[Dict[str, Any]], StandardExchangeInfo]
DiffConverter = Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]


class ExchangeState:
    """Manages the state of exchange information and handles updates."""

    def __init__(self) -> None:
        """Initialize the exchange state."""
        self._current_state: Optional[StandardExchangeInfo] = None

    @property
    def current_state(self) -> Optional[StandardExchangeInfo]:
        """Get the current state."""
        return self._current_state

    def update_state(
        self, new_data: Dict[str, Any], converter: Converter
    ) -> Optional[StandardExchangeInfo]:
        """
        Update the current state with new data.

        Args:
            new_data: Raw data from the exchange
            converter: Function to convert raw data to StandardExchangeInfo

        Returns:
            Updated StandardExchangeInfo if state changed, None otherwise
        """
        try:
            new_state = converter(new_data)

            if self._current_state is None:
                self._current_state = new_state
                return new_state

            diff = DeepDiff(self._current_state.model_dump(), new_state.model_dump(), ignore_order=True)

            if not diff:
                return None

            self._current_state = new_state
            return new_state

        except Exception as e:
            logging.error("Error updating state: %s", e)
            return None

    def apply_diff(
        self, diff_data: Dict[str, Any], converter: DiffConverter
    ) -> Optional[StandardExchangeInfo]:
        """
        Apply a diff update to the current state.

        Args:
            diff_data: Diff data from the exchange
            converter: Function to convert current state and diff data to updated state dict

        Returns:
            Updated StandardExchangeInfo if state changed, None otherwise
        """
        if self._current_state is None:
            logging.warning("Cannot apply diff without initial state")
            return None

        try:
            current_dict = self._current_state.model_dump()

            updated_dict = converter(current_dict, diff_data)

            new_state = StandardExchangeInfo.model_validate(updated_dict)

            diff = DeepDiff(self._current_state.model_dump(), new_state.model_dump(), ignore_order=True)

            if not diff:
                return None

            self._current_state = new_state
            return new_state

        except Exception as e:
            logging.error("Error applying diff: %s", e)
            return None
