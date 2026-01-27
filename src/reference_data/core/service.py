"""
Service layer that manages exchange adapters and maintains the latest snapshot.
"""

import logging
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from ..adapters.base import BaseExchangeAdapter
from ..adapters.binance import BinanceExchangeAdapter
from ..adapters.bitget import BitgetExchangeAdapter
from ..adapters.bybit import BybitExchangeAdapter
from ..adapters.coinbase import CoinbaseExchangeAdapter
from ..adapters.coinbase_intl import CoinbaseIntlExchangeAdapter
from ..adapters.okx import OKXExchangeAdapter
from ..api.models import ExchangeEnum, StandardExchangeInfo
from .event_bus import EventBus


@dataclass
class ServiceConfig:
    """Configuration for enabling/disabling exchanges and testnet mode."""

    enable_binance: bool = True
    enable_okx: bool = True
    enable_bybit: bool = True
    enable_bitget: bool = True
    enable_coinbase: bool = True
    enable_coinbase_intl: bool = True
    use_testnet: bool = False


class ReferenceDataService:
    """
    Service layer that initializes exchange adapters,
    merges their data streams, and maintains the latest snapshot for each exchange.
    """

    def __init__(self, config: Optional[ServiceConfig] = None) -> None:
        """
        Initialize adapters and snapshot storage.

        Args:
            config: Optional ServiceConfig for enabling/disabling exchanges
        """
        self.config = config or ServiceConfig()
        self.adapters: Dict[ExchangeEnum, BaseExchangeAdapter] = {}
        self.latest_snapshot: Dict[ExchangeEnum, StandardExchangeInfo] = {}
        self._unsubscribes: list[Callable[[], None]] = []
        self._bus: EventBus[StandardExchangeInfo] = EventBus()

        # Initialize enabled adapters
        if self.config.enable_binance:
            self.adapters[ExchangeEnum.BINANCE] = BinanceExchangeAdapter(
                is_testnet=self.config.use_testnet
            )
        if self.config.enable_okx:
            self.adapters[ExchangeEnum.OKX] = OKXExchangeAdapter(is_testnet=self.config.use_testnet)
        if self.config.enable_bybit:
            self.adapters[ExchangeEnum.BYBIT] = BybitExchangeAdapter(
                is_testnet=self.config.use_testnet
            )
        if self.config.enable_bitget:
            self.adapters[ExchangeEnum.BITGET] = BitgetExchangeAdapter(
                is_testnet=self.config.use_testnet
            )
        if self.config.enable_coinbase:
            self.adapters[ExchangeEnum.COINBASE] = CoinbaseExchangeAdapter(
                is_testnet=self.config.use_testnet
            )
        if self.config.enable_coinbase_intl:
            self.adapters[ExchangeEnum.COINBASE_INTL] = CoinbaseIntlExchangeAdapter(
                is_testnet=self.config.use_testnet
            )

    def subscribe(
        self, callback: Callable[[StandardExchangeInfo], None]
    ) -> Callable[[], None]:
        """
        Subscribe to all exchange updates.

        Args:
            callback: Function to call when new exchange info is available.

        Returns:
            An unsubscribe function that removes the callback.
        """
        return self._bus.subscribe(callback)

    async def start(self) -> None:
        """
        Start all adapters and subscribe to their updates.
        Updates the latest snapshot as new data arrives.

        Raises:
            ValueError: If no adapters are enabled
        """
        if not self.adapters:
            raise ValueError("No adapters enabled")

        def on_update(info: StandardExchangeInfo) -> None:
            self.latest_snapshot[info.exchange] = info
            self._bus.publish(info)  # Re-broadcast to service subscribers
            logging.info("Updated snapshot for %s", info.exchange.value)

        for adapter in self.adapters.values():
            unsub = adapter.subscribe(on_update)
            self._unsubscribes.append(unsub)

    async def stop(self) -> None:
        """Stop the service by unsubscribing and closing adapters."""
        for unsub in self._unsubscribes:
            unsub()
        self._unsubscribes.clear()
        for adapter in self.adapters.values():
            await adapter.close()

    def get_snapshot(self) -> Dict[ExchangeEnum, StandardExchangeInfo]:
        """
        Retrieve the current latest snapshot for each exchange.

        Returns:
            A dictionary mapping ExchangeEnum to StandardExchangeInfo.
        """
        return self.latest_snapshot
