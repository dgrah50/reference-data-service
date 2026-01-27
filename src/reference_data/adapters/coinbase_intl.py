"""
Coinbase International exchange adapter implementation.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

from ..api.models import ExchangeEnum, InstrumentType, StandardExchangeInfo, TradingPair

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
from ..core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError
from ..core.rate_limiter import RateLimiter
from .base import BaseExchangeAdapter
from .config import COINBASE_INTL_CONFIG, ExchangeConfig
from .state import ExchangeState


class CoinbaseIntlExchangeAdapter(BaseExchangeAdapter):
    """Adapter for fetching and normalizing exchange info data from Coinbase International."""

    def __init__(self, config: Optional[ExchangeConfig] = None, is_testnet: bool = False) -> None:
        """
        Initialize the Coinbase International adapter.

        Args:
            config: Optional configuration, uses COINBASE_INTL_CONFIG if not provided
            is_testnet: Whether to use testnet endpoints (default: False)
        """
        super().__init__()
        self.config = config or COINBASE_INTL_CONFIG
        self.is_testnet = is_testnet
        self.rest_rate_limiter = RateLimiter(
            max_calls=self.config.rest_rate_limit, period=self.config.rest_rate_period
        )
        self.ws_rate_limiter = RateLimiter(
            max_calls=self.config.ws_rate_limit, period=self.config.ws_rate_period
        )
        self.state = ExchangeState()
        self._all_pairs: Dict[InstrumentType, List[TradingPair]] = {}
        self._circuit_breaker = CircuitBreaker(
            name=f"{ExchangeEnum.COINBASE_INTL.value}",
            config=CircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=2,
                timeout=60.0,
            )
        )

    def get_supported_instruments(self) -> List[InstrumentType]:
        """
        Get list of supported instrument types for this adapter.

        Returns:
            List of supported InstrumentType values (perpetual only)
        """
        return self.config.get_supported_instruments(self.is_testnet)

    def _convert_perpetual(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Coinbase International perpetual data to TradingPair format.

        Args:
            raw: Raw JSON data from Coinbase International instruments endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.PERPETUAL.
        """
        instruments: List[Dict[str, Any]] = raw.get("instruments", [])
        pairs: List[TradingPair] = []

        for instrument in instruments:
            # Only process PERP type instruments
            if instrument.get("type") != "PERP":
                continue

            # Validate required fields
            if not all(
                instrument.get(field) for field in ["symbol", "base_asset_name", "quote_asset_name"]
            ):
                continue

            # Extract tick_size from quote_increment
            tick_size: Optional[float] = None
            if "quote_increment" in instrument:
                tick_size = float(instrument["quote_increment"])

            # Extract lot_size from base_increment
            lot_size: Optional[float] = None
            if "base_increment" in instrument:
                lot_size = float(instrument["base_increment"])

            # Settlement currency is quote asset for USDC-margined perpetuals
            settlement_currency = instrument.get("quote_asset_name")

            # Contract size defaults to 1.0 for USDC-margined perpetuals
            contract_size = 1.0

            pair = TradingPair(
                instrument_type=InstrumentType.PERPETUAL,
                symbol=str(instrument["symbol"]),
                base_asset=str(instrument["base_asset_name"]),
                quote_asset=str(instrument["quote_asset_name"]),
                status=instrument.get("trading_state"),
                tick_size=tick_size,
                lot_size=lot_size,
                contract_size=contract_size,
                settlement_currency=settlement_currency,
                expiry=None,  # Perpetuals have no expiry
            )
            pairs.append(pair)

        return pairs

    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw Coinbase International data to StandardExchangeInfo format.

        Merges all pairs from self._all_pairs into a single StandardExchangeInfo.

        Args:
            raw: Raw JSON data from Coinbase International (used for timestamp extraction).

        Returns:
            A StandardExchangeInfo instance with normalized data from all instrument types.
        """
        # Use current timestamp in milliseconds if not provided
        server_time: int = int(time.time() * 1000)

        # Merge all trading pairs from all instrument types
        all_pairs: List[TradingPair] = []
        for pairs in self._all_pairs.values():
            all_pairs.extend(pairs)

        return StandardExchangeInfo(
            exchange=ExchangeEnum.COINBASE_INTL, timestamp=server_time, trading_pairs=all_pairs
        )

    async def _fetch_initial_snapshot(self) -> None:
        """
        Fetch the initial exchange info configuration from Coinbase International via REST.
        Fetches data for all supported instrument types (perpetual only).
        Applies rate limiting and retry logic with exponential backoff.
        """
        supported_instruments = self.get_supported_instruments()
        tasks = []

        for instrument_type in supported_instruments:
            endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
            if endpoint:
                tasks.append(self._fetch_instrument_data(instrument_type, endpoint.rest_url))

        # Fetch all instrument types in parallel
        await asyncio.gather(*tasks)

        # After all data is fetched, emit the merged state
        if updated_state := self.state.update_state({}, self._to_standard):
            logging.info(
                "Fetched Coinbase International snapshot from REST for all instrument types"
            )
            self._bus.publish(updated_state)

    async def _fetch_instrument_data(self, instrument_type: InstrumentType, rest_url: str) -> None:
        """
        Fetch data for a specific instrument type.
        Retries indefinitely with circuit breaker protection.

        Args:
            instrument_type: The type of instrument to fetch
            rest_url: The REST endpoint URL for this instrument type
        """
        retry_delay = 1

        while True:
            # If circuit breaker is open, wait for it to transition to half-open
            if not self.circuit_breaker.is_available:
                logging.info(f"{self.circuit_breaker.name} circuit breaker is open, waiting 60s...")
                await asyncio.sleep(60)
                continue

            try:
                async with self.circuit_breaker:
                    await self.rest_rate_limiter.acquire()
                    async with self.session.get(rest_url, timeout=REQUEST_TIMEOUT) as resp:
                        resp.raise_for_status()
                        raw_data: Dict[str, Any] = await resp.json()

                        # Convert data based on instrument type (perpetual only)
                        if instrument_type == InstrumentType.PERPETUAL:
                            pairs = self._convert_perpetual(raw_data)
                        else:
                            logging.warning(f"Unknown instrument type: {instrument_type}")
                            return

                        # Store pairs by instrument type
                        self._all_pairs[instrument_type] = pairs
                        logging.info(
                            f"Fetched {len(pairs)} {instrument_type.value} pairs from Coinbase International"
                        )
                        return  # Success - exit the loop
            except CircuitOpenError:
                # Circuit just opened, will wait on next iteration
                logging.info(f"{self.circuit_breaker.name} circuit breaker opened")
                continue
            except Exception as e:
                logging.error(
                    f"Coinbase International REST fetch failed for {instrument_type.value}: {e}"
                )

            # Exponential backoff for regular failures (before circuit opens)
            logging.info(f"Retrying {instrument_type.value} REST fetch in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _listen(self) -> None:
        """
        Listen to the Coinbase International API.
        First fetches initial snapshot via REST for all instrument types,
        then polls every 300 seconds to refresh the data.
        Applies rate limiting and retry logic with exponential backoff.
        """
        # Fetch initial snapshot for all instrument types
        await self._fetch_initial_snapshot()

        # Poll every 300 seconds
        while True:
            await asyncio.sleep(300)
            try:
                await self._fetch_initial_snapshot()
            except Exception as e:
                logging.error(f"Coinbase International periodic update failed: {e}")
