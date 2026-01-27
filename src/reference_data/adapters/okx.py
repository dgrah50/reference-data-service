"""
OKX exchange adapter implementation.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from ..api.models import ExchangeEnum, InstrumentType, StandardExchangeInfo, TradingPair

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
from ..core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError
from ..core.rate_limiter import RateLimiter
from .base import BaseExchangeAdapter
from .config import OKX_CONFIG, ExchangeConfig
from .state import ExchangeState


class OKXExchangeAdapter(BaseExchangeAdapter):
    """Adapter for fetching and normalizing exchange info data from OKX."""

    def __init__(self, config: Optional[ExchangeConfig] = None, is_testnet: bool = False) -> None:
        """
        Initialize the OKX adapter.

        Args:
            config: Optional configuration, uses OKX_CONFIG if not provided
            is_testnet: Whether to use testnet endpoints (default: False)
        """
        super().__init__()
        self.config = config or OKX_CONFIG
        self.is_testnet = is_testnet
        self.rest_rate_limiter = RateLimiter(
            max_calls=self.config.rest_rate_limit, period=self.config.rest_rate_period
        )
        self.ws_rate_limiter = RateLimiter(
            max_calls=self.config.ws_rate_limit, period=self.config.ws_rate_period
        )
        self.state = ExchangeState()
        self._last_ping_time = 0.0
        self._all_pairs: Dict[InstrumentType, List[TradingPair]] = {}
        self._circuit_breaker = CircuitBreaker(
            name=f"{ExchangeEnum.OKX.value}",
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
            List of supported InstrumentType values
        """
        return self.config.get_supported_instruments(self.is_testnet)

    def _convert_spot(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw OKX spot data to TradingPair format.

        Args:
            raw: Raw JSON data from OKX spot endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.SPOT.
        """
        data_list: List[Dict[str, Any]] = raw.get("data", [])
        pairs: List[TradingPair] = []

        for inst in data_list:
            if not all(inst.get(field) for field in ["instId", "baseCcy", "quoteCcy"]):
                continue

            pair = TradingPair(
                instrument_type=InstrumentType.SPOT,
                symbol=str(inst["instId"]),
                base_asset=str(inst["baseCcy"]),
                quote_asset=str(inst["quoteCcy"]),
                status=inst.get("state"),
                tick_size=float(inst["tickSz"]) if inst.get("tickSz") else None,
                lot_size=float(inst["lotSz"]) if inst.get("lotSz") else None,
            )
            pairs.append(pair)

        return pairs

    def _convert_perpetual(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw OKX SWAP (perpetual) data to TradingPair format.

        Args:
            raw: Raw JSON data from OKX SWAP endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.PERPETUAL.
        """
        data_list: List[Dict[str, Any]] = raw.get("data", [])
        pairs: List[TradingPair] = []

        for inst in data_list:
            if not all(inst.get(field) for field in ["instId", "baseCcy", "quoteCcy"]):
                continue

            pair = TradingPair(
                instrument_type=InstrumentType.PERPETUAL,
                symbol=str(inst["instId"]),
                base_asset=str(inst["baseCcy"]),
                quote_asset=str(inst["quoteCcy"]),
                status=inst.get("state"),
                tick_size=float(inst["tickSz"]) if inst.get("tickSz") else None,
                lot_size=float(inst["lotSz"]) if inst.get("lotSz") else None,
                contract_size=float(inst["ctVal"]) if inst.get("ctVal") else None,
                settlement_currency=inst.get("settleCcy"),
            )
            pairs.append(pair)

        return pairs

    def _convert_futures(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw OKX FUTURES data to TradingPair format.

        Args:
            raw: Raw JSON data from OKX FUTURES endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.FUTURES.
        """
        data_list: List[Dict[str, Any]] = raw.get("data", [])
        pairs: List[TradingPair] = []

        for inst in data_list:
            if not all(inst.get(field) for field in ["instId", "baseCcy", "quoteCcy"]):
                continue

            # Extract expiry from expTime if present
            expiry: Optional[str] = None
            if "expTime" in inst and inst["expTime"]:
                expiry = datetime.fromtimestamp(int(inst["expTime"]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

            pair = TradingPair(
                instrument_type=InstrumentType.FUTURES,
                symbol=str(inst["instId"]),
                base_asset=str(inst["baseCcy"]),
                quote_asset=str(inst["quoteCcy"]),
                status=inst.get("state"),
                tick_size=float(inst["tickSz"]) if inst.get("tickSz") else None,
                lot_size=float(inst["lotSz"]) if inst.get("lotSz") else None,
                contract_size=float(inst["ctVal"]) if inst.get("ctVal") else None,
                settlement_currency=inst.get("settleCcy"),
                expiry=expiry,
            )
            pairs.append(pair)

        return pairs

    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw OKX data to StandardExchangeInfo format.

        Merges all pairs from self._all_pairs into a single StandardExchangeInfo.

        Args:
            raw: Raw JSON data from OKX (used for timestamp extraction).

        Returns:
            A StandardExchangeInfo instance with normalized data from all instrument types.
        """
        timestamp: int = int(time.time() * 1000)

        # Merge all trading pairs from all instrument types
        all_pairs: List[TradingPair] = []
        for pairs in self._all_pairs.values():
            all_pairs.extend(pairs)

        return StandardExchangeInfo(
            exchange=ExchangeEnum.OKX, timestamp=timestamp, trading_pairs=all_pairs
        )

    async def _fetch_initial_snapshot(self) -> None:
        """
        Fetch the initial exchange info configuration from OKX via REST.
        Fetches data for all supported instrument types in parallel.
        Handles partial failures gracefully.
        """
        supported_instruments = self.get_supported_instruments()
        tasks = []

        for instrument_type in supported_instruments:
            endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
            if endpoint:
                tasks.append(self._fetch_instrument_data(instrument_type, endpoint.rest_url))

        # Fetch all instrument types in parallel, allowing partial failures
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any exceptions but continue with successful fetches
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Failed to fetch instrument data: {result}")

        # After all data is fetched (including partial), emit the merged state
        if self._all_pairs:  # Only emit if we have any data
            if updated_state := self.state.update_state({}, self._to_standard):
                logging.info("Fetched OKX snapshot from REST for all instrument types")
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

                        # Convert data based on instrument type
                        if instrument_type == InstrumentType.SPOT:
                            pairs = self._convert_spot(raw_data)
                        elif instrument_type == InstrumentType.PERPETUAL:
                            pairs = self._convert_perpetual(raw_data)
                        elif instrument_type == InstrumentType.FUTURES:
                            pairs = self._convert_futures(raw_data)
                        else:
                            logging.warning(f"Unknown instrument type: {instrument_type}")
                            return

                        # Store pairs by instrument type
                        self._all_pairs[instrument_type] = pairs
                        logging.info(f"Fetched {len(pairs)} {instrument_type.value} pairs from OKX")
                        return  # Success - exit the loop
            except CircuitOpenError:
                # Circuit just opened, will wait on next iteration
                logging.info(f"{self.circuit_breaker.name} circuit breaker opened")
                continue
            except Exception as e:
                logging.error(f"OKX REST fetch failed for {instrument_type.value}: {e}")

            # Exponential backoff for regular failures (before circuit opens)
            logging.info(f"Retrying {instrument_type.value} REST fetch in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _listen(self) -> None:
        """
        Listen to the OKX WebSocket endpoint.
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
                logging.error(f"OKX periodic update failed: {e}")
