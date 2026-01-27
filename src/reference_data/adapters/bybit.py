"""
Bybit exchange adapter implementation.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

from ..api.models import ExchangeEnum, InstrumentType, StandardExchangeInfo, TradingPair

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
from ..core.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitOpenError
from ..core.rate_limiter import RateLimiter
from .base import BaseExchangeAdapter
from .config import BYBIT_CONFIG, ExchangeConfig
from .state import ExchangeState


class BybitExchangeAdapter(BaseExchangeAdapter):
    """Adapter for fetching and normalizing exchange info data from Bybit."""

    def __init__(self, config: Optional[ExchangeConfig] = None, is_testnet: bool = False) -> None:
        """
        Initialize the Bybit adapter.

        Args:
            config: Optional configuration, uses BYBIT_CONFIG if not provided
            is_testnet: Whether to use testnet endpoints (default: False)
        """
        super().__init__()
        self.config = config or BYBIT_CONFIG
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
            name=f"{ExchangeEnum.BYBIT.value}",
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
        Convert raw Bybit spot data to TradingPair format.

        Args:
            raw: Raw JSON data from Bybit spot endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.SPOT.
        """
        if raw.get("retCode") != 0:
            error_msg = raw.get("retMsg", "Unknown error")
            raise Exception(f"Bybit API error: {error_msg}")

        result = raw.get("result", {})
        instruments: List[Dict[str, Any]] = result.get("list", [])
        pairs: List[TradingPair] = []

        for inst in instruments:
            if not all(inst.get(field) for field in ["symbol", "baseCoin", "quoteCoin"]):
                continue

            # Extract tick size from priceFilter
            tick_size: Optional[float] = None
            if price_filter := inst.get("priceFilter"):
                if tick_sz := price_filter.get("tickSize"):
                    tick_size = float(tick_sz)

            # Extract lot size from lotSizeFilter (use basePrecision for spot)
            lot_size: Optional[float] = None
            if lot_filter := inst.get("lotSizeFilter"):
                if base_precision := lot_filter.get("basePrecision"):
                    lot_size = float(base_precision)

            pair = TradingPair(
                instrument_type=InstrumentType.SPOT,
                symbol=str(inst["symbol"]),
                base_asset=str(inst["baseCoin"]),
                quote_asset=str(inst["quoteCoin"]),
                status=inst.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
            )
            pairs.append(pair)

        return pairs

    def _convert_perpetual(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Bybit linear perpetual data to TradingPair format.

        Args:
            raw: Raw JSON data from Bybit linear endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.PERPETUAL.
        """
        if raw.get("retCode") != 0:
            error_msg = raw.get("retMsg", "Unknown error")
            raise Exception(f"Bybit API error: {error_msg}")

        result = raw.get("result", {})
        instruments: List[Dict[str, Any]] = result.get("list", [])
        pairs: List[TradingPair] = []

        for inst in instruments:
            if not all(inst.get(field) for field in ["symbol", "baseCoin", "quoteCoin"]):
                continue

            # Extract tick size from priceFilter
            tick_size: Optional[float] = None
            if price_filter := inst.get("priceFilter"):
                if tick_sz := price_filter.get("tickSize"):
                    tick_size = float(tick_sz)

            # Extract lot size from lotSizeFilter (use qtyStep for derivatives)
            lot_size: Optional[float] = None
            if lot_filter := inst.get("lotSizeFilter"):
                if qty_step := lot_filter.get("qtyStep"):
                    lot_size = float(qty_step)

            # Extract expiry from deliveryTime if present (non-perpetual contracts)
            expiry: Optional[str] = None
            contract_type = inst.get("contractType", "")
            if contract_type != "LinearPerpetual" and "deliveryTime" in inst:
                delivery_ms = int(inst["deliveryTime"])
                expiry = datetime.utcfromtimestamp(delivery_ms / 1000).strftime("%Y-%m-%d")

            pair = TradingPair(
                instrument_type=InstrumentType.PERPETUAL,
                symbol=str(inst["symbol"]),
                base_asset=str(inst["baseCoin"]),
                quote_asset=str(inst["quoteCoin"]),
                status=inst.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
                settlement_currency=inst.get("settleCoin"),
                expiry=expiry,
            )
            pairs.append(pair)

        return pairs

    def _convert_futures(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Bybit inverse futures data to TradingPair format.

        Args:
            raw: Raw JSON data from Bybit inverse endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.FUTURES.
        """
        if raw.get("retCode") != 0:
            error_msg = raw.get("retMsg", "Unknown error")
            raise Exception(f"Bybit API error: {error_msg}")

        result = raw.get("result", {})
        instruments: List[Dict[str, Any]] = result.get("list", [])
        pairs: List[TradingPair] = []

        for inst in instruments:
            if not all(inst.get(field) for field in ["symbol", "baseCoin", "quoteCoin"]):
                continue

            # Extract tick size from priceFilter
            tick_size: Optional[float] = None
            if price_filter := inst.get("priceFilter"):
                if tick_sz := price_filter.get("tickSize"):
                    tick_size = float(tick_sz)

            # Extract lot size from lotSizeFilter (use qtyStep for derivatives)
            lot_size: Optional[float] = None
            if lot_filter := inst.get("lotSizeFilter"):
                if qty_step := lot_filter.get("qtyStep"):
                    lot_size = float(qty_step)

            # Extract expiry from deliveryTime if present (non-perpetual contracts)
            expiry: Optional[str] = None
            contract_type = inst.get("contractType", "")
            if contract_type != "InversePerpetual" and "deliveryTime" in inst:
                delivery_ms = int(inst["deliveryTime"])
                expiry = datetime.utcfromtimestamp(delivery_ms / 1000).strftime("%Y-%m-%d")

            pair = TradingPair(
                instrument_type=InstrumentType.FUTURES,
                symbol=str(inst["symbol"]),
                base_asset=str(inst["baseCoin"]),
                quote_asset=str(inst["quoteCoin"]),
                status=inst.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
                settlement_currency=inst.get("settleCoin"),
                expiry=expiry,
            )
            pairs.append(pair)

        return pairs

    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw Bybit data to StandardExchangeInfo format.

        Merges all pairs from self._all_pairs into a single StandardExchangeInfo.

        Args:
            raw: Raw JSON data from Bybit (used for timestamp extraction).

        Returns:
            A StandardExchangeInfo instance with normalized data from all instrument types.
        """
        timestamp: int = int(time.time() * 1000)

        # Merge all trading pairs from all instrument types
        all_pairs: List[TradingPair] = []
        for pairs in self._all_pairs.values():
            all_pairs.extend(pairs)

        return StandardExchangeInfo(
            exchange=ExchangeEnum.BYBIT, timestamp=timestamp, trading_pairs=all_pairs
        )

    async def _fetch_initial_snapshot(self) -> None:
        """
        Fetch the initial exchange info configuration from Bybit via REST.
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
                logging.info("Fetched Bybit snapshot from REST for all instrument types")
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
                        logging.info(f"Fetched {len(pairs)} {instrument_type.value} pairs from Bybit")
                        return  # Success - exit the loop
            except CircuitOpenError:
                # Circuit just opened, will wait on next iteration
                logging.info(f"{self.circuit_breaker.name} circuit breaker opened")
                continue
            except Exception as e:
                logging.error(f"Bybit REST fetch failed for {instrument_type.value}: {e}")

            # Exponential backoff for regular failures (before circuit opens)
            logging.info(f"Retrying {instrument_type.value} REST fetch in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _listen(self) -> None:
        """
        Listen to the Bybit WebSocket endpoint.
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
                logging.error(f"Bybit periodic update failed: {e}")
