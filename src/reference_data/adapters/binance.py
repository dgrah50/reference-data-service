"""
Binance exchange adapter implementation.
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
from .config import BINANCE_CONFIG, ExchangeConfig
from .state import ExchangeState


class BinanceExchangeAdapter(BaseExchangeAdapter):
    """Adapter for fetching and normalizing exchange info data from Binance."""

    def __init__(self, config: Optional[ExchangeConfig] = None, is_testnet: bool = False) -> None:
        """
        Initialize the Binance adapter.

        Args:
            config: Optional configuration, uses BINANCE_CONFIG if not provided
            is_testnet: Whether to use testnet endpoints (default: False)
        """
        super().__init__()
        self.config = config or BINANCE_CONFIG
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
        self._known_symbols: Dict[InstrumentType, set] = {}
        self._circuit_breaker = CircuitBreaker(
            name=f"{ExchangeEnum.BINANCE.value}",
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
        Convert raw Binance spot data to TradingPair format.

        Args:
            raw: Raw JSON data from Binance spot endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.SPOT.
        """
        if "result" in raw:
            raw = raw["result"]

        symbols: List[Dict[str, Any]] = raw.get("symbols", [])
        pairs: List[TradingPair] = []

        for symbol in symbols:
            if not all(symbol.get(field) for field in ["symbol", "baseAsset", "quoteAsset"]):
                continue

            tick_size: Optional[float] = None
            lot_size: Optional[float] = None

            for f in symbol.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", "0.0"))
                if f.get("filterType") == "LOT_SIZE":
                    lot_size = float(f.get("stepSize", "0.0"))

            pair = TradingPair(
                instrument_type=InstrumentType.SPOT,
                symbol=str(symbol["symbol"]),
                base_asset=str(symbol["baseAsset"]),
                quote_asset=str(symbol["quoteAsset"]),
                status=symbol.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
            )
            pairs.append(pair)

        return pairs

    def _convert_perpetual(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Binance USD-M perpetual data to TradingPair format.

        Args:
            raw: Raw JSON data from Binance USD-M futures endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.PERPETUAL.
        """
        if "result" in raw:
            raw = raw["result"]

        symbols: List[Dict[str, Any]] = raw.get("symbols", [])
        pairs: List[TradingPair] = []

        for symbol in symbols:
            if not all(symbol.get(field) for field in ["symbol", "baseAsset", "quoteAsset"]):
                continue

            tick_size: Optional[float] = None
            lot_size: Optional[float] = None

            for f in symbol.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", "0.0"))
                if f.get("filterType") == "LOT_SIZE":
                    lot_size = float(f.get("stepSize", "0.0"))

            # Extract expiry from deliveryDate if present (non-perpetual contracts)
            expiry: Optional[str] = None
            if symbol.get("contractType") != "PERPETUAL" and "deliveryDate" in symbol:
                delivery_ms = symbol["deliveryDate"]
                expiry = datetime.fromtimestamp(delivery_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

            pair = TradingPair(
                instrument_type=InstrumentType.PERPETUAL,
                symbol=str(symbol["symbol"]),
                base_asset=str(symbol["baseAsset"]),
                quote_asset=str(symbol["quoteAsset"]),
                status=symbol.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
                contract_size=float(symbol.get("contractSize", 1.0)),
                settlement_currency=symbol.get("marginAsset"),
                expiry=expiry,
            )
            pairs.append(pair)

        return pairs

    def _convert_futures(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Binance COIN-M futures data to TradingPair format.

        Args:
            raw: Raw JSON data from Binance COIN-M futures endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.FUTURES.
        """
        if "result" in raw:
            raw = raw["result"]

        symbols: List[Dict[str, Any]] = raw.get("symbols", [])
        pairs: List[TradingPair] = []

        for symbol in symbols:
            if not all(symbol.get(field) for field in ["symbol", "baseAsset", "quoteAsset"]):
                continue

            tick_size: Optional[float] = None
            lot_size: Optional[float] = None

            for f in symbol.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", "0.0"))
                if f.get("filterType") == "LOT_SIZE":
                    lot_size = float(f.get("stepSize", "0.0"))

            # Extract expiry from deliveryDate if present (non-perpetual contracts)
            expiry: Optional[str] = None
            if symbol.get("contractType") != "PERPETUAL" and "deliveryDate" in symbol:
                delivery_ms = symbol["deliveryDate"]
                expiry = datetime.fromtimestamp(delivery_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

            pair = TradingPair(
                instrument_type=InstrumentType.FUTURES,
                symbol=str(symbol["symbol"]),
                base_asset=str(symbol["baseAsset"]),
                quote_asset=str(symbol["quoteAsset"]),
                status=symbol.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
                contract_size=float(symbol.get("contractSize", 1.0)),
                settlement_currency=symbol.get("marginAsset"),
                expiry=expiry,
            )
            pairs.append(pair)

        return pairs

    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw Binance data to StandardExchangeInfo format.

        Merges all pairs from self._all_pairs into a single StandardExchangeInfo.

        Args:
            raw: Raw JSON data from Binance (used for timestamp extraction).

        Returns:
            A StandardExchangeInfo instance with normalized data from all instrument types.
        """
        if "result" in raw:
            raw = raw["result"]

        server_time: int = raw.get("serverTime", int(time.time() * 1000))

        # Merge all trading pairs from all instrument types
        all_pairs: List[TradingPair] = []
        for pairs in self._all_pairs.values():
            all_pairs.extend(pairs)

        return StandardExchangeInfo(
            exchange=ExchangeEnum.BINANCE, timestamp=server_time, trading_pairs=all_pairs
        )


    async def _fetch_initial_snapshot(self) -> None:
        """
        Fetch the initial exchange info configuration from Binance via REST.
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
                logging.info("Fetched Binance snapshot from REST for all instrument types")
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
                        logging.info(f"Fetched {len(pairs)} {instrument_type.value} pairs from Binance")
                        return  # Success - exit the loop
            except CircuitOpenError:
                # Circuit just opened, will wait on next iteration
                logging.info(f"{self.circuit_breaker.name} circuit breaker opened")
                continue
            except Exception as e:
                logging.error(f"Binance REST fetch failed for {instrument_type.value}: {e}")

            # Exponential backoff for regular failures (before circuit opens)
            logging.info(f"Retrying {instrument_type.value} REST fetch in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _listen(self) -> None:
        """
        Listen to Binance ticker streams to detect new symbols.
        First fetches initial snapshot via REST for all instrument types,
        then subscribes to !miniTicker@arr streams to detect new listings.
        When a new symbol is detected, triggers REST refresh.
        """
        # Fetch initial snapshot for all instrument types
        await self._fetch_initial_snapshot()

        # Initialize known symbols from initial fetch
        for instrument_type, pairs in self._all_pairs.items():
            self._known_symbols[instrument_type] = {p.symbol for p in pairs}

        # Start ticker listeners for all supported instrument types
        supported = self.get_supported_instruments()
        tasks = [
            asyncio.create_task(self._listen_ticker_stream(instrument_type))
            for instrument_type in supported
        ]

        if tasks:
            await asyncio.gather(*tasks)

    async def _listen_ticker_stream(self, instrument_type: InstrumentType) -> None:
        """
        Listen to Binance !miniTicker@arr stream for an instrument type.
        Detects new symbols and triggers REST refresh when found.

        Args:
            instrument_type: The instrument type to monitor
        """
        endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
        if not endpoint or not endpoint.ws_url:
            logging.warning(
                f"Binance {instrument_type.value} WebSocket URL not configured, falling back to polling"
            )
            await self._poll_instrument(instrument_type)
            return

        # Construct the stream URL with !miniTicker@arr
        stream_url = f"{endpoint.ws_url}/!miniTicker@arr"
        retry_delay = 1

        while True:
            try:
                await self.ws_rate_limiter.acquire()
                async with self.session.ws_connect(
                    stream_url, max_msg_size=1024 * 1024 * 16
                ) as ws:
                    logging.info(
                        "Connected to Binance %s ticker stream at %s",
                        instrument_type.value,
                        stream_url,
                    )
                    retry_delay = 1
                    self._last_ping_time = time.time()

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()

                            # !miniTicker@arr returns an array of ticker objects
                            if isinstance(data, list):
                                await self._process_ticker_data(instrument_type, data)

                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong(msg.data)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error(
                                "Binance %s ticker stream error: %s",
                                instrument_type.value,
                                ws.exception(),
                            )
                            break

            except Exception as e:
                logging.error(
                    "Binance %s ticker stream exception: %s", instrument_type.value, e
                )

            logging.info(
                "Binance %s ticker stream disconnected. Reconnecting in %s seconds...",
                instrument_type.value,
                retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _process_ticker_data(
        self, instrument_type: InstrumentType, tickers: List[Dict[str, Any]]
    ) -> None:
        """
        Process ticker data and detect new symbols.

        Args:
            instrument_type: The instrument type being processed
            tickers: List of ticker objects from the stream
        """
        known = self._known_symbols.get(instrument_type, set())
        new_symbols = set()

        for ticker in tickers:
            symbol = ticker.get("s")  # 's' is symbol in miniTicker format
            if symbol and symbol not in known:
                new_symbols.add(symbol)

        if new_symbols:
            logging.info(
                "Detected %d new %s symbols on Binance: %s",
                len(new_symbols),
                instrument_type.value,
                list(new_symbols)[:5],  # Log first 5
            )
            # Trigger REST refresh to get full instrument details
            endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
            if endpoint:
                try:
                    await self._fetch_instrument_data(instrument_type, endpoint.rest_url)
                    # Update known symbols
                    if instrument_type in self._all_pairs:
                        self._known_symbols[instrument_type] = {
                            p.symbol for p in self._all_pairs[instrument_type]
                        }
                    # Emit updated state
                    if updated_state := self.state.update_state({}, self._to_standard):
                        self._bus.publish(updated_state)
                except Exception as e:
                    logging.error(
                        f"Failed to refresh {instrument_type.value} after new symbol detection: {e}"
                    )

    async def _poll_instrument(self, instrument_type: InstrumentType) -> None:
        """
        Fallback polling for a single instrument type when WebSocket unavailable.

        Args:
            instrument_type: The instrument type to poll
        """
        while True:
            await asyncio.sleep(300)
            try:
                endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
                if endpoint:
                    await self._fetch_instrument_data(instrument_type, endpoint.rest_url)
                    if self._all_pairs:
                        if updated_state := self.state.update_state({}, self._to_standard):
                            self._bus.publish(updated_state)
            except Exception as e:
                logging.error(f"Binance {instrument_type.value} poll failed: {e}")
