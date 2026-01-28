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
        self._last_ping_time = 0.0
        self._all_pairs: Dict[InstrumentType, List[TradingPair]] = {}
        self._known_symbols: Dict[InstrumentType, set] = {}
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
        Listen to Coinbase International instruments channel.
        First fetches initial snapshot via REST, then subscribes to
        instruments channel for real-time updates.
        """
        # Fetch initial snapshot
        await self._fetch_initial_snapshot()

        # Initialize known symbols from initial fetch
        for instrument_type, pairs in self._all_pairs.items():
            self._known_symbols[instrument_type] = {p.symbol for p in pairs}

        # Get WebSocket URL
        endpoint = self.config.get_endpoint(InstrumentType.PERPETUAL, self.is_testnet)
        if not endpoint or not endpoint.ws_url:
            logging.warning(
                "Coinbase International WebSocket URL not configured, falling back to polling"
            )
            await self._poll_instrument(InstrumentType.PERPETUAL)
            return

        await self._listen_instruments_channel(endpoint.ws_url)

    async def _listen_instruments_channel(self, ws_url: str) -> None:
        """
        Listen to Coinbase International instruments channel.
        Provides direct instrument updates for perpetuals.

        Args:
            ws_url: WebSocket URL to connect to
        """
        retry_delay = 1

        async def send_ping(ws: aiohttp.ClientWebSocketResponse) -> None:
            """Send periodic pings to keep the connection alive."""
            while True:
                try:
                    await asyncio.sleep(self.config.ping_interval)
                    await ws.send_json({"type": "ping"})
                    self._last_ping_time = time.time()
                except Exception as e:
                    logging.error("Error sending Coinbase Intl ping: %s", e)
                    break

        while True:
            try:
                await self.ws_rate_limiter.acquire()
                async with self.session.ws_connect(ws_url) as ws:
                    logging.info(
                        "Connected to Coinbase International instruments channel at %s", ws_url
                    )
                    retry_delay = 1
                    self._last_ping_time = time.time()

                    # Subscribe to instruments channel
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": "instruments",
                    }
                    await ws.send_json(subscribe_msg)

                    # Start ping task
                    ping_task = asyncio.create_task(send_ping(ws))

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.json()
                                msg_type = data.get("type")

                                # Handle pong response
                                if msg_type == "pong":
                                    continue

                                # Handle subscription confirmation
                                if msg_type == "subscriptions":
                                    logging.info(
                                        "Subscribed to Coinbase International instruments channel"
                                    )
                                    continue

                                # Handle error
                                if msg_type == "error":
                                    logging.error(
                                        "Coinbase Intl WebSocket error: %s", data.get("message")
                                    )
                                    continue

                                # Handle instruments update
                                if msg_type == "instruments" or "instruments" in data:
                                    await self._process_instruments_message(data)

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logging.error(
                                    "Coinbase Intl instruments channel error: %s", ws.exception()
                                )
                                break
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                logging.error("Coinbase International instruments channel exception: %s", e)

            logging.info(
                "Coinbase Intl instruments channel disconnected. Reconnecting in %s seconds...",
                retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _process_instruments_message(self, data: Dict[str, Any]) -> None:
        """
        Process Coinbase International instruments message.

        Args:
            data: Instruments message from WebSocket
        """
        known = self._known_symbols.get(InstrumentType.PERPETUAL, set())
        instruments = data.get("instruments", [])

        # Check for new symbols
        new_symbols = set()
        for instrument in instruments:
            symbol = instrument.get("symbol")
            # Only track PERP instruments
            if symbol and instrument.get("type") == "PERP" and symbol not in known:
                new_symbols.add(symbol)

        if new_symbols:
            logging.info(
                "Detected %d new PERPETUAL instruments on Coinbase International: %s",
                len(new_symbols),
                list(new_symbols)[:5],
            )
            # Trigger REST refresh to get full instrument details
            endpoint = self.config.get_endpoint(InstrumentType.PERPETUAL, self.is_testnet)
            if endpoint:
                try:
                    await self._fetch_instrument_data(InstrumentType.PERPETUAL, endpoint.rest_url)
                    # Update known symbols
                    if InstrumentType.PERPETUAL in self._all_pairs:
                        self._known_symbols[InstrumentType.PERPETUAL] = {
                            p.symbol for p in self._all_pairs[InstrumentType.PERPETUAL]
                        }
                    # Emit updated state
                    if updated_state := self.state.update_state({}, self._to_standard):
                        self._bus.publish(updated_state)
                except Exception as e:
                    logging.error(
                        f"Failed to refresh PERPETUAL after new instrument detection: {e}"
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
                logging.error(f"Coinbase Intl {instrument_type.value} poll failed: {e}")
