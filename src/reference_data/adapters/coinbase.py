"""
Coinbase exchange adapter implementation.
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
from .config import COINBASE_CONFIG, ExchangeConfig
from .state import ExchangeState


class CoinbaseExchangeAdapter(BaseExchangeAdapter):
    """Adapter for fetching and normalizing exchange info data from Coinbase (spot only)."""

    def __init__(self, config: Optional[ExchangeConfig] = None, is_testnet: bool = False) -> None:
        """
        Initialize the Coinbase adapter.

        Args:
            config: Optional configuration, uses COINBASE_CONFIG if not provided
            is_testnet: Whether to use testnet endpoints (default: False)
        """
        super().__init__()
        self.config = config or COINBASE_CONFIG
        self.is_testnet = is_testnet
        self.rest_rate_limiter = RateLimiter(
            max_calls=self.config.rest_rate_limit, period=self.config.rest_rate_period
        )
        self.state = ExchangeState()
        self.ws_rate_limiter = RateLimiter(
            max_calls=self.config.ws_rate_limit, period=self.config.ws_rate_period
        )
        self._last_ping_time = 0.0
        self._all_pairs: Dict[InstrumentType, List[TradingPair]] = {}
        self._known_symbols: Dict[InstrumentType, set] = {}
        self._circuit_breaker = CircuitBreaker(
            name=f"{ExchangeEnum.COINBASE.value}",
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
            List of supported InstrumentType values (spot only for Coinbase)
        """
        return self.config.get_supported_instruments(self.is_testnet)

    def _convert_spot(self, raw: Dict[str, Any]) -> List[TradingPair]:
        """
        Convert raw Coinbase spot data to TradingPair format.

        Args:
            raw: Raw JSON wrapper containing products array from Coinbase /products endpoint.

        Returns:
            List of TradingPair instances with InstrumentType.SPOT.
        """
        pairs: List[TradingPair] = []

        # Extract the products array from the wrapper
        products = raw.get("products", [])

        for product in products:
            # Skip if missing required fields
            if not all(product.get(field) for field in ["id", "base_currency", "quote_currency"]):
                continue

            # Skip if trading is disabled
            if product.get("trading_disabled", False):
                continue

            # Extract tick_size and lot_size
            tick_size: Optional[float] = None
            lot_size: Optional[float] = None

            if "quote_increment" in product:
                try:
                    tick_size = float(product["quote_increment"])
                except (ValueError, TypeError):
                    pass

            if "base_increment" in product:
                try:
                    lot_size = float(product["base_increment"])
                except (ValueError, TypeError):
                    pass

            pair = TradingPair(
                instrument_type=InstrumentType.SPOT,
                symbol=str(product["id"]),
                base_asset=str(product["base_currency"]),
                quote_asset=str(product["quote_currency"]),
                status=product.get("status"),
                tick_size=tick_size,
                lot_size=lot_size,
            )
            pairs.append(pair)

        return pairs

    def _to_standard(self, raw: Dict[str, Any]) -> StandardExchangeInfo:
        """
        Convert raw Coinbase data to StandardExchangeInfo format.

        Args:
            raw: Raw JSON wrapper (may be empty dict for polling).

        Returns:
            A StandardExchangeInfo instance with normalized data.
        """
        # Use current time as server timestamp (Coinbase doesn't provide one)
        server_time: int = int(time.time() * 1000)

        # Merge all trading pairs from all instrument types
        all_pairs: List[TradingPair] = []
        for pairs in self._all_pairs.values():
            all_pairs.extend(pairs)

        return StandardExchangeInfo(
            exchange=ExchangeEnum.COINBASE, timestamp=server_time, trading_pairs=all_pairs
        )

    async def _fetch_initial_snapshot(self) -> None:
        """
        Fetch the initial exchange info configuration from Coinbase via REST.
        Applies rate limiting and retry logic with exponential backoff.
        """
        supported_instruments = self.get_supported_instruments()

        for instrument_type in supported_instruments:
            endpoint = self.config.get_endpoint(instrument_type, self.is_testnet)
            if endpoint:
                await self._fetch_instrument_data(instrument_type, endpoint.rest_url)

        # After all data is fetched, emit the merged state
        if updated_state := self.state.update_state({}, self._to_standard):
            logging.info("Fetched Coinbase snapshot from REST")
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
                        raw_data: List[Dict[str, Any]] = await resp.json()

                        # Wrap the array in a dict to match the interface
                        wrapped_data: Dict[str, Any] = {"products": raw_data}

                        # Convert data based on instrument type (spot only for Coinbase)
                        if instrument_type == InstrumentType.SPOT:
                            pairs = self._convert_spot(wrapped_data)
                        else:
                            logging.warning(
                                f"Unsupported instrument type for Coinbase: {instrument_type}"
                            )
                            return

                        # Store pairs by instrument type
                        self._all_pairs[instrument_type] = pairs
                        logging.info(
                            f"Fetched {len(pairs)} {instrument_type.value} pairs from Coinbase"
                        )
                        return  # Success - exit the loop
            except CircuitOpenError:
                # Circuit just opened, will wait on next iteration
                logging.info(f"{self.circuit_breaker.name} circuit breaker opened")
                continue
            except Exception as e:
                logging.error(f"Coinbase REST fetch failed for {instrument_type.value}: {e}")

            # Exponential backoff for regular failures (before circuit opens)
            logging.info(f"Retrying {instrument_type.value} REST fetch in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _listen(self) -> None:
        """
        Listen to Coinbase status channel to detect new products.
        First fetches initial snapshot via REST, then subscribes to
        status channel which provides product list updates.
        """
        # Fetch initial snapshot
        await self._fetch_initial_snapshot()

        # Initialize known symbols from initial fetch
        for instrument_type, pairs in self._all_pairs.items():
            self._known_symbols[instrument_type] = {p.symbol for p in pairs}

        # Get WebSocket URL
        endpoint = self.config.get_endpoint(InstrumentType.SPOT, self.is_testnet)
        if not endpoint or not endpoint.ws_url:
            logging.warning("Coinbase WebSocket URL not configured, falling back to polling")
            await self._poll_instrument(InstrumentType.SPOT)
            return

        await self._listen_status_channel(endpoint.ws_url)

    async def _listen_status_channel(self, ws_url: str) -> None:
        """
        Listen to Coinbase status channel for product updates.
        The status channel provides the full list of products with each message.

        Args:
            ws_url: WebSocket URL to connect to
        """
        retry_delay = 1

        while True:
            try:
                await self.ws_rate_limiter.acquire()
                async with self.session.ws_connect(ws_url) as ws:
                    logging.info("Connected to Coinbase status channel at %s", ws_url)
                    retry_delay = 1
                    self._last_ping_time = time.time()

                    # Subscribe to status channel
                    subscribe_msg = {
                        "type": "subscribe",
                        "channels": ["status"],
                    }
                    await ws.send_json(subscribe_msg)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            msg_type = data.get("type")

                            # Handle subscription confirmation
                            if msg_type == "subscriptions":
                                logging.info("Subscribed to Coinbase status channel")
                                continue

                            # Handle error
                            if msg_type == "error":
                                logging.error("Coinbase WebSocket error: %s", data.get("message"))
                                continue

                            # Handle status message - contains products list
                            if msg_type == "status":
                                await self._process_status_message(data)

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error("Coinbase status channel error: %s", ws.exception())
                            break

            except Exception as e:
                logging.error("Coinbase status channel exception: %s", e)

            logging.info(
                "Coinbase status channel disconnected. Reconnecting in %s seconds...",
                retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _process_status_message(self, data: Dict[str, Any]) -> None:
        """
        Process Coinbase status message and detect new products.

        Args:
            data: Status message from WebSocket
        """
        known = self._known_symbols.get(InstrumentType.SPOT, set())
        products = data.get("products", [])

        # Check for new symbols
        new_symbols = set()
        for product in products:
            product_id = product.get("id")
            if product_id and product_id not in known:
                new_symbols.add(product_id)

        if new_symbols:
            logging.info(
                "Detected %d new SPOT products on Coinbase: %s",
                len(new_symbols),
                list(new_symbols)[:5],
            )
            # Trigger REST refresh to get full product details
            endpoint = self.config.get_endpoint(InstrumentType.SPOT, self.is_testnet)
            if endpoint:
                try:
                    await self._fetch_instrument_data(InstrumentType.SPOT, endpoint.rest_url)
                    # Update known symbols
                    if InstrumentType.SPOT in self._all_pairs:
                        self._known_symbols[InstrumentType.SPOT] = {
                            p.symbol for p in self._all_pairs[InstrumentType.SPOT]
                        }
                    # Emit updated state
                    if updated_state := self.state.update_state({}, self._to_standard):
                        self._bus.publish(updated_state)
                except Exception as e:
                    logging.error(f"Failed to refresh SPOT after new product detection: {e}")

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
                logging.error(f"Coinbase {instrument_type.value} poll failed: {e}")
