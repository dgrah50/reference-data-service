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

    def _get_okx_inst_type(self, instrument_type: InstrumentType) -> str:
        """
        Map InstrumentType to OKX instType string.

        Args:
            instrument_type: Internal instrument type

        Returns:
            OKX instType string (SPOT, SWAP, FUTURES)
        """
        mapping = {
            InstrumentType.SPOT: "SPOT",
            InstrumentType.PERPETUAL: "SWAP",
            InstrumentType.FUTURES: "FUTURES",
        }
        return mapping.get(instrument_type, "SPOT")

    def _convert_diff(
        self, instrument_type: InstrumentType, diff_data: Dict[str, Any]
    ) -> List[TradingPair]:
        """
        Convert OKX diff update to TradingPair list.

        Args:
            instrument_type: The type of instrument being updated
            diff_data: Diff data from OKX WebSocket

        Returns:
            List of updated TradingPair instances
        """
        if instrument_type == InstrumentType.SPOT:
            return self._convert_spot(diff_data)
        elif instrument_type == InstrumentType.PERPETUAL:
            return self._convert_perpetual(diff_data)
        elif instrument_type == InstrumentType.FUTURES:
            return self._convert_futures(diff_data)
        return []

    async def _listen(self) -> None:
        """
        Listen to the OKX WebSocket endpoint for all instrument types.
        First fetches initial snapshot via REST, then subscribes to
        WebSocket instruments channel for real-time updates.
        OKX supports subscribing to multiple instTypes on a single connection.
        """
        # Fetch initial snapshot for all instrument types
        await self._fetch_initial_snapshot()

        # Get WebSocket URL (same for all instrument types on OKX)
        endpoint = self.config.get_endpoint(InstrumentType.SPOT, self.is_testnet)
        if not endpoint or not endpoint.ws_url:
            logging.warning("OKX WebSocket URL not configured, falling back to polling")
            await self._poll_all_instruments()
            return

        # Build subscription message for all supported instrument types
        supported = self.get_supported_instruments()
        subscribe_args = [
            {"channel": "instruments", "instType": self._get_okx_inst_type(inst)}
            for inst in supported
        ]
        subscribe_message = {"op": "subscribe", "args": subscribe_args}

        retry_delay = 1

        async def send_ping(ws: aiohttp.ClientWebSocketResponse) -> None:
            """Send periodic pings to keep the connection alive."""
            while True:
                try:
                    await asyncio.sleep(self.config.ping_interval)
                    await ws.send_str("ping")
                    self._last_ping_time = time.time()
                    logging.debug("Sent ping to OKX WebSocket")
                except Exception as e:
                    logging.error("Error sending OKX ping: %s", e)
                    break

        while True:
            try:
                await self.ws_rate_limiter.acquire()
                async with self.session.ws_connect(endpoint.ws_url) as ws:
                    logging.info("Connected to OKX websocket at %s", endpoint.ws_url)
                    await ws.send_json(subscribe_message)
                    retry_delay = 1  # Reset delay on successful connection
                    self._last_ping_time = time.time()

                    # Create ping task to keep OKX connection alive
                    ping_task = asyncio.create_task(send_ping(ws))

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                # Handle pong response (OKX returns "pong" as string)
                                if msg.data == "pong":
                                    continue

                                data: Dict[str, Any] = msg.json()

                                # Handle pong as JSON (some versions)
                                if data.get("event") == "pong":
                                    continue

                                # Handle subscription confirmation
                                if data.get("event") == "subscribe":
                                    channel = data.get("arg", {}).get("channel", "")
                                    inst_type = data.get("arg", {}).get("instType", "")
                                    logging.info(
                                        "OKX WebSocket subscribed to %s/%s", channel, inst_type
                                    )
                                    continue

                                # Handle error events
                                if data.get("event") == "error":
                                    logging.error("OKX WebSocket error: %s", data.get("msg"))
                                    continue

                                # Handle instrument data updates
                                if "data" in data and data.get("arg", {}).get("channel") == "instruments":
                                    okx_inst_type = data.get("arg", {}).get("instType", "")

                                    # Map OKX instType back to InstrumentType
                                    inst_type_map = {
                                        "SPOT": InstrumentType.SPOT,
                                        "SWAP": InstrumentType.PERPETUAL,
                                        "FUTURES": InstrumentType.FUTURES,
                                    }
                                    instrument_type = inst_type_map.get(okx_inst_type)

                                    if instrument_type:
                                        pairs = self._convert_diff(instrument_type, data)
                                        if pairs:
                                            self._all_pairs[instrument_type] = pairs
                                            if updated_state := self.state.update_state(
                                                {}, self._to_standard
                                            ):
                                                self._bus.publish(updated_state)
                                                logging.debug(
                                                    "OKX %s instruments updated: %d pairs",
                                                    okx_inst_type,
                                                    len(pairs),
                                                )

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logging.error("OKX WebSocket error: %s", ws.exception())
                                break
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                logging.error("OKX websocket connection exception: %s", e)

            logging.info(
                "OKX websocket connection lost. Reconnecting in %s seconds...", retry_delay
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _poll_all_instruments(self) -> None:
        """
        Fallback polling for all instruments when WebSocket is unavailable.
        """
        while True:
            await asyncio.sleep(300)
            try:
                await self._fetch_initial_snapshot()
            except Exception as e:
                logging.error(f"OKX periodic update failed: {e}")
