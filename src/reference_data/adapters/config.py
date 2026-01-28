"""
Configuration classes for exchange adapters.

This module provides a hierarchical configuration system supporting
mainnet/testnet × spot/perpetual/futures endpoints for multiple exchanges.
"""

from dataclasses import dataclass
from typing import Optional

from ..api.models import InstrumentType


@dataclass
class EndpointConfig:
    """Configuration for a single endpoint (REST + WebSocket)."""

    rest_url: str
    ws_url: str


@dataclass
class InstrumentEndpoints:
    """Endpoints for different instrument types."""

    spot: Optional[EndpointConfig] = None
    perpetual: Optional[EndpointConfig] = None
    futures: Optional[EndpointConfig] = None


@dataclass
class ExchangeConfig:
    """Exchange configuration with mainnet/testnet support."""

    mainnet: InstrumentEndpoints
    testnet: InstrumentEndpoints
    rest_rate_limit: int = 5
    rest_rate_period: float = 1.0
    ws_rate_limit: int = 5
    ws_rate_period: float = 1.0
    ping_interval: float = 15.0
    ping_timeout: float = 60.0

    def get_endpoint(
        self, instrument_type: InstrumentType, is_testnet: bool = False
    ) -> Optional[EndpointConfig]:
        """
        Get endpoint configuration for a specific instrument type.

        Args:
            instrument_type: The type of instrument (spot/perpetual/futures)
            is_testnet: Whether to use testnet endpoints

        Returns:
            EndpointConfig if supported, None otherwise
        """
        endpoints = self.testnet if is_testnet else self.mainnet
        return getattr(endpoints, instrument_type.value, None)

    def get_supported_instruments(self, is_testnet: bool = False) -> list[InstrumentType]:
        """
        Get list of supported instrument types.

        Args:
            is_testnet: Whether to check testnet or mainnet endpoints

        Returns:
            List of supported InstrumentType values
        """
        endpoints = self.testnet if is_testnet else self.mainnet
        supported = []
        if endpoints.spot:
            supported.append(InstrumentType.SPOT)
        if endpoints.perpetual:
            supported.append(InstrumentType.PERPETUAL)
        if endpoints.futures:
            supported.append(InstrumentType.FUTURES)
        return supported


# ============================
# Exchange Configuration Constants
# ============================

BINANCE_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api.binance.com/api/v3/exchangeInfo",
            ws_url="wss://stream.binance.com:9443/ws",
        ),
        perpetual=EndpointConfig(
            rest_url="https://fapi.binance.com/fapi/v1/exchangeInfo",
            ws_url="wss://fstream.binance.com/ws",
        ),
        futures=EndpointConfig(
            rest_url="https://dapi.binance.com/dapi/v1/exchangeInfo",
            ws_url="wss://dstream.binance.com/ws",
        ),
    ),
    testnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://testnet.binance.vision/api/v3/exchangeInfo",
            ws_url="wss://testnet.binance.vision/ws",
        ),
        perpetual=EndpointConfig(
            rest_url="https://testnet.binancefuture.com/fapi/v1/exchangeInfo",
            ws_url="wss://stream.binancefuture.com/ws",
        ),
        futures=EndpointConfig(
            rest_url="https://testnet.binancefuture.com/dapi/v1/exchangeInfo",
            ws_url="wss://dstream.binancefuture.com/ws",
        ),
    ),
    rest_rate_limit=10,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=55.0,
)

OKX_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=SPOT",
            ws_url="wss://ws.okx.com:8443/ws/v5/public",
        ),
        perpetual=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            ws_url="wss://ws.okx.com:8443/ws/v5/public",
        ),
        futures=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=FUTURES",
            ws_url="wss://ws.okx.com:8443/ws/v5/public",
        ),
    ),
    testnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=SPOT",
            ws_url="wss://wspap.okx.com:8443/ws/v5/public",
        ),
        perpetual=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=SWAP",
            ws_url="wss://wspap.okx.com:8443/ws/v5/public",
        ),
        futures=EndpointConfig(
            rest_url="https://www.okx.com/api/v5/public/instruments?instType=FUTURES",
            ws_url="wss://wspap.okx.com:8443/ws/v5/public",
        ),
    ),
    rest_rate_limit=5,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=30.0,
)

BYBIT_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api.bybit.com/v5/market/instruments-info?category=spot",
            ws_url="wss://stream.bybit.com/v5/public/spot",
        ),
        perpetual=EndpointConfig(
            rest_url="https://api.bybit.com/v5/market/instruments-info?category=linear",
            ws_url="wss://stream.bybit.com/v5/public/linear",
        ),
        futures=EndpointConfig(
            rest_url="https://api.bybit.com/v5/market/instruments-info?category=inverse",
            ws_url="wss://stream.bybit.com/v5/public/inverse",
        ),
    ),
    testnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api-testnet.bybit.com/v5/market/instruments-info?category=spot",
            ws_url="wss://stream-testnet.bybit.com/v5/public/spot",
        ),
        perpetual=EndpointConfig(
            rest_url="https://api-testnet.bybit.com/v5/market/instruments-info?category=linear",
            ws_url="wss://stream-testnet.bybit.com/v5/public/linear",
        ),
        futures=EndpointConfig(
            rest_url="https://api-testnet.bybit.com/v5/market/instruments-info?category=inverse",
            ws_url="wss://stream-testnet.bybit.com/v5/public/inverse",
        ),
    ),
    rest_rate_limit=5,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=60.0,
)

BITGET_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/spot/public/symbols",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
        perpetual=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
        futures=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/mix/market/contracts?productType=COIN-FUTURES",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
    ),
    testnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/spot/public/symbols",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
        perpetual=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
        futures=EndpointConfig(
            rest_url="https://api.bitget.com/api/v2/mix/market/contracts?productType=COIN-FUTURES",
            ws_url="wss://ws.bitget.com/v2/ws/public",
        ),
    ),
    rest_rate_limit=5,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=60.0,
)

COINBASE_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api.exchange.coinbase.com/products",
            ws_url="wss://ws-feed.exchange.coinbase.com",
        ),
        perpetual=None,
        futures=None,
    ),
    testnet=InstrumentEndpoints(
        spot=EndpointConfig(
            rest_url="https://api-public.sandbox.exchange.coinbase.com/products",
            ws_url="wss://ws-feed-public.sandbox.exchange.coinbase.com",
        ),
        perpetual=None,
        futures=None,
    ),
    rest_rate_limit=5,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=60.0,
)

COINBASE_INTL_CONFIG = ExchangeConfig(
    mainnet=InstrumentEndpoints(
        spot=None,
        perpetual=EndpointConfig(
            rest_url="https://api.international.coinbase.com/api/v1/instruments",
            ws_url="wss://ws-feed.international.coinbase.com",
        ),
        futures=None,
    ),
    testnet=InstrumentEndpoints(
        spot=None,
        perpetual=EndpointConfig(
            rest_url="https://api-n5e1.coinbase.com/api/v1/instruments",
            ws_url="wss://ws-feed-n5e1.coinbase.com",
        ),
        futures=None,
    ),
    rest_rate_limit=5,
    rest_rate_period=1.0,
    ws_rate_limit=5,
    ws_rate_period=1.0,
    ping_interval=15.0,
    ping_timeout=60.0,
)


# ============================
# Legacy Classes for Backward Compatibility
# ============================


class BinanceConfig:
    """
    Legacy configuration for Binance adapter.

    Maintained for backward compatibility. New code should use BINANCE_CONFIG directly.
    """

    def __init__(self, is_testnet: bool = False) -> None:
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet)
        if endpoint is None:
            raise ValueError("Binance spot endpoint not configured")

        self.rest_url = endpoint.rest_url
        self.ws_url = endpoint.ws_url
        self.rest_rate_limit = BINANCE_CONFIG.rest_rate_limit
        self.rest_rate_period = BINANCE_CONFIG.rest_rate_period
        self.ws_rate_limit = BINANCE_CONFIG.ws_rate_limit
        self.ws_rate_period = BINANCE_CONFIG.ws_rate_period
        self.ping_interval = BINANCE_CONFIG.ping_interval
        self.ping_timeout = BINANCE_CONFIG.ping_timeout


class OKXConfig:
    """
    Legacy configuration for OKX adapter.

    Maintained for backward compatibility. New code should use OKX_CONFIG directly.
    """

    def __init__(self, is_testnet: bool = False) -> None:
        endpoint = OKX_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet)
        if endpoint is None:
            raise ValueError("OKX spot endpoint not configured")

        self.rest_url = endpoint.rest_url
        self.ws_url = endpoint.ws_url
        self.rest_rate_limit = OKX_CONFIG.rest_rate_limit
        self.rest_rate_period = OKX_CONFIG.rest_rate_period
        self.ws_rate_limit = OKX_CONFIG.ws_rate_limit
        self.ws_rate_period = OKX_CONFIG.ws_rate_period
        self.ping_interval = OKX_CONFIG.ping_interval
        self.ping_timeout = OKX_CONFIG.ping_timeout
