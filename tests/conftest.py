"""
Test fixtures for the reference data service.
"""

from typing import AsyncGenerator, Generator

import pytest
from fastapi.testclient import TestClient

from reference_data.adapters.config import BinanceConfig, OKXConfig
from reference_data.api.app import app
from reference_data.core.service import ReferenceDataService


@pytest.fixture
def test_client() -> Generator[TestClient, None, None]:
    """
    Fixture to create a FastAPI test client.

    Yields:
        A FastAPI TestClient instance.
    """
    with TestClient(app) as client:
        yield client


@pytest.fixture
async def service() -> AsyncGenerator[ReferenceDataService, None]:
    """
    Fixture to create and manage a ReferenceDataService instance.

    Yields:
        A ReferenceDataService instance.
    """
    service = ReferenceDataService()
    await service.start()
    yield service
    await service.stop()


@pytest.fixture
def okx_config():
    """Fixture providing test configuration for OKX exchange."""
    return OKXConfig(is_testnet=True)


@pytest.fixture
def binance_config():
    """Fixture providing test configuration for Binance exchange."""
    return BinanceConfig(is_testnet=True)


@pytest.fixture
def okx_rest_data():
    """Fixture providing sample OKX REST API response data."""
    return {
        "code": "0",
        "data": [
            {
                "instId": "BTC-USDT",
                "baseCcy": "BTC",
                "quoteCcy": "USDT",
                "state": "live",
                "tickSz": "0.1",
                "lotSz": "0.00001",
                "listTime": "1625097600000",
            }
        ],
        "msg": "",
    }


@pytest.fixture
def okx_ws_messages():
    """Fixture providing sample OKX WebSocket messages."""
    return [
        {"event": "subscribe", "arg": {"channel": "instruments", "instType": "SPOT"}},
        {
            "arg": {"channel": "instruments", "instType": "SPOT"},
            "data": [
                {
                    "instId": "BTC-USDT",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.00001",
                    "listTime": "1625097600000",
                }
            ],
        },
    ]


@pytest.fixture
def mock_session(mocker):
    """Fixture providing a mock aiohttp ClientSession."""
    mock = mocker.MagicMock()
    mock.closed = False

    get_context = mocker.MagicMock()
    mock.get.return_value = get_context

    ws_context = mocker.MagicMock()
    mock.ws_connect.return_value = ws_context

    return mock


@pytest.fixture
def binance_rest_data():
    """Fixture providing sample Binance REST API response data."""
    return {
        "serverTime": 1625097600000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00001"},
                ],
            }
        ],
    }


@pytest.fixture
def binance_ws_messages():
    """Fixture providing sample Binance WebSocket messages."""
    return [
        # Subscription confirmation message
        {"id": "test-id", "status": "ok", "method": "exchangeInfo.subscribe"},
        # Data update message
        {
            "stream": "exchangeInfo",
            "data": {
                "serverTime": 1625097600000,
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.00001"},
                        ],
                    }
                ],
            },
        },
    ]


@pytest.fixture
def binance_perpetual_data():
    """Sample Binance USD-M futures REST API response."""
    return {
        "serverTime": 1625097600000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "status": "TRADING",
                "contractSize": 1,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                ],
            }
        ],
    }


@pytest.fixture
def binance_futures_data():
    """Sample Binance COIN-M futures REST API response."""
    return {
        "serverTime": 1625097600000,
        "symbols": [
            {
                "symbol": "BTCUSD_PERP",
                "pair": "BTCUSD",
                "contractType": "PERPETUAL",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "status": "TRADING",
                "contractSize": 100,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "stepSize": "1"},
                ],
            },
            {
                "symbol": "BTCUSD_250328",
                "pair": "BTCUSD",
                "contractType": "CURRENT_QUARTER",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "status": "TRADING",
                "contractSize": 100,
                "deliveryDate": 1743148800000,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "stepSize": "1"},
                ],
            },
        ],
    }


@pytest.fixture
def okx_perpetual_data():
    """Sample OKX SWAP (perpetual) REST API response."""
    return {
        "code": "0",
        "data": [
            {
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "baseCcy": "BTC",
                "quoteCcy": "USDT",
                "settleCcy": "USDT",
                "ctVal": "0.01",
                "state": "live",
                "tickSz": "0.1",
                "lotSz": "1",
            }
        ],
        "msg": "",
    }


@pytest.fixture
def okx_futures_data():
    """Sample OKX FUTURES REST API response."""
    return {
        "code": "0",
        "data": [
            {
                "instId": "BTC-USD-250328",
                "instType": "FUTURES",
                "baseCcy": "BTC",
                "quoteCcy": "USD",
                "settleCcy": "BTC",
                "ctVal": "100",
                "state": "live",
                "tickSz": "0.1",
                "lotSz": "1",
                "expTime": "1743148800000",
            }
        ],
        "msg": "",
    }


@pytest.fixture
def bybit_spot_data():
    """Sample Bybit spot REST API response."""
    return {
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "status": "Trading",
                    "priceFilter": {"tickSize": "0.01"},
                    "lotSizeFilter": {"basePrecision": "0.000001"},
                }
            ]
        },
    }


@pytest.fixture
def bybit_perpetual_data():
    """Sample Bybit linear perpetual REST API response."""
    return {
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "settleCoin": "USDT",
                    "status": "Trading",
                    "contractType": "LinearPerpetual",
                    "priceFilter": {"tickSize": "0.10"},
                    "lotSizeFilter": {"qtyStep": "0.001"},
                }
            ]
        },
    }


@pytest.fixture
def bybit_futures_data():
    """Sample Bybit inverse futures REST API response."""
    return {
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": "BTCUSD",
                    "baseCoin": "BTC",
                    "quoteCoin": "USD",
                    "settleCoin": "BTC",
                    "status": "Trading",
                    "contractType": "InversePerpetual",
                    "priceFilter": {"tickSize": "0.50"},
                    "lotSizeFilter": {"qtyStep": "1"},
                }
            ]
        },
    }


@pytest.fixture
def coinbase_spot_data():
    """Sample Coinbase spot REST API response."""
    return [
        {
            "id": "BTC-USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "quote_increment": "0.01",
            "base_increment": "0.00000001",
            "status": "online",
            "trading_disabled": False,
        }
    ]


@pytest.fixture
def coinbase_intl_perpetual_data():
    """Sample Coinbase International perpetual REST API response."""
    return {
        "instruments": [
            {
                "instrument_id": "BTC-PERP",
                "symbol": "BTC-PERP",
                "type": "PERP",
                "base_asset_name": "BTC",
                "quote_asset_name": "USDC",
                "base_increment": "0.0001",
                "quote_increment": "0.1",
                "trading_state": "TRADING",
            }
        ]
    }


@pytest.fixture
def bitget_spot_data():
    """Sample Bitget spot REST API response."""
    return {
        "code": "00000",
        "data": [
            {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "status": "online",
                "priceScale": "2",
                "quantityScale": "6",
            }
        ],
    }


@pytest.fixture
def bitget_perpetual_data():
    """Sample Bitget perpetual (USDT-FUTURES) REST API response."""
    return {
        "code": "00000",
        "data": [
            {
                "symbol": "BTCUSDT",
                "baseCoin": "BTC",
                "quoteCoin": "USDT",
                "symbolStatus": "normal",
                "symbolType": "perpetual",
                "pricePlace": "1",
                "volumePlace": "3",
                "sizeMultiplier": "0.001",
                "supportMarginCoins": ["USDT"],
            }
        ],
    }


@pytest.fixture
def bitget_futures_data():
    """Sample Bitget futures (COIN-FUTURES) REST API response."""
    return {
        "code": "00000",
        "data": [
            {
                "symbol": "BTCUSD",
                "baseCoin": "BTC",
                "quoteCoin": "USD",
                "symbolStatus": "normal",
                "symbolType": "perpetual",
                "pricePlace": "1",
                "volumePlace": "0",
                "sizeMultiplier": "1",
                "supportMarginCoins": ["BTC"],
            }
        ],
    }
