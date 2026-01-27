"""
Tests for exchange adapters.
"""

from typing import Dict, Any

import pytest

from reference_data.adapters.config import BinanceConfig, OKXConfig
from reference_data.api.models import TradingPair


@pytest.fixture
def binance_config():
    """Create a test configuration for Binance."""
    return BinanceConfig(is_testnet=True)


@pytest.fixture
def okx_config():
    """Create a test configuration for OKX."""
    return OKXConfig(is_testnet=True)


@pytest.fixture
def binance_rest_data():
    """Sample REST response data from Binance."""
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
def okx_rest_data():
    """Sample REST response data from OKX."""
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
    }


def verify_trading_pair(pair: TradingPair, expected: Dict[str, Any]):
    """Helper function to verify trading pair data."""
    assert pair.symbol == expected["symbol"]
    assert pair.base_asset == expected["base_asset"]
    assert pair.quote_asset == expected["quote_asset"]
    assert pair.status == expected["status"]
    assert pair.tick_size == expected["tick_size"]
    assert pair.lot_size == expected["lot_size"]
