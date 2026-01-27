"""Tests for the Coinbase exchange adapter."""

import pytest

from reference_data.adapters.coinbase import CoinbaseExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_coinbase_adapter_initialization():
    """Test that the Coinbase adapter initializes correctly with spot-only support."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT in supported
    assert InstrumentType.PERPETUAL not in supported
    assert InstrumentType.FUTURES not in supported


@pytest.mark.asyncio
async def test_coinbase_spot_to_standard(coinbase_spot_data):
    """Test conversion of Coinbase spot data to standard format."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    wrapped_data = {"products": coinbase_spot_data}
    result = adapter._convert_spot(wrapped_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.SPOT
    assert pair.symbol == "BTC-USD"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USD"
    assert pair.status == "online"
    assert pair.tick_size == 0.01
    assert pair.lot_size == 0.00000001
    assert pair.contract_size is None
    assert pair.settlement_currency is None
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_coinbase_spot_filtering():
    """Test that disabled trading pairs are filtered out."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    data = {
        "products": [
            {
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "quote_increment": "0.01",
                "base_increment": "0.00000001",
                "status": "online",
                "trading_disabled": False,
            },
            {
                "id": "ETH-USD",
                "base_currency": "ETH",
                "quote_currency": "USD",
                "quote_increment": "0.01",
                "base_increment": "0.0000001",
                "status": "offline",
                "trading_disabled": True,
            },
        ]
    }
    result = adapter._convert_spot(data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


@pytest.mark.asyncio
async def test_coinbase_spot_missing_fields():
    """Test that products with missing required fields are skipped."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    data = {
        "products": [
            {
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "status": "online",
            },
            {
                "id": "ETH-USD",
                # Missing base_currency
                "quote_currency": "USD",
                "status": "online",
            },
        ]
    }
    result = adapter._convert_spot(data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


@pytest.mark.asyncio
async def test_coinbase_spot_invalid_increments():
    """Test handling of invalid increment values."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    data = {
        "products": [
            {
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "quote_increment": "invalid",
                "base_increment": None,
                "status": "online",
                "trading_disabled": False,
            }
        ]
    }
    result = adapter._convert_spot(data)
    assert len(result) == 1
    pair = result[0]
    assert pair.tick_size is None
    assert pair.lot_size is None
