"""Tests for the Bybit exchange adapter."""

import pytest

from reference_data.adapters.bybit import BybitExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_bybit_adapter_initialization():
    """Test that Bybit adapter initializes correctly and supports all instrument types."""
    adapter = BybitExchangeAdapter(is_testnet=True)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT in supported
    assert InstrumentType.PERPETUAL in supported
    assert InstrumentType.FUTURES in supported


@pytest.mark.asyncio
async def test_bybit_spot_to_standard(bybit_spot_data):
    """Test conversion of Bybit spot data to standard format."""
    adapter = BybitExchangeAdapter(is_testnet=True)
    result = adapter._convert_spot(bybit_spot_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.SPOT
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.tick_size == 0.01
    assert pair.lot_size == 0.000001
    assert pair.contract_size is None
    assert pair.settlement_currency is None


@pytest.mark.asyncio
async def test_bybit_perpetual_to_standard(bybit_perpetual_data):
    """Test conversion of Bybit perpetual data to standard format."""
    adapter = BybitExchangeAdapter(is_testnet=True)
    result = adapter._convert_perpetual(bybit_perpetual_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.PERPETUAL
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.settlement_currency == "USDT"
    assert pair.tick_size == 0.10
    assert pair.lot_size == 0.001
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_bybit_futures_to_standard(bybit_futures_data):
    """Test conversion of Bybit futures data to standard format."""
    adapter = BybitExchangeAdapter(is_testnet=True)
    result = adapter._convert_futures(bybit_futures_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.FUTURES
    assert pair.symbol == "BTCUSD"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USD"
    assert pair.settlement_currency == "BTC"
    assert pair.tick_size == 0.50
    assert pair.lot_size == 1.0


@pytest.mark.asyncio
async def test_bybit_error_handling():
    """Test that Bybit adapter handles API errors correctly."""
    adapter = BybitExchangeAdapter(is_testnet=True)
    error_data = {"retCode": 10001, "retMsg": "Invalid API key"}

    with pytest.raises(Exception) as exc_info:
        adapter._convert_spot(error_data)
    assert "Bybit API error" in str(exc_info.value)
    assert "Invalid API key" in str(exc_info.value)


@pytest.mark.asyncio
async def test_bybit_missing_fields(bybit_spot_data):
    """Test that Bybit adapter handles missing required fields gracefully."""
    adapter = BybitExchangeAdapter(is_testnet=True)

    # Create data with missing baseCoin
    incomplete_data = {
        "retCode": 0,
        "result": {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "quoteCoin": "USDT",
                    "status": "Trading",
                }
            ]
        },
    }

    result = adapter._convert_spot(incomplete_data)
    # Should skip incomplete records
    assert len(result) == 0
