"""Tests for the Binance exchange adapter."""

import pytest
from reference_data.adapters.binance import BinanceExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_binance_adapter_initialization():
    """Test that the Binance adapter initializes correctly."""
    adapter = BinanceExchangeAdapter(is_testnet=True)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT in supported
    assert InstrumentType.PERPETUAL in supported
    assert InstrumentType.FUTURES in supported


@pytest.mark.asyncio
async def test_binance_spot_to_standard(binance_rest_data):
    """Test conversion from Binance spot format."""
    adapter = BinanceExchangeAdapter(is_testnet=True)
    result = adapter._convert_spot(binance_rest_data)

    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.SPOT
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.status == "TRADING"
    assert pair.tick_size == 0.01
    assert pair.lot_size == 0.00001
    assert pair.contract_size is None
    assert pair.settlement_currency is None
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_binance_perpetual_to_standard(binance_perpetual_data):
    """Test conversion from Binance USD-M perpetual format."""
    adapter = BinanceExchangeAdapter(is_testnet=True)
    result = adapter._convert_perpetual(binance_perpetual_data)

    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.PERPETUAL
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.contract_size == 1.0
    assert pair.settlement_currency == "USDT"
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_binance_futures_to_standard(binance_futures_data):
    """Test conversion from Binance COIN-M futures format."""
    adapter = BinanceExchangeAdapter(is_testnet=True)
    result = adapter._convert_futures(binance_futures_data)

    assert len(result) == 2
    perp = next(p for p in result if p.symbol == "BTCUSD_PERP")
    quarterly = next(p for p in result if p.symbol == "BTCUSD_250328")

    assert perp.instrument_type == InstrumentType.FUTURES
    assert perp.contract_size == 100.0
    assert perp.settlement_currency == "BTC"
    assert perp.expiry is None

    assert quarterly.instrument_type == InstrumentType.FUTURES
    assert quarterly.contract_size == 100.0
    assert quarterly.expiry is not None
