"""Tests for the Bitget exchange adapter."""

import pytest

from reference_data.adapters.bitget import BitgetExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_bitget_adapter_initialization():
    """Test that the Bitget adapter initializes correctly with all instrument types."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT in supported
    assert InstrumentType.PERPETUAL in supported
    assert InstrumentType.FUTURES in supported


@pytest.mark.asyncio
async def test_bitget_spot_to_standard(bitget_spot_data):
    """Test conversion of Bitget spot data to standard format."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    result = adapter._convert_spot(bitget_spot_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.SPOT
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.status == "online"
    assert pair.tick_size == 0.01  # 10^(-2)
    assert pair.lot_size == 0.000001  # 10^(-6)
    assert pair.contract_size is None


@pytest.mark.asyncio
async def test_bitget_perpetual_to_standard(bitget_perpetual_data):
    """Test conversion of Bitget perpetual data to standard format."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    result = adapter._convert_perpetual(bitget_perpetual_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.PERPETUAL
    assert pair.symbol == "BTCUSDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.status == "TRADING"  # Mapped from "normal"
    assert pair.settlement_currency == "USDT"
    assert pair.contract_size == 0.001
    assert pair.tick_size == 0.1  # 10^(-1)
    assert pair.lot_size == 0.001  # 10^(-3)
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_bitget_futures_to_standard(bitget_futures_data):
    """Test conversion of Bitget futures data to standard format."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    result = adapter._convert_futures(bitget_futures_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.FUTURES
    assert pair.symbol == "BTCUSD"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USD"
    assert pair.status == "TRADING"  # Mapped from "normal"
    assert pair.settlement_currency == "BTC"
    assert pair.contract_size == 1.0
    assert pair.tick_size == 0.1  # 10^(-1)
    assert pair.lot_size == 1.0  # 10^(-0)


@pytest.mark.asyncio
async def test_bitget_spot_error_handling():
    """Test error handling for invalid Bitget API responses."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    error_data = {"code": "40001", "msg": "Invalid request"}

    with pytest.raises(Exception) as exc_info:
        adapter._convert_spot(error_data)
    assert "Bitget API error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_bitget_perpetual_error_handling():
    """Test error handling for invalid perpetual API responses."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    error_data = {"code": "40001", "msg": "Invalid request"}

    with pytest.raises(Exception) as exc_info:
        adapter._convert_perpetual(error_data)
    assert "Bitget API error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_bitget_futures_error_handling():
    """Test error handling for invalid futures API responses."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    error_data = {"code": "40001", "msg": "Invalid request"}

    with pytest.raises(Exception) as exc_info:
        adapter._convert_futures(error_data)
    assert "Bitget API error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_bitget_spot_missing_fields(bitget_spot_data):
    """Test handling of missing required fields in spot data."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    # Remove required field
    bitget_spot_data["data"][0].pop("baseCoin")
    result = adapter._convert_spot(bitget_spot_data)
    # Should skip the invalid entry
    assert len(result) == 0


@pytest.mark.asyncio
async def test_bitget_perpetual_settlement_currency_extraction(bitget_perpetual_data):
    """Test that settlement currency is correctly extracted from supportMarginCoins."""
    adapter = BitgetExchangeAdapter(is_testnet=False)
    result = adapter._convert_perpetual(bitget_perpetual_data)
    assert len(result) == 1
    assert result[0].settlement_currency == "USDT"


@pytest.mark.asyncio
async def test_bitget_to_standard_merges_all_pairs():
    """Test that _to_standard merges all instrument types correctly."""
    adapter = BitgetExchangeAdapter(is_testnet=False)

    # Manually populate _all_pairs
    from reference_data.api.models import TradingPair

    adapter._all_pairs[InstrumentType.SPOT] = [
        TradingPair(
            instrument_type=InstrumentType.SPOT,
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="online",
        )
    ]
    adapter._all_pairs[InstrumentType.PERPETUAL] = [
        TradingPair(
            instrument_type=InstrumentType.PERPETUAL,
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
        )
    ]

    result = adapter._to_standard({})

    assert result.exchange.value == "Bitget"
    assert len(result.trading_pairs) == 2
    assert any(p.instrument_type == InstrumentType.SPOT for p in result.trading_pairs)
    assert any(p.instrument_type == InstrumentType.PERPETUAL for p in result.trading_pairs)
