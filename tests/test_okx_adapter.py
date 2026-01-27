"""
Tests for the OKX exchange adapter.
"""

import pytest

from reference_data.adapters.okx import OKXExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_okx_adapter_initialization():
    """Test that the OKX adapter initializes correctly."""
    adapter = OKXExchangeAdapter(is_testnet=False)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT in supported
    assert InstrumentType.PERPETUAL in supported
    assert InstrumentType.FUTURES in supported


@pytest.mark.asyncio
async def test_okx_spot_to_standard(okx_rest_data):
    """Test conversion from OKX spot format."""
    adapter = OKXExchangeAdapter(is_testnet=False)
    result = adapter._convert_spot(okx_rest_data)

    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.SPOT
    assert pair.symbol == "BTC-USDT"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDT"
    assert pair.status == "live"
    assert pair.tick_size == 0.1
    assert pair.lot_size == 0.00001
    assert pair.contract_size is None
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_okx_perpetual_to_standard(okx_perpetual_data):
    """Test conversion from OKX SWAP format."""
    adapter = OKXExchangeAdapter(is_testnet=False)
    result = adapter._convert_perpetual(okx_perpetual_data)

    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.PERPETUAL
    assert pair.symbol == "BTC-USDT-SWAP"
    assert pair.contract_size == 0.01
    assert pair.settlement_currency == "USDT"
    assert pair.expiry is None


@pytest.mark.asyncio
async def test_okx_futures_to_standard(okx_futures_data):
    """Test conversion from OKX FUTURES format."""
    adapter = OKXExchangeAdapter(is_testnet=False)
    result = adapter._convert_futures(okx_futures_data)

    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.FUTURES
    assert pair.symbol == "BTC-USD-250328"
    assert pair.contract_size == 100.0
    assert pair.settlement_currency == "BTC"
    assert pair.expiry is not None
