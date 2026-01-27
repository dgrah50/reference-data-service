"""Tests for the Coinbase International exchange adapter."""

import pytest
from reference_data.adapters.coinbase_intl import CoinbaseIntlExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.asyncio
async def test_coinbase_intl_adapter_initialization():
    """Test that the adapter initializes correctly with perpetual-only support."""
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=False)
    supported = adapter.get_supported_instruments()
    assert InstrumentType.SPOT not in supported
    assert InstrumentType.PERPETUAL in supported
    assert InstrumentType.FUTURES not in supported


@pytest.mark.asyncio
async def test_coinbase_intl_perpetual_to_standard(coinbase_intl_perpetual_data):
    """Test conversion of Coinbase International perpetual data to TradingPair format."""
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=False)
    result = adapter._convert_perpetual(coinbase_intl_perpetual_data)
    assert len(result) == 1
    pair = result[0]
    assert pair.instrument_type == InstrumentType.PERPETUAL
    assert pair.symbol == "BTC-PERP"
    assert pair.base_asset == "BTC"
    assert pair.quote_asset == "USDC"
    assert pair.status == "TRADING"
    assert pair.tick_size == 0.1
    assert pair.lot_size == 0.0001
    assert pair.settlement_currency == "USDC"
    assert pair.contract_size == 1.0  # Default for USDC-margined
    assert pair.expiry is None
