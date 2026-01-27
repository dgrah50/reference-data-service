"""Integration tests for Coinbase adapter."""

import pytest

from reference_data.adapters.coinbase import CoinbaseExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_fetch_spot_snapshot(integration_timeout):
    """Test fetching real spot data from Coinbase sandbox."""
    # Coinbase sandbox is for testing authentication-required APIs
    # For public data, we use mainnet but be gentle with requests
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.SPOT, is_testnet=False)
        assert endpoint is not None, "Spot endpoint not configured"

        # Fetch spot data
        await adapter._fetch_instrument_data(InstrumentType.SPOT, endpoint.rest_url)

        # Verify data was stored
        assert InstrumentType.SPOT in adapter._all_pairs
        spot_pairs = adapter._all_pairs[InstrumentType.SPOT]

        # Verify we got some data
        assert spot_pairs is not None
        assert len(spot_pairs) > 0, "Expected at least one spot trading pair"

        # Verify all pairs are spot type
        assert all(p.instrument_type == InstrumentType.SPOT for p in spot_pairs), (
            "All pairs should be spot type"
        )

        # Verify required fields are populated for first 5 pairs
        for pair in spot_pairs[:5]:
            assert pair.symbol is not None, f"Symbol missing for pair: {pair}"
            assert pair.base_asset is not None, f"Base asset missing for pair: {pair}"
            assert pair.quote_asset is not None, f"Quote asset missing for pair: {pair}"
            # Status might be None for some pairs, so we don't assert on it

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_adapter_initialization(integration_timeout):
    """Test that Coinbase adapter initializes correctly."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    try:
        # Verify adapter configuration
        assert adapter.is_testnet is False
        assert adapter.config is not None
        assert adapter.session is not None

        # Verify supported instruments (Coinbase only supports spot)
        supported = adapter.get_supported_instruments()
        assert InstrumentType.SPOT in supported
        # Coinbase doesn't support perpetual or futures
        assert InstrumentType.PERPETUAL not in supported
        assert InstrumentType.FUTURES not in supported

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_spot_data_quality(integration_timeout):
    """Test the quality of spot data returned from Coinbase."""
    adapter = CoinbaseExchangeAdapter(is_testnet=False)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.SPOT, is_testnet=False)
        assert endpoint is not None, "Spot endpoint not configured"

        # Fetch spot data
        await adapter._fetch_instrument_data(InstrumentType.SPOT, endpoint.rest_url)

        # Get the stored pairs
        spot_pairs = adapter._all_pairs[InstrumentType.SPOT]

        # Check for at least one BTC pair (should always exist on Coinbase)
        btc_pairs = [p for p in spot_pairs if "BTC" in p.symbol]
        assert len(btc_pairs) > 0, "Expected at least one BTC trading pair"

        # Verify data consistency for BTC pairs
        for pair in btc_pairs[:3]:
            # Symbol should be in format BASE-QUOTE
            assert "-" in pair.symbol, f"Symbol format unexpected: {pair.symbol}"

            # Base or quote should be BTC
            assert pair.base_asset == "BTC" or pair.quote_asset == "BTC", (
                f"Expected BTC in base or quote: {pair}"
            )

            # Tick size and lot size should be positive if present
            if pair.tick_size is not None:
                assert pair.tick_size > 0, f"Tick size should be positive: {pair}"
            if pair.lot_size is not None:
                assert pair.lot_size > 0, f"Lot size should be positive: {pair}"

    finally:
        await adapter.close()
