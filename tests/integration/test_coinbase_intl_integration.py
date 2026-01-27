"""Integration tests for Coinbase International adapter."""

import pytest

from reference_data.adapters.coinbase_intl import CoinbaseIntlExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_intl_fetch_perpetual_snapshot(integration_timeout):
    """Test fetching real perpetual data from Coinbase International testnet."""
    # Coinbase International uses testnet for testing
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=True)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.PERPETUAL, is_testnet=True)
        assert endpoint is not None, "Perpetual endpoint not configured"

        # Fetch perpetual data
        await adapter._fetch_instrument_data(InstrumentType.PERPETUAL, endpoint.rest_url)

        # Verify data was stored
        assert InstrumentType.PERPETUAL in adapter._all_pairs
        perpetual_pairs = adapter._all_pairs[InstrumentType.PERPETUAL]

        # Verify we got some data
        assert perpetual_pairs is not None
        assert len(perpetual_pairs) > 0, "Expected at least one perpetual trading pair"

        # Verify all pairs are perpetual type
        assert all(p.instrument_type == InstrumentType.PERPETUAL for p in perpetual_pairs), (
            "All pairs should be perpetual type"
        )

        # Verify required fields are populated for first 5 pairs
        for pair in perpetual_pairs[:5]:
            assert pair.symbol is not None, f"Symbol missing for pair: {pair}"
            assert pair.base_asset is not None, f"Base asset missing for pair: {pair}"
            assert pair.quote_asset is not None, f"Quote asset missing for pair: {pair}"
            # Perpetual-specific fields
            assert pair.settlement_currency is not None, (
                f"Settlement currency missing for pair: {pair}"
            )
            # Contract size should be 1.0 for USDC-margined perpetuals
            assert pair.contract_size == 1.0, f"Contract size should be 1.0: {pair}"
            # Perpetuals should not have expiry
            assert pair.expiry is None, f"Perpetuals should not have expiry: {pair}"

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_intl_adapter_initialization(integration_timeout):
    """Test that Coinbase International adapter initializes correctly in testnet mode."""
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=True)
    try:
        # Verify adapter configuration
        assert adapter.is_testnet is True
        assert adapter.config is not None
        assert adapter.session is not None

        # Verify supported instruments (Coinbase International only supports perpetual)
        supported = adapter.get_supported_instruments()
        assert InstrumentType.PERPETUAL in supported
        # Coinbase International doesn't support spot or futures
        assert InstrumentType.SPOT not in supported
        assert InstrumentType.FUTURES not in supported

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_intl_perpetual_data_quality(integration_timeout):
    """Test the quality of perpetual data returned from Coinbase International."""
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=True)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.PERPETUAL, is_testnet=True)
        assert endpoint is not None, "Perpetual endpoint not configured"

        # Fetch perpetual data
        await adapter._fetch_instrument_data(InstrumentType.PERPETUAL, endpoint.rest_url)

        # Get the stored pairs
        perpetual_pairs = adapter._all_pairs[InstrumentType.PERPETUAL]

        # Check for at least one BTC perpetual (common on most exchanges)
        btc_pairs = [p for p in perpetual_pairs if "BTC" in p.symbol]
        assert len(btc_pairs) > 0, "Expected at least one BTC perpetual trading pair"

        # Verify data consistency for BTC pairs
        for pair in btc_pairs[:3]:
            # Symbol should contain PERP or similar indicator
            assert "PERP" in pair.symbol.upper() or "BTC" in pair.symbol.upper(), (
                f"Symbol format unexpected: {pair.symbol}"
            )

            # Base should be BTC
            assert pair.base_asset == "BTC", f"Expected BTC as base asset: {pair}"

            # Quote should be USDC (Coinbase International uses USDC)
            assert pair.quote_asset == "USDC", f"Expected USDC as quote asset: {pair}"

            # Settlement currency should be USDC
            assert pair.settlement_currency == "USDC", (
                f"Expected USDC as settlement currency: {pair}"
            )

            # Tick size and lot size should be positive if present
            if pair.tick_size is not None:
                assert pair.tick_size > 0, f"Tick size should be positive: {pair}"
            if pair.lot_size is not None:
                assert pair.lot_size > 0, f"Lot size should be positive: {pair}"

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_coinbase_intl_mainnet_fallback(integration_timeout):
    """Test fetching from mainnet if testnet is unavailable."""
    # Try mainnet as fallback
    adapter = CoinbaseIntlExchangeAdapter(is_testnet=False)
    try:
        # Get the endpoint configuration for mainnet
        endpoint = adapter.config.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        if endpoint is not None:
            # Fetch perpetual data from mainnet
            await adapter._fetch_instrument_data(InstrumentType.PERPETUAL, endpoint.rest_url)

            # Verify data was stored
            assert InstrumentType.PERPETUAL in adapter._all_pairs
            perpetual_pairs = adapter._all_pairs[InstrumentType.PERPETUAL]

            # Verify we got some data
            assert perpetual_pairs is not None
            assert len(perpetual_pairs) > 0, "Expected at least one perpetual trading pair"

    finally:
        await adapter.close()
