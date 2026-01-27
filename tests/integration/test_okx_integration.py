"""Integration tests for OKX adapter."""

import pytest

from reference_data.adapters.okx import OKXExchangeAdapter
from reference_data.api.models import InstrumentType


@pytest.mark.integration
@pytest.mark.asyncio
async def test_okx_fetch_spot_snapshot(integration_timeout):
    """Test fetching real spot data from OKX."""
    # Note: OKX doesn't have a separate testnet for public data, using mainnet
    adapter = OKXExchangeAdapter(is_testnet=False)
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
            assert pair.status is not None, f"Status missing for pair: {pair}"

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_okx_fetch_perpetual_snapshot(integration_timeout):
    """Test fetching real perpetual data from OKX."""
    # Note: OKX doesn't have a separate testnet for public data, using mainnet
    adapter = OKXExchangeAdapter(is_testnet=False)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)
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
            assert pair.status is not None, f"Status missing for pair: {pair}"
            # Perpetual-specific fields
            assert pair.settlement_currency is not None, (
                f"Settlement currency missing for pair: {pair}"
            )

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_okx_fetch_futures_snapshot(integration_timeout):
    """Test fetching real futures data from OKX."""
    # Note: OKX doesn't have a separate testnet for public data, using mainnet
    adapter = OKXExchangeAdapter(is_testnet=False)
    try:
        # Get the endpoint configuration
        endpoint = adapter.config.get_endpoint(InstrumentType.FUTURES, is_testnet=False)
        assert endpoint is not None, "Futures endpoint not configured"

        # Fetch futures data
        await adapter._fetch_instrument_data(InstrumentType.FUTURES, endpoint.rest_url)

        # Verify data was stored
        assert InstrumentType.FUTURES in adapter._all_pairs
        futures_pairs = adapter._all_pairs[InstrumentType.FUTURES]

        # Verify we got some data
        assert futures_pairs is not None
        assert len(futures_pairs) > 0, "Expected at least one futures trading pair"

        # Verify all pairs are futures type
        assert all(p.instrument_type == InstrumentType.FUTURES for p in futures_pairs), (
            "All pairs should be futures type"
        )

        # Verify required fields are populated for first 5 pairs
        for pair in futures_pairs[:5]:
            assert pair.symbol is not None, f"Symbol missing for pair: {pair}"
            assert pair.base_asset is not None, f"Base asset missing for pair: {pair}"
            assert pair.quote_asset is not None, f"Quote asset missing for pair: {pair}"
            assert pair.status is not None, f"Status missing for pair: {pair}"
            # Futures-specific fields
            assert pair.settlement_currency is not None, (
                f"Settlement currency missing for pair: {pair}"
            )

    finally:
        await adapter.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_okx_adapter_initialization(integration_timeout):
    """Test that OKX adapter initializes correctly."""
    adapter = OKXExchangeAdapter(is_testnet=False)
    try:
        # Verify adapter configuration
        assert adapter.is_testnet is False
        assert adapter.config is not None
        assert adapter.session is not None

        # Verify supported instruments
        supported = adapter.get_supported_instruments()
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    finally:
        await adapter.close()
