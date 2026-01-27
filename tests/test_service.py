"""Tests for the reference data service."""

import pytest

from reference_data.adapters.binance import BinanceExchangeAdapter
from reference_data.adapters.bitget import BitgetExchangeAdapter
from reference_data.adapters.bybit import BybitExchangeAdapter
from reference_data.adapters.coinbase import CoinbaseExchangeAdapter
from reference_data.adapters.coinbase_intl import CoinbaseIntlExchangeAdapter
from reference_data.adapters.okx import OKXExchangeAdapter
from reference_data.api.models import ExchangeEnum
from reference_data.core.service import ReferenceDataService, ServiceConfig


def test_service_config_defaults():
    """Test that ServiceConfig has correct default values."""
    config = ServiceConfig()
    assert config.enable_binance is True
    assert config.enable_okx is True
    assert config.enable_bybit is True
    assert config.enable_bitget is True
    assert config.enable_coinbase is True
    assert config.enable_coinbase_intl is True
    assert config.use_testnet is False


def test_service_initializes_all_adapters_by_default():
    """Test that ReferenceDataService initializes all 6 adapters by default."""
    service = ReferenceDataService()
    assert len(service.adapters) == 6
    assert ExchangeEnum.BINANCE in service.adapters
    assert ExchangeEnum.OKX in service.adapters
    assert ExchangeEnum.BYBIT in service.adapters
    assert ExchangeEnum.BITGET in service.adapters
    assert ExchangeEnum.COINBASE in service.adapters
    assert ExchangeEnum.COINBASE_INTL in service.adapters


def test_service_with_selective_adapters():
    """Test that ReferenceDataService only initializes enabled adapters."""
    config = ServiceConfig(
        enable_binance=True,
        enable_okx=False,
        enable_bybit=False,
        enable_bitget=False,
        enable_coinbase=False,
        enable_coinbase_intl=False,
    )
    service = ReferenceDataService(config=config)
    assert len(service.adapters) == 1
    assert ExchangeEnum.BINANCE in service.adapters


def test_service_with_testnet():
    """Test that ReferenceDataService passes testnet flag to adapters."""
    config = ServiceConfig(use_testnet=True)
    service = ReferenceDataService(config=config)
    # Verify testnet is passed to adapters
    binance_adapter = service.adapters[ExchangeEnum.BINANCE]
    assert isinstance(binance_adapter, BinanceExchangeAdapter)
    assert binance_adapter.is_testnet is True

    okx_adapter = service.adapters[ExchangeEnum.OKX]
    assert isinstance(okx_adapter, OKXExchangeAdapter)
    assert okx_adapter.is_testnet is True

    bybit_adapter = service.adapters[ExchangeEnum.BYBIT]
    assert isinstance(bybit_adapter, BybitExchangeAdapter)
    assert bybit_adapter.is_testnet is True

    bitget_adapter = service.adapters[ExchangeEnum.BITGET]
    assert isinstance(bitget_adapter, BitgetExchangeAdapter)
    assert bitget_adapter.is_testnet is True

    coinbase_adapter = service.adapters[ExchangeEnum.COINBASE]
    assert isinstance(coinbase_adapter, CoinbaseExchangeAdapter)
    assert coinbase_adapter.is_testnet is True

    coinbase_intl_adapter = service.adapters[ExchangeEnum.COINBASE_INTL]
    assert isinstance(coinbase_intl_adapter, CoinbaseIntlExchangeAdapter)
    assert coinbase_intl_adapter.is_testnet is True


def test_service_with_no_adapters_enabled():
    """Test that service can be created with no adapters, but start() raises error."""
    config = ServiceConfig(
        enable_binance=False,
        enable_okx=False,
        enable_bybit=False,
        enable_bitget=False,
        enable_coinbase=False,
        enable_coinbase_intl=False,
    )
    service = ReferenceDataService(config=config)
    assert len(service.adapters) == 0


@pytest.mark.asyncio
async def test_service_start_with_no_adapters_raises_error():
    """Test that start() raises ValueError when no adapters are enabled."""
    config = ServiceConfig(
        enable_binance=False,
        enable_okx=False,
        enable_bybit=False,
        enable_bitget=False,
        enable_coinbase=False,
        enable_coinbase_intl=False,
    )
    service = ReferenceDataService(config=config)

    with pytest.raises(ValueError, match="No adapters enabled"):
        await service.start()


def test_service_snapshot_initialized_empty():
    """Test that latest_snapshot is initialized as empty dict."""
    service = ReferenceDataService()
    assert service.latest_snapshot == {}


def test_get_snapshot_returns_dict():
    """Test that get_snapshot() returns the snapshot dictionary."""
    service = ReferenceDataService()
    snapshot = service.get_snapshot()
    assert isinstance(snapshot, dict)
    assert snapshot == {}
