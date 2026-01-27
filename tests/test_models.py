"""Tests for reference data models."""

from src.reference_data.api.models import (
    ExchangeEnum,
    InstrumentType,
    StandardExchangeInfo,
    TradingPair,
)


class TestInstrumentType:
    """Test InstrumentType enum."""

    def test_instrument_type_values(self):
        """Test that InstrumentType has correct values."""
        assert InstrumentType.SPOT == "spot"
        assert InstrumentType.PERPETUAL == "perpetual"
        assert InstrumentType.FUTURES == "futures"

    def test_instrument_type_members(self):
        """Test that InstrumentType has exactly 3 members."""
        assert len(InstrumentType) == 3


class TestExchangeEnum:
    """Test ExchangeEnum."""

    def test_exchange_enum_has_all_exchanges(self):
        """Test that ExchangeEnum has all 6 exchanges."""
        assert ExchangeEnum.BINANCE == "Binance"
        assert ExchangeEnum.OKX == "OKX"
        assert ExchangeEnum.BYBIT == "Bybit"
        assert ExchangeEnum.BITGET == "Bitget"
        assert ExchangeEnum.COINBASE == "Coinbase"
        assert ExchangeEnum.COINBASE_INTL == "CoinbaseInternational"

    def test_exchange_enum_member_count(self):
        """Test that ExchangeEnum has exactly 6 members."""
        assert len(ExchangeEnum) == 6


class TestTradingPair:
    """Test TradingPair model."""

    def test_spot_trading_pair(self):
        """Test spot trading pair with derivative fields as None."""
        pair = TradingPair(
            instrument_type=InstrumentType.SPOT,
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            tick_size=0.01,
            lot_size=0.00001,
        )

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

    def test_perpetual_trading_pair(self):
        """Test perpetual trading pair with contract_size and settlement_currency."""
        pair = TradingPair(
            instrument_type=InstrumentType.PERPETUAL,
            symbol="BTCUSDT-PERP",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            tick_size=0.1,
            lot_size=0.001,
            contract_size=1.0,
            settlement_currency="USDT",
        )

        assert pair.instrument_type == InstrumentType.PERPETUAL
        assert pair.symbol == "BTCUSDT-PERP"
        assert pair.base_asset == "BTC"
        assert pair.quote_asset == "USDT"
        assert pair.contract_size == 1.0
        assert pair.settlement_currency == "USDT"
        assert pair.expiry is None

    def test_futures_trading_pair(self):
        """Test futures trading pair with all derivative fields including expiry."""
        pair = TradingPair(
            instrument_type=InstrumentType.FUTURES,
            symbol="BTCUSDT-20260327",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            tick_size=0.1,
            lot_size=0.001,
            contract_size=1.0,
            settlement_currency="USDT",
            expiry="2026-03-27",
        )

        assert pair.instrument_type == InstrumentType.FUTURES
        assert pair.symbol == "BTCUSDT-20260327"
        assert pair.base_asset == "BTC"
        assert pair.quote_asset == "USDT"
        assert pair.contract_size == 1.0
        assert pair.settlement_currency == "USDT"
        assert pair.expiry == "2026-03-27"

    def test_instrument_type_is_first_field(self):
        """Test that instrument_type is the first field in the model."""
        # Check model fields order
        fields = list(TradingPair.model_fields.keys())
        assert fields[0] == "instrument_type"


class TestStandardExchangeInfo:
    """Test StandardExchangeInfo model."""

    def test_standard_exchange_info_with_multiple_instrument_types(self):
        """Test that StandardExchangeInfo works with mixed instrument types."""
        spot_pair = TradingPair(
            instrument_type=InstrumentType.SPOT,
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
        )

        perp_pair = TradingPair(
            instrument_type=InstrumentType.PERPETUAL,
            symbol="BTCUSDT-PERP",
            base_asset="BTC",
            quote_asset="USDT",
            contract_size=1.0,
            settlement_currency="USDT",
        )

        info = StandardExchangeInfo(
            exchange=ExchangeEnum.BINANCE,
            timestamp=1706400000,
            trading_pairs=[spot_pair, perp_pair],
        )

        assert info.exchange == ExchangeEnum.BINANCE
        assert info.timestamp == 1706400000
        assert len(info.trading_pairs) == 2
        assert info.trading_pairs[0].instrument_type == InstrumentType.SPOT
        assert info.trading_pairs[1].instrument_type == InstrumentType.PERPETUAL
