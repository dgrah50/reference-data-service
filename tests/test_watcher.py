"""Tests for the exchange watcher module."""

from unittest.mock import MagicMock

from reference_data.api.models import (
    ExchangeEnum,
    InstrumentType,
    StandardExchangeInfo,
    TradingPair,
)
from reference_data.core.event_bus import EventBus
from reference_data.core.watcher import (
    ChangeEvent,
    ExchangeWatchConfig,
    ExchangeWatcher,
    WatchableField,
    load_watch_configs_from_dict,
)


class MockService:
    """Mock service for testing the watcher."""

    def __init__(self) -> None:
        self._bus: EventBus[StandardExchangeInfo] = EventBus()

    def subscribe(self, callback):
        return self._bus.subscribe(callback)

    def publish(self, info: StandardExchangeInfo) -> None:
        """Publish an update (for testing)."""
        self._bus.publish(info)


def create_exchange_info(
    exchange: ExchangeEnum,
    pairs: list[tuple[str, InstrumentType]],
    timestamp: int = 1000,
) -> StandardExchangeInfo:
    """Helper to create StandardExchangeInfo for testing."""
    trading_pairs = [
        TradingPair(
            instrument_type=inst_type,
            symbol=symbol,
            base_asset=symbol.split("-")[0] if "-" in symbol else symbol[:3],
            quote_asset=symbol.split("-")[1] if "-" in symbol else symbol[3:],
        )
        for symbol, inst_type in pairs
    ]
    return StandardExchangeInfo(
        exchange=exchange,
        timestamp=timestamp,
        trading_pairs=trading_pairs,
    )


class TestWatchableField:
    """Test WatchableField enum."""

    def test_watchable_field_values(self):
        """Test that WatchableField has expected values."""
        assert WatchableField.PAIR_COUNT == "pair_count"
        assert WatchableField.PAIR_SYMBOLS == "pair_symbols"
        assert WatchableField.SPOT_PAIR_COUNT == "spot_pair_count"
        assert WatchableField.PERPETUAL_PAIR_COUNT == "perpetual_pair_count"
        assert WatchableField.FUTURES_PAIR_COUNT == "futures_pair_count"
        assert WatchableField.TIMESTAMP == "timestamp"


class TestExchangeWatchConfig:
    """Test ExchangeWatchConfig dataclass."""

    def test_create_config(self):
        """Test creating a watch config."""
        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_COUNT, WatchableField.PAIR_SYMBOLS],
        )
        assert config.exchange == ExchangeEnum.BINANCE
        assert len(config.fields) == 2
        assert WatchableField.PAIR_COUNT in config.fields


class TestChangeEvent:
    """Test ChangeEvent dataclass."""

    def test_create_change_event(self):
        """Test creating a change event."""
        event = ChangeEvent(
            exchange=ExchangeEnum.BINANCE,
            field=WatchableField.PAIR_COUNT,
            old_value=10,
            new_value=15,
        )
        assert event.exchange == ExchangeEnum.BINANCE
        assert event.field == WatchableField.PAIR_COUNT
        assert event.old_value == 10
        assert event.new_value == 15


class TestExchangeWatcher:
    """Test ExchangeWatcher class."""

    def test_watcher_detects_pair_count_change(self):
        """Test that watcher detects pair count changes."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_COUNT],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Emit initial state
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE,
                [("BTC-USDT", InstrumentType.SPOT)],
            )
        )

        # Emit changed state
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE,
                [("BTC-USDT", InstrumentType.SPOT), ("ETH-USDT", InstrumentType.SPOT)],
            )
        )

        assert len(events) == 1
        assert events[0].exchange == ExchangeEnum.BINANCE
        assert events[0].field == WatchableField.PAIR_COUNT
        assert events[0].old_value == 1
        assert events[0].new_value == 2

        watcher.stop()

    def test_watcher_ignores_other_exchanges(self):
        """Test that watcher only reacts to configured exchange."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_COUNT],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Emit for different exchange
        service.publish(
            create_exchange_info(ExchangeEnum.OKX, [("BTC-USDT", InstrumentType.SPOT)])
        )
        service.publish(
            create_exchange_info(
                ExchangeEnum.OKX,
                [("BTC-USDT", InstrumentType.SPOT), ("ETH-USDT", InstrumentType.SPOT)],
            )
        )

        assert len(events) == 0

        watcher.stop()

    def test_watcher_no_event_when_unchanged(self):
        """Test that watcher doesn't emit when value unchanged."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_COUNT],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Emit same state twice
        info = create_exchange_info(
            ExchangeEnum.BINANCE, [("BTC-USDT", InstrumentType.SPOT)]
        )
        service.publish(info)
        service.publish(info)

        assert len(events) == 0

        watcher.stop()

    def test_watcher_detects_symbol_changes(self):
        """Test that watcher detects symbol set changes."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_SYMBOLS],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Initial symbols
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE, [("BTC-USDT", InstrumentType.SPOT)]
            )
        )

        # Changed symbols
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE, [("ETH-USDT", InstrumentType.SPOT)]
            )
        )

        assert len(events) == 1
        assert events[0].field == WatchableField.PAIR_SYMBOLS
        assert "BTC-USDT" in events[0].old_value
        assert "ETH-USDT" in events[0].new_value

        watcher.stop()

    def test_watcher_detects_instrument_type_counts(self):
        """Test that watcher detects instrument type count changes."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.SPOT_PAIR_COUNT, WatchableField.PERPETUAL_PAIR_COUNT],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Initial: 1 spot
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE, [("BTC-USDT", InstrumentType.SPOT)]
            )
        )

        # Add perpetual
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE,
                [
                    ("BTC-USDT", InstrumentType.SPOT),
                    ("BTC-USDT-PERP", InstrumentType.PERPETUAL),
                ],
            )
        )

        assert len(events) == 1
        assert events[0].field == WatchableField.PERPETUAL_PAIR_COUNT
        assert events[0].old_value == 0
        assert events[0].new_value == 1

        watcher.stop()

    def test_watcher_multiple_exchanges(self):
        """Test watcher with multiple exchange configs."""
        service = MockService()
        events: list[ChangeEvent] = []

        configs = [
            ExchangeWatchConfig(
                exchange=ExchangeEnum.BINANCE, fields=[WatchableField.PAIR_COUNT]
            ),
            ExchangeWatchConfig(
                exchange=ExchangeEnum.OKX, fields=[WatchableField.PAIR_COUNT]
            ),
        ]
        watcher = ExchangeWatcher(configs)
        watcher.start(service, events.append)

        # Binance initial
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE, [("BTC-USDT", InstrumentType.SPOT)]
            )
        )
        # OKX initial
        service.publish(
            create_exchange_info(ExchangeEnum.OKX, [("BTC-USDT", InstrumentType.SPOT)])
        )

        # Binance change
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE,
                [("BTC-USDT", InstrumentType.SPOT), ("ETH-USDT", InstrumentType.SPOT)],
            )
        )
        # OKX change
        service.publish(
            create_exchange_info(
                ExchangeEnum.OKX,
                [("BTC-USDT", InstrumentType.SPOT), ("ETH-USDT", InstrumentType.SPOT)],
            )
        )

        assert len(events) == 2
        exchanges = {e.exchange for e in events}
        assert ExchangeEnum.BINANCE in exchanges
        assert ExchangeEnum.OKX in exchanges

        watcher.stop()

    def test_watcher_stop_disposes_subscriptions(self):
        """Test that stop() disposes all subscriptions."""
        service = MockService()
        events: list[ChangeEvent] = []

        config = ExchangeWatchConfig(
            exchange=ExchangeEnum.BINANCE,
            fields=[WatchableField.PAIR_COUNT],
        )
        watcher = ExchangeWatcher([config])
        watcher.start(service, events.append)

        # Initial
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE, [("BTC-USDT", InstrumentType.SPOT)]
            )
        )

        watcher.stop()

        # Should not receive this after stop
        service.publish(
            create_exchange_info(
                ExchangeEnum.BINANCE,
                [("BTC-USDT", InstrumentType.SPOT), ("ETH-USDT", InstrumentType.SPOT)],
            )
        )

        assert len(events) == 0


class TestLoadWatchConfigsFromDict:
    """Test config loading from dict."""

    def test_load_single_exchange(self):
        """Test loading config for single exchange."""
        data = {
            "exchanges": [
                {
                    "exchange": "Binance",
                    "fields": ["pair_count", "pair_symbols"],
                }
            ]
        }

        configs = load_watch_configs_from_dict(data)

        assert len(configs) == 1
        assert configs[0].exchange == ExchangeEnum.BINANCE
        assert len(configs[0].fields) == 2
        assert WatchableField.PAIR_COUNT in configs[0].fields
        assert WatchableField.PAIR_SYMBOLS in configs[0].fields

    def test_load_multiple_exchanges(self):
        """Test loading config for multiple exchanges."""
        data = {
            "exchanges": [
                {"exchange": "Binance", "fields": ["pair_count"]},
                {"exchange": "OKX", "fields": ["spot_pair_count", "perpetual_pair_count"]},
                {"exchange": "Coinbase", "fields": ["pair_count"]},
            ]
        }

        configs = load_watch_configs_from_dict(data)

        assert len(configs) == 3
        assert configs[0].exchange == ExchangeEnum.BINANCE
        assert configs[1].exchange == ExchangeEnum.OKX
        assert configs[2].exchange == ExchangeEnum.COINBASE

    def test_load_all_field_types(self):
        """Test loading all watchable field types."""
        data = {
            "exchanges": [
                {
                    "exchange": "Binance",
                    "fields": [
                        "pair_count",
                        "pair_symbols",
                        "spot_pair_count",
                        "perpetual_pair_count",
                        "futures_pair_count",
                        "timestamp",
                    ],
                }
            ]
        }

        configs = load_watch_configs_from_dict(data)

        assert len(configs[0].fields) == 6
