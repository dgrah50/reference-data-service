"""
Exchange field watcher for detecting changes in exchange data streams.
"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

from ..api.models import ExchangeEnum, InstrumentType, StandardExchangeInfo

if TYPE_CHECKING:
    from .service import ReferenceDataService


class WatchableField(str, Enum):
    """All possible watchable fields across exchanges."""

    PAIR_COUNT = "pair_count"
    PAIR_SYMBOLS = "pair_symbols"
    SPOT_PAIR_COUNT = "spot_pair_count"
    PERPETUAL_PAIR_COUNT = "perpetual_pair_count"
    FUTURES_PAIR_COUNT = "futures_pair_count"
    TIMESTAMP = "timestamp"


def _count_by_instrument_type(info: StandardExchangeInfo, instrument_type: InstrumentType) -> int:
    """Count trading pairs by instrument type."""
    return sum(1 for p in info.trading_pairs if p.instrument_type == instrument_type)


# Field extractors - maps field enum to extraction logic
FIELD_EXTRACTORS: dict[WatchableField, Callable[[StandardExchangeInfo], Any]] = {
    WatchableField.PAIR_COUNT: lambda info: len(info.trading_pairs),
    WatchableField.PAIR_SYMBOLS: lambda info: frozenset(p.symbol for p in info.trading_pairs),
    WatchableField.SPOT_PAIR_COUNT: lambda info: _count_by_instrument_type(info, InstrumentType.SPOT),
    WatchableField.PERPETUAL_PAIR_COUNT: lambda info: _count_by_instrument_type(
        info, InstrumentType.PERPETUAL
    ),
    WatchableField.FUTURES_PAIR_COUNT: lambda info: _count_by_instrument_type(
        info, InstrumentType.FUTURES
    ),
    WatchableField.TIMESTAMP: lambda info: info.timestamp,
}


@dataclass
class ExchangeWatchConfig:
    """Configuration for watching fields on a single exchange."""

    exchange: ExchangeEnum
    fields: list[WatchableField]


@dataclass
class ChangeEvent:
    """Emitted when a watched field changes."""

    exchange: ExchangeEnum
    field: WatchableField
    old_value: Any
    new_value: Any


class ExchangeWatcher:
    """Watches configured fields across exchanges."""

    def __init__(self, configs: list[ExchangeWatchConfig]):
        self.configs = configs
        self._unsubscribes: list[Callable[[], None]] = []
        self._previous: dict[tuple[ExchangeEnum, WatchableField], Any] = {}

    def start(
        self,
        service: "ReferenceDataService",
        on_change: Callable[[ChangeEvent], None],
    ) -> None:
        """
        Start watching all configured fields.

        Args:
            service: The ReferenceDataService to subscribe to.
            on_change: Callback invoked when a watched field changes.
        """

        def handle_update(info: StandardExchangeInfo) -> None:
            for config in self.configs:
                if info.exchange != config.exchange:
                    continue
                for field in config.fields:
                    key = (config.exchange, field)
                    extractor = FIELD_EXTRACTORS[field]
                    new_value = extractor(info)
                    old_value = self._previous.get(key)

                    if old_value is not None and new_value != old_value:
                        on_change(
                            ChangeEvent(
                                exchange=config.exchange,
                                field=field,
                                old_value=old_value,
                                new_value=new_value,
                            )
                        )
                    self._previous[key] = new_value

        self._unsubscribes.append(service.subscribe(handle_update))

    def stop(self) -> None:
        """Stop all watchers."""
        for unsub in self._unsubscribes:
            unsub()
        self._unsubscribes.clear()
        self._previous.clear()


def load_watch_configs_from_dict(data: dict) -> list[ExchangeWatchConfig]:
    """Load watch configs from a dictionary (parsed YAML/JSON)."""
    return [
        ExchangeWatchConfig(
            exchange=ExchangeEnum(ex["exchange"]),
            fields=[WatchableField(f) for f in ex["fields"]],
        )
        for ex in data["exchanges"]
    ]
