"""
Data models for the reference data service.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class InstrumentType(str, Enum):
    """Enumeration for instrument types."""

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURES = "futures"


class ExchangeEnum(str, Enum):
    """Enumeration for supported exchanges."""

    BINANCE = "Binance"
    OKX = "OKX"
    BYBIT = "Bybit"
    BITGET = "Bitget"
    COINBASE = "Coinbase"
    COINBASE_INTL = "CoinbaseInternational"


class TradingPair(BaseModel):
    """Standardized model for a trading pair."""

    instrument_type: InstrumentType
    symbol: str
    base_asset: str
    quote_asset: str
    status: Optional[str] = None
    tick_size: Optional[float] = None
    lot_size: Optional[float] = None
    contract_size: Optional[float] = None
    settlement_currency: Optional[str] = None
    expiry: Optional[str] = None


class StandardExchangeInfo(BaseModel):
    """Standardized exchange information model."""

    exchange: ExchangeEnum
    timestamp: int
    trading_pairs: List[TradingPair]
