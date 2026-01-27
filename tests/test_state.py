"""
Tests for the exchange state management.
"""

from reference_data.adapters.state import ExchangeState
from reference_data.api.models import (
    StandardExchangeInfo,
    TradingPair,
    ExchangeEnum,
    InstrumentType,
)


def create_standard_info(pairs: list) -> StandardExchangeInfo:
    """Helper function to create StandardExchangeInfo instances."""
    return StandardExchangeInfo(
        exchange=ExchangeEnum.BINANCE, timestamp=1625097600000, trading_pairs=pairs
    )


def create_trading_pair(symbol: str, status: str = "TRADING") -> TradingPair:
    """Helper function to create TradingPair instances."""
    base, quote = symbol.split("-")
    return TradingPair(
        instrument_type=InstrumentType.SPOT,
        symbol=symbol,
        base_asset=base,
        quote_asset=quote,
        status=status,
        tick_size=0.01,
        lot_size=0.00001,
    )


def test_state_initialization():
    """Test that the state initializes correctly."""
    state = ExchangeState()
    assert state.current_state is None


def test_initial_state_update():
    """Test updating state from None."""
    state = ExchangeState()

    # Create initial data
    pairs = [create_trading_pair("BTC-USDT")]
    info = create_standard_info(pairs)

    # Mock converter function
    def converter(data):
        return info

    # Update state
    result = state.update_state({"data": []}, converter)

    # Verify result
    assert result == info
    assert state.current_state == info


def test_state_update_with_changes():
    """Test updating state with changes."""
    state = ExchangeState()

    # Create initial state
    initial_pairs = [create_trading_pair("BTC-USDT")]
    initial_info = create_standard_info(initial_pairs)

    # Set initial state
    def initial_converter(data):
        return initial_info

    state.update_state({"data": []}, initial_converter)

    # Create updated state with new pair
    updated_pairs = [create_trading_pair("BTC-USDT"), create_trading_pair("ETH-USDT")]
    updated_info = create_standard_info(updated_pairs)

    # Update state
    def update_converter(data):
        return updated_info

    result = state.update_state({"data": []}, update_converter)

    # Verify result
    assert result == updated_info
    assert state.current_state == updated_info
    assert len(state.current_state.trading_pairs) == 2


def test_state_update_no_changes():
    """Test updating state with no changes."""
    state = ExchangeState()

    # Create initial state
    pairs = [create_trading_pair("BTC-USDT")]
    info = create_standard_info(pairs)

    # Set initial state
    def converter(data):
        return info

    state.update_state({"data": []}, converter)

    # Update with same state
    result = state.update_state({"data": []}, converter)

    # Verify no update occurred
    assert result is None
    assert state.current_state == info


def test_state_update_with_status_change():
    """Test updating state with trading pair status change."""
    state = ExchangeState()

    # Create initial state
    initial_pairs = [create_trading_pair("BTC-USDT", status="TRADING")]
    initial_info = create_standard_info(initial_pairs)

    # Set initial state
    def initial_converter(data):
        return initial_info

    state.update_state({"data": []}, initial_converter)

    # Create updated state with status change
    updated_pairs = [create_trading_pair("BTC-USDT", status="BREAK")]
    updated_info = create_standard_info(updated_pairs)

    # Update state
    def update_converter(data):
        return updated_info

    result = state.update_state({"data": []}, update_converter)

    # Verify result
    assert result == updated_info
    assert state.current_state == updated_info
    assert state.current_state.trading_pairs[0].status == "BREAK"


def test_apply_diff():
    """Test applying a diff update to the state."""
    state = ExchangeState()

    # Create initial state
    initial_pairs = [create_trading_pair("BTC-USDT")]
    initial_info = create_standard_info(initial_pairs)

    # Set initial state
    def initial_converter(data):
        return initial_info

    state.update_state({"data": []}, initial_converter)

    # Create a diff update
    diff_data = {"symbol": "BTC-USDT", "status": "BREAK"}

    def diff_converter(current, diff):
        current_dict = current.copy()
        pairs = current_dict["trading_pairs"]
        for pair in pairs:
            if pair["symbol"] == diff["symbol"]:
                pair["status"] = diff["status"]
        return current_dict

    result = state.apply_diff(diff_data, diff_converter)

    # Verify result
    assert result is not None
    assert result.trading_pairs[0].status == "BREAK"
    assert state.current_state == result


def test_apply_diff_no_changes():
    """Test applying a diff that results in no changes."""
    state = ExchangeState()

    # Create initial state
    initial_pairs = [create_trading_pair("BTC-USDT")]
    initial_info = create_standard_info(initial_pairs)

    # Set initial state
    def initial_converter(data):
        return initial_info

    state.update_state({"data": []}, initial_converter)

    # Create a diff update with same data
    diff_data = {"symbol": "BTC-USDT", "status": "TRADING"}

    def diff_converter(current, diff):
        return current.copy()

    result = state.apply_diff(diff_data, diff_converter)

    # Verify no update occurred
    assert result is None
    assert state.current_state == initial_info


def test_apply_diff_without_initial_state():
    """Test applying a diff without initial state."""
    state = ExchangeState()

    # Try to apply diff without initial state
    def diff_converter(current, diff):
        return current

    result = state.apply_diff({"data": []}, diff_converter)

    # Verify operation failed gracefully
    assert result is None
    assert state.current_state is None
