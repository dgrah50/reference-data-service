"""
Tests for the circuit breaker module.
"""

import pytest
import time
from unittest.mock import patch

from reference_data.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
)


@pytest.fixture
def default_config():
    """Fixture providing default circuit breaker configuration."""
    return CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout=1.0,
        half_open_max_calls=1,
    )


@pytest.fixture
def circuit_breaker(default_config):
    """Fixture providing a circuit breaker instance."""
    return CircuitBreaker("test_exchange", default_config)


class TestCircuitBreakerInitialization:
    """Tests for circuit breaker initialization."""

    def test_circuit_starts_closed(self, circuit_breaker):
        """Test that circuit starts in closed state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.is_available is True

    def test_circuit_starts_with_zero_counts(self, circuit_breaker):
        """Test that circuit starts with zero failure and success counts."""
        assert circuit_breaker._failure_count == 0
        assert circuit_breaker._success_count == 0
        assert circuit_breaker._half_open_calls == 0

    def test_circuit_uses_default_config_when_none_provided(self):
        """Test that circuit uses default config when none is provided."""
        cb = CircuitBreaker("test")
        assert cb.config.failure_threshold == 5
        assert cb.config.success_threshold == 2
        assert cb.config.timeout == 60.0
        assert cb.config.half_open_max_calls == 1


class TestCircuitBreakerOpensAfterFailures:
    """Tests for circuit opening after failure threshold."""

    @pytest.mark.asyncio
    async def test_opens_after_failure_threshold(self, circuit_breaker):
        """Test that circuit opens after reaching failure threshold."""
        # Simulate failures up to threshold
        for _ in range(3):  # failure_threshold = 3
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.is_available is False

    @pytest.mark.asyncio
    async def test_stays_closed_below_threshold(self, circuit_breaker):
        """Test that circuit stays closed when below failure threshold."""
        # Simulate failures below threshold
        for _ in range(2):  # failure_threshold = 3
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.is_available is True

    @pytest.mark.asyncio
    async def test_failure_count_resets_on_success(self, circuit_breaker):
        """Test that failure count resets after a successful call."""
        # Simulate two failures
        for _ in range(2):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        # One success
        async with circuit_breaker:
            pass

        assert circuit_breaker._failure_count == 0

        # Two more failures should not open circuit
        for _ in range(2):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.CLOSED


class TestCircuitBreakerRejectsWhenOpen:
    """Tests for circuit rejecting calls when open."""

    @pytest.mark.asyncio
    async def test_rejects_calls_when_open(self, circuit_breaker):
        """Test that circuit rejects calls when in open state."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        # Try to make a call
        with pytest.raises(CircuitOpenError) as exc_info:
            async with circuit_breaker:
                pass

        assert "is open" in str(exc_info.value)
        assert circuit_breaker.name in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_is_available_returns_false_when_open(self, circuit_breaker):
        """Test that is_available returns False when circuit is open."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.is_available is False


class TestCircuitBreakerHalfOpen:
    """Tests for circuit breaker half-open state transitions."""

    @pytest.mark.asyncio
    async def test_transitions_to_half_open_after_timeout(self, circuit_breaker):
        """Test that circuit transitions to half-open after timeout."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.OPEN

        # Mock time to simulate timeout elapsed
        original_time = circuit_breaker._last_failure_time
        with patch("time.monotonic", return_value=original_time + 2.0):
            # Check is_available sees the timeout elapsed
            assert circuit_breaker.is_available is True

            # Enter context to trigger transition
            async with circuit_breaker:
                pass

        assert circuit_breaker.state in [CircuitState.HALF_OPEN, CircuitState.CLOSED]

    @pytest.mark.asyncio
    async def test_closes_after_success_threshold_in_half_open(self, circuit_breaker):
        """Test that circuit closes after reaching success threshold in half-open."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        # Simulate timeout and transition to half-open
        original_time = circuit_breaker._last_failure_time
        with patch("time.monotonic", return_value=original_time + 2.0):
            # First success in half-open
            async with circuit_breaker:
                pass

            # Circuit should be in half-open (need 2 successes to close)
            # Note: half_open_max_calls=1, so we need to reset for next call
            circuit_breaker._half_open_calls = 0

            # Second success
            async with circuit_breaker:
                pass

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker._failure_count == 0
        assert circuit_breaker._success_count == 0

    @pytest.mark.asyncio
    async def test_returns_to_open_on_failure_in_half_open(self, circuit_breaker):
        """Test that circuit returns to open on failure in half-open state."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        # Simulate timeout and transition to half-open
        original_time = circuit_breaker._last_failure_time
        with patch("time.monotonic", return_value=original_time + 2.0):
            # Failure in half-open
            try:
                async with circuit_breaker:
                    raise Exception("Failure in half-open")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker._half_open_calls == 0

    @pytest.mark.asyncio
    async def test_max_calls_in_half_open(self, circuit_breaker):
        """Test that circuit limits concurrent calls in half-open state."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        # Manually set to half-open
        circuit_breaker._state = CircuitState.HALF_OPEN
        circuit_breaker._half_open_calls = 1  # Already at max

        # Try to make another call
        with pytest.raises(CircuitOpenError) as exc_info:
            async with circuit_breaker:
                pass

        assert "max calls reached" in str(exc_info.value)


class TestCircuitBreakerReset:
    """Tests for circuit breaker manual reset."""

    @pytest.mark.asyncio
    async def test_reset_returns_to_closed(self, circuit_breaker):
        """Test that reset returns circuit to closed state."""
        # Open the circuit
        for _ in range(3):
            try:
                async with circuit_breaker:
                    raise Exception("Simulated failure")
            except Exception:
                pass

        assert circuit_breaker.state == CircuitState.OPEN

        # Reset
        circuit_breaker.reset()

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker._failure_count == 0
        assert circuit_breaker._success_count == 0
        assert circuit_breaker._last_failure_time is None
        assert circuit_breaker._half_open_calls == 0
        assert circuit_breaker.is_available is True


class TestCircuitBreakerConfig:
    """Tests for circuit breaker configuration."""

    def test_custom_failure_threshold(self):
        """Test circuit with custom failure threshold."""
        config = CircuitBreakerConfig(failure_threshold=10)
        cb = CircuitBreaker("test", config)
        assert cb.config.failure_threshold == 10

    def test_custom_success_threshold(self):
        """Test circuit with custom success threshold."""
        config = CircuitBreakerConfig(success_threshold=5)
        cb = CircuitBreaker("test", config)
        assert cb.config.success_threshold == 5

    def test_custom_timeout(self):
        """Test circuit with custom timeout."""
        config = CircuitBreakerConfig(timeout=120.0)
        cb = CircuitBreaker("test", config)
        assert cb.config.timeout == 120.0

    def test_custom_half_open_max_calls(self):
        """Test circuit with custom half-open max calls."""
        config = CircuitBreakerConfig(half_open_max_calls=3)
        cb = CircuitBreaker("test", config)
        assert cb.config.half_open_max_calls == 3
