"""Tests for structured logging utilities."""

import json
import logging
from io import StringIO

from reference_data.utils.logging import (
    StructuredFormatter,
    ExchangeLoggerAdapter,
    get_exchange_logger,
    setup_logging,
    log_exchange_event,
)


class TestStructuredFormatter:
    """Test StructuredFormatter."""

    def test_formats_as_json(self):
        """Test that formatter outputs valid JSON."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["message"] == "Test message"
        assert data["logger"] == "test"

    def test_includes_extra_fields(self):
        """Test that extra fields are included in context."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.exchange = "binance"
        record.pair_count = 100

        output = formatter.format(record)
        data = json.loads(output)

        assert data["context"]["exchange"] == "binance"
        assert data["context"]["pair_count"] == 100


class TestExchangeLoggerAdapter:
    """Test ExchangeLoggerAdapter."""

    def test_adds_exchange_context(self):
        """Test that adapter adds exchange to extra."""
        logger = logging.getLogger("test")
        adapter = ExchangeLoggerAdapter(logger, {"exchange": "binance"})

        msg, kwargs = adapter.process("Test", {})

        assert kwargs["extra"]["exchange"] == "binance"


class TestGetExchangeLogger:
    """Test get_exchange_logger function."""

    def test_returns_adapter_with_exchange(self):
        """Test that function returns properly configured adapter."""
        adapter = get_exchange_logger("Binance")

        assert isinstance(adapter, ExchangeLoggerAdapter)
        assert adapter.extra["exchange"] == "Binance"


class TestSetupLogging:
    """Test setup_logging function."""

    def test_setup_basic_logging(self):
        """Test basic logging setup."""
        stream = StringIO()
        setup_logging(level=logging.INFO, structured=False, stream=stream)

        logger = logging.getLogger("reference_data.test")
        logger.info("Test message")

        output = stream.getvalue()
        assert "Test message" in output

    def test_setup_structured_logging(self):
        """Test structured JSON logging setup."""
        stream = StringIO()
        setup_logging(level=logging.INFO, structured=True, stream=stream)

        logger = logging.getLogger("reference_data.test")
        logger.info("Test message")

        output = stream.getvalue()
        data = json.loads(output)
        assert data["message"] == "Test message"
