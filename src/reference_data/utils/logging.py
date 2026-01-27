"""
Structured logging utilities for the reference data service.
"""

import json
import logging
import sys
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Outputs logs as JSON objects for easy parsing by log aggregators.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields from record
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in {
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "message", "taskName"
            }
        }
        if extra_fields:
            log_data["context"] = extra_fields

        return json.dumps(log_data)


class ExchangeLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that automatically includes exchange context.
    """

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def get_exchange_logger(exchange: str) -> ExchangeLoggerAdapter:
    """
    Get a logger with exchange context automatically included.

    Args:
        exchange: Exchange name to include in all log messages

    Returns:
        Logger adapter with exchange context
    """
    logger = logging.getLogger(f"reference_data.{exchange.lower()}")
    return ExchangeLoggerAdapter(logger, {"exchange": exchange})


def setup_logging(
    level: int = logging.INFO,
    structured: bool = False,
    stream: Optional[Any] = None,
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (default: INFO)
        structured: Use JSON structured logging (default: False)
        stream: Output stream (default: sys.stdout)
    """
    root_logger = logging.getLogger("reference_data")
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers.clear()

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setLevel(level)

    if structured:
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )

    root_logger.addHandler(handler)


def log_exchange_event(
    logger: logging.Logger,
    level: int,
    message: str,
    exchange: str,
    instrument_type: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """
    Log an exchange-related event with structured context.

    Args:
        logger: Logger instance
        level: Log level
        message: Log message
        exchange: Exchange name
        instrument_type: Optional instrument type
        **kwargs: Additional context fields
    """
    extra = {"exchange": exchange}
    if instrument_type:
        extra["instrument_type"] = instrument_type
    extra.update(kwargs)

    logger.log(level, message, extra=extra)
