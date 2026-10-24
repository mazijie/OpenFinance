"""Logging helpers."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured application logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "event"):
            payload["event"] = record.event
        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            payload.update(record.extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class StructuredLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter supporting event and structured fields."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        event = kwargs.pop("event", None)
        extra_fields = kwargs.pop("extra_fields", None)
        if event is not None:
            extra["event"] = event
        if extra_fields is not None:
            extra["extra_fields"] = extra_fields
        return msg, kwargs


_DEF_HANDLER_NAME = "openfinance_json_handler"


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging once."""

    root_logger = logging.getLogger()
    if any(getattr(handler, "name", "") == _DEF_HANDLER_NAME for handler in root_logger.handlers):
        root_logger.setLevel(level.upper())
        return

    handler = logging.StreamHandler()
    handler.name = _DEF_HANDLER_NAME
    handler.setFormatter(JsonFormatter())

    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())


def get_logger(name: str) -> StructuredLoggerAdapter:
    """Return structured logger adapter."""

    return StructuredLoggerAdapter(logging.getLogger(name), {})
