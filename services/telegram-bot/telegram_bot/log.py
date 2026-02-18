"""Structured JSON logging with mandatory correlation_id field.

Every log call must include ``correlation_id``. If callers omit it the
logging module injects "MISSING" so that Logstash / Kibana alerts on it
rather than silently dropping the field.
"""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


SERVICE_NAME = "awet-telegram-gateway"


def configure_logging(level: str = "INFO") -> None:
    """Wire structlog + stdlib logging to emit newline-delimited JSON."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    # Keep logs structured JSON only (suppress verbose httpx/httpcore request logs).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _ensure_correlation_id,
            _ensure_service,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts_utc"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _ensure_correlation_id(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Inject placeholder if caller forgot correlation_id."""
    if "correlation_id" not in event_dict:
        event_dict["correlation_id"] = "MISSING"
    return event_dict


def _ensure_service(logger: Any, method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    if "service" not in event_dict:
        event_dict["service"] = SERVICE_NAME
    return event_dict


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
