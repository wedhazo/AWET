from __future__ import annotations

import contextvars
import logging
from typing import Any, Callable

import structlog
from fastapi import Request, Response

correlation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def set_correlation_id(value: str | None) -> None:
    correlation_id_var.set(value)


def get_correlation_id() -> str | None:
    return correlation_id_var.get()


def add_correlation_id(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    event_dict["correlation_id"] = get_correlation_id()
    return event_dict


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_correlation_id,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )


def correlation_middleware(header_name: str) -> Callable[[Request, Callable], Any]:
    async def middleware(request: Request, call_next: Callable) -> Response:
        correlation_id = request.headers.get(header_name)
        set_correlation_id(correlation_id)
        response = await call_next(request)
        if correlation_id:
            response.headers[header_name] = correlation_id
        return response

    return middleware
