"""Centralized correlation context for AWET pipeline.

Provides a single ContextVar-based store so every agent, script, and
middleware shares the same ``correlation_id`` without import cycles.

Usage
-----
    from src.core.context import correlation_ctx, new_correlation_id

    token = correlation_ctx.set(new_correlation_id())
    ...
    correlation_ctx.reset(token)
"""
from __future__ import annotations

import contextvars
import uuid

correlation_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def new_correlation_id() -> str:
    """Generate a new UUID4 correlation id."""
    return str(uuid.uuid4())


def get_correlation_id() -> str | None:
    """Read the current correlation id (may be ``None``)."""
    return correlation_ctx.get()


def set_correlation_id(value: str | None) -> contextvars.Token[str | None]:
    """Write a correlation id into the current context.

    Returns the token so callers can ``correlation_ctx.reset(token)``
    if they need to restore the previous value.
    """
    return correlation_ctx.set(value)


def ensure_correlation_id() -> str:
    """Return the current id or generate one if missing."""
    cid = correlation_ctx.get()
    if cid is None:
        cid = new_correlation_id()
        correlation_ctx.set(cid)
    return cid
