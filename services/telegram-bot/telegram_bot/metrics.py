"""Prometheus metrics for the Telegram bot.

Counter / histogram definitions are module-level singletons so that
multiple callers always reference the same registry objects.
"""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ── Registry ────────────────────────────────────────────────────────────────
# Use a dedicated registry so we don't accidentally pull in process metrics
# from other libraries in the same process.
REGISTRY = CollectorRegistry(auto_describe=True)

# ── Counters ────────────────────────────────────────────────────────────────
TELEGRAM_UPDATES_TOTAL = Counter(
    "telegram_updates_total",
    "Total Telegram updates received, by update type.",
    ["type"],          # message | command | other
    registry=REGISTRY,
)

BOT_REPLIES_TOTAL = Counter(
    "bot_replies_total",
    "Total bot replies sent to Telegram, by outcome.",
    ["status"],        # ok | error
    registry=REGISTRY,
)

KB_QUERY_REQUESTS_TOTAL = Counter(
    "kb_query_requests_total",
    "Total HTTP requests made to kb-query, by outcome.",
    ["status"],        # ok | error | timeout
    registry=REGISTRY,
)

RATE_LIMITED_TOTAL = Counter(
    "bot_rate_limited_total",
    "Messages dropped due to per-chat rate limiting.",
    registry=REGISTRY,
)

# ── Histograms ───────────────────────────────────────────────────────────────
KB_QUERY_LATENCY = Histogram(
    "kb_query_latency_seconds",
    "End-to-end latency of kb-query HTTP calls.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY,
)


def render_metrics() -> tuple[bytes, str]:
    """Return (body_bytes, content_type) for the /metrics endpoint."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
