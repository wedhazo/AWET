"""Prometheus metrics for reddit-ingestor.

All metrics use a private registry so they don't interfere with the
main AWET metrics registry when running in the same process for tests.
"""
from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)

REGISTRY = CollectorRegistry()

PUBLISHED_TOTAL = Counter(
    "reddit_ingestor_published_total",
    "Events successfully queued for Kafka delivery",
    registry=REGISTRY,
)

DEDUPED_TOTAL = Counter(
    "reddit_ingestor_deduped_total",
    "Events skipped because content_hash was already seen",
    registry=REGISTRY,
)

PROVIDER_ERRORS_TOTAL = Counter(
    "reddit_ingestor_provider_errors_total",
    "Errors returned by the Reddit provider",
    registry=REGISTRY,
)

KAFKA_ERRORS_TOTAL = Counter(
    "reddit_ingestor_kafka_errors_total",
    "Kafka produce / delivery errors",
    registry=REGISTRY,
)

DLQ_TOTAL = Counter(
    "reddit_ingestor_dlq_total",
    "Events routed to the DLQ topic",
    registry=REGISTRY,
)

INGESTION_LATENCY = Histogram(
    "reddit_ingestor_ingestion_latency_seconds",
    "End-to-end latency of one poll cycle",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    registry=REGISTRY,
)

REDIS_UNAVAILABLE_TOTAL = Counter(
    "reddit_ingestor_redis_unavailable_total",
    "Times Redis was unreachable during dedup check",
    registry=REGISTRY,
)


def metrics_body() -> bytes:
    """Serialise the registry in Prometheus exposition format."""
    return generate_latest(REGISTRY)


def metrics_content_type() -> str:
    return CONTENT_TYPE_LATEST
