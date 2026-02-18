"""Kafka producer wrapper for reddit-ingestor.

Handles:
- Publishing to ``social.reddit.raw`` using the Phase-1 Avro schema.
- DLQ routing to ``dlq.social.reddit.raw`` after exhausting retries.
- Delivery confirmation callbacks with structured logging.
"""
from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import structlog
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
)
from confluent_kafka.serializing_producer import SerializingProducer
from reddit_ingestor.metrics import (
    DLQ_TOTAL,
    KAFKA_ERRORS_TOTAL,
    PUBLISHED_TOTAL,
)

logger = structlog.get_logger("reddit_ingestor.publisher")


def _now_utc() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class EventPublisher:
    """Publishes raw Reddit events to Kafka with DLQ fallback."""

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        raw_topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._raw_topic = raw_topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries
        self._bootstrap = bootstrap_servers
        self._registry = SchemaRegistryClient({"url": schema_registry_url})

        # Load schemas from disk (file shipped with the image / repo)
        self._raw_schema_str = self._load_schema("social.reddit.raw.v1")
        self._dlq_schema_str = self._load_schema("dlq.social.reddit.v1")

        # Build producers with Avro serializers
        self._raw_producer = self._make_producer(self._raw_schema_str)
        self._dlq_producer = self._make_producer(self._dlq_schema_str)
        logger.info(
            "publisher_init",
            raw_topic=raw_topic,
            dlq_topic=dlq_topic,
            bootstrap=bootstrap_servers,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _load_schema(name: str) -> str:
        """Load an Avro schema JSON string from schemas/avro/."""
        import pathlib

        # Try repo root first, then relative to CWD
        for base in (
            pathlib.Path(__file__).resolve().parents[3],  # repo root
            pathlib.Path.cwd(),
        ):
            path = base / "schemas" / "avro" / f"{name}.avsc"
            if path.exists():
                return path.read_text()
        msg = f"Schema file not found: {name}.avsc"
        raise FileNotFoundError(msg)

    def _make_producer(self, schema_str: str) -> SerializingProducer:
        avro_ser = AvroSerializer(self._registry, schema_str)
        return SerializingProducer(
            {
                "bootstrap.servers": self._bootstrap,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": avro_ser,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 200,
                "linger.ms": 10,
                "compression.type": "lz4",
                "request.timeout.ms": 30_000,
                "delivery.timeout.ms": 120_000,
            },
        )

    # ------------------------------------------------------------------
    def publish_raw(self, event: dict[str, Any], key: str) -> None:
        """Produce one social.reddit.raw event, routing to DLQ on failure."""
        try:
            self._raw_producer.produce(
                topic=self._raw_topic,
                key=key,
                value=event,
                on_delivery=self._on_delivery,
            )
            PUBLISHED_TOTAL.inc()
        except (KafkaException, BufferError) as exc:
            KAFKA_ERRORS_TOTAL.inc()
            cid = event.get("correlation_id", "unknown")
            logger.error(
                "kafka_produce_failed",
                correlation_id=cid,
                error=str(exc),
            )
            self._route_to_dlq(event, key, exc)

    def _route_to_dlq(
        self,
        original_event: dict[str, Any],
        key: str,
        error: Exception,
        *,
        retry_count: int = 0,
    ) -> None:
        """Send the failed event to the DLQ topic."""
        cid = original_event.get("correlation_id", str(uuid4()))
        now = _now_utc()
        dlq_event: dict[str, Any] = {
            "event_id": str(uuid4()),
            "correlation_id": cid,
            "original_topic": self._raw_topic,
            "original_key": key,
            "original_value": json.dumps(original_event).encode("utf-8"),
            "error_message": str(error),
            "error_class": type(error).__qualname__,
            "stack_trace": traceback.format_exc()[:4096],
            "retry_count": retry_count,
            "max_retries": self._max_retries,
            "first_failed_at_utc": now,
            "last_failed_at_utc": now,
            "source": "reddit-ingestor",
            "schema_version": "dlq.social.reddit.v1",
        }
        try:
            self._dlq_producer.produce(
                topic=self._dlq_topic,
                key=key,
                value=dlq_event,
                on_delivery=self._on_dlq_delivery,
            )
            DLQ_TOTAL.inc()
            logger.warning(
                "event_routed_to_dlq",
                correlation_id=cid,
                original_topic=self._raw_topic,
                error_class=dlq_event["error_class"],
            )
        except Exception as dlq_exc:
            # Last-resort: DLQ itself failed — log and move on
            logger.critical(
                "dlq_produce_failed",
                correlation_id=cid,
                error=str(dlq_exc),
            )

    # ------------------------------------------------------------------
    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Any) -> None:
        if err:
            KAFKA_ERRORS_TOTAL.inc()
            logger.error("kafka_delivery_failed", error=str(err))
        else:
            logger.debug(
                "kafka_delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    @staticmethod
    def _on_dlq_delivery(err: KafkaError | None, msg: Any) -> None:
        if err:
            logger.critical("dlq_delivery_failed", error=str(err))

    # ------------------------------------------------------------------
    def poll(self, timeout: float = 0.0) -> None:
        self._raw_producer.poll(timeout)
        self._dlq_producer.poll(timeout)

    def flush(self, timeout: float = 30.0) -> None:
        self._raw_producer.flush(timeout)
        self._dlq_producer.flush(timeout)

    def close(self) -> None:
        self.flush()
        logger.info("publisher_closed")
