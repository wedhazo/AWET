"""Dead-Letter Queue publisher for Kafka poison pills.

Best-effort, fire-and-forget. **Never** raises — a DLQ failure must not
interrupt the consumer loop.  Messages are serialized as plain JSON
(``StringSerializer``) so any malformed event can be captured regardless
of its original schema.

Usage::

    from src.streaming.dlq import DLQPublisher
    dlq = DLQPublisher(bootstrap_servers="localhost:9092")
    dlq.send(topic="market.raw", error="ValueDeserializationError ...", raw=b"...")
    dlq.flush()
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any

import structlog

from src.streaming.topics import DLQ_GENERIC

log = structlog.get_logger("dlq")

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover – optional dependency
    Producer = None  # type: ignore[assignment,misc]


class DLQPublisher:
    """Lazy-init, best-effort DLQ producer."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        dlq_topic: str = DLQ_GENERIC,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._dlq_topic = dlq_topic
        self._producer: Any | None = None  # lazy

    # -- lazy init so import alone is side-effect-free -----------------------
    def _ensure_producer(self) -> Any | None:
        if self._producer is not None:
            return self._producer
        if Producer is None:
            log.warning("dlq_producer_unavailable", reason="confluent_kafka not installed")
            return None
        try:
            self._producer = Producer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "linger.ms": 100,
                    "batch.num.messages": 64,
                    "compression.type": "lz4",
                }
            )
            log.info("dlq_producer_created", topic=self._dlq_topic)
        except Exception:
            log.exception("dlq_producer_init_failed")
            self._producer = None
        return self._producer

    # -- public API -----------------------------------------------------------
    def send(
        self,
        *,
        topic: str,
        error: str,
        raw: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> bool:
        """Publish a dead-letter record.  Returns *True* on success."""
        producer = self._ensure_producer()
        if producer is None:
            return False

        envelope: dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "original_topic": topic,
            "error_message": error,
            "error_ts": time.time(),
            "raw_base64": raw.hex() if raw else None,
        }
        if headers:
            envelope["original_headers"] = headers

        try:
            producer.produce(
                self._dlq_topic,
                value=json.dumps(envelope).encode(),
                key=topic.encode(),
            )
            log.info("dlq_message_sent", original_topic=topic)
            return True
        except Exception:
            log.exception("dlq_produce_failed", original_topic=topic)
            return False

    def flush(self, timeout: float = 5.0) -> None:
        if self._producer is not None:
            self._producer.flush(timeout)
