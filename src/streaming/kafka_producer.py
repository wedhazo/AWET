from __future__ import annotations

import logging
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer

from src.core.config import KafkaConfig

logger = logging.getLogger(__name__)


_delivery_count = 0
_error_count = 0

def _delivery_callback(err, msg):
    """Callback for message delivery confirmation."""
    global _delivery_count, _error_count
    if err:
        _error_count += 1
        print(f"DELIVERY ERROR: {err}")
    else:
        _delivery_count += 1
        if _delivery_count <= 5 or _delivery_count % 1000 == 0:
            print(f"Delivered #{_delivery_count} to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


class AvroProducer:
    def __init__(self, config: KafkaConfig) -> None:
        self._schema_registry = SchemaRegistryClient({"url": config.schema_registry_url})
        self._bootstrap_servers = ",".join(config.bootstrap_servers)
        self._producers: dict[str, SerializingProducer] = {}
        self._active_producer: SerializingProducer | None = None

    def _get_producer(self, schema_str: str) -> SerializingProducer:
        if schema_str in self._producers:
            return self._producers[schema_str]
        value_serializer = AvroSerializer(self._schema_registry, schema_str)
        producer = SerializingProducer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": value_serializer,
            }
        )
        self._producers[schema_str] = producer
        return producer

    def produce(self, topic: str, schema_str: str, value: dict[str, Any], key: str) -> None:
        producer = self._get_producer(schema_str)
        self._active_producer = producer
        producer.produce(topic=topic, key=key, value=value, on_delivery=_delivery_callback)
        # Don't poll here - let the caller control when to poll

    def poll(self, timeout: float = 0) -> int:
        """Poll all producers for delivery events."""
        total = 0
        for producer in self._producers.values():
            total += producer.poll(timeout)
        return total

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all producers and wait for delivery."""
        remaining = 0
        for producer in self._producers.values():
            remaining += producer.flush(timeout)
        return remaining

    def __len__(self) -> int:
        """Return number of messages still in queues."""
        total = 0
        for producer in self._producers.values():
            total += len(producer)
        return total
