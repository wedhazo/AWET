from __future__ import annotations

from typing import Any, Callable

from confluent_kafka import KafkaError
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.deserializing_consumer import DeserializingConsumer

from src.core.config import KafkaConfig


class AvroConsumer:
    def __init__(self, config: KafkaConfig, group_id: str, schema_str: str, topic: str) -> None:
        self._schema_registry = SchemaRegistryClient({"url": config.schema_registry_url})
        self._consumer = DeserializingConsumer(
            {
                "bootstrap.servers": ",".join(config.bootstrap_servers),
                "group.id": group_id,
                "auto.offset.reset": config.auto_offset_reset,
                "enable.auto.commit": False,
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": AvroDeserializer(self._schema_registry, schema_str),
            }
        )
        self._consumer.subscribe([topic])
        self._topic = topic

    def poll(self, timeout: float = 1.0) -> Any | None:
        try:
            msg = self._consumer.poll(timeout)
        except ConsumeError as e:
            # Handle topic not ready errors gracefully
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return None
            raise
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return None
            raise RuntimeError(msg.error())
        return msg

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
