"""Kafka producer/consumer factories used by every service.

The same module is used both inside Compose containers (KAFKA_BROKERS=kafka:9092)
and from the host during testing (KAFKA_BROKERS=localhost:9094).
"""
import json
import os
import uuid
from typing import Any
from confluent_kafka import Producer, Consumer


def _brokers() -> str:
    return os.environ.get("KAFKA_BROKERS", "localhost:9094")


def make_producer() -> Producer:
    """Idempotent JSON producer with snappy compression."""
    return Producer({
        "bootstrap.servers": _brokers(),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 10,
    })


def make_consumer(
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
) -> Consumer:
    """Standard worker consumer: manual offset commit, replay-friendly."""
    consumer = Consumer({
        "bootstrap.servers": _brokers(),
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": auto_offset_reset,
        "session.timeout.ms": 30000,
        "isolation.level": "read_committed",
    })
    consumer.subscribe(topics)
    return consumer


def make_unique_consumer(topics: list[str]) -> Consumer:
    """Per-instance consumer group → fanout (every message to every instance).

    Used by the dashboard service so multiple dashboard processes each see all
    events. `auto.offset.reset='latest'` means a fresh dashboard only gets new
    messages — historical state comes from Postgres on warm-up instead.
    """
    return make_consumer(
        f"viewer-{uuid.uuid4()}",
        topics,
        auto_offset_reset="latest",
    )


def produce(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    """Produce a JSON-serialized payload with a string key."""
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )


def flush(producer: Producer, timeout: float = 5.0) -> None:
    """Block until in-flight messages are delivered (or timeout)."""
    producer.flush(timeout)
