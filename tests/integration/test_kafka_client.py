"""Integration tests for Kafka producer/consumer factories.
Requires `docker compose up -d kafka`. Connects from host via localhost:9094 (EXTERNAL listener)."""
import json
import time
import uuid
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from services.shared.kafka_client import (
    make_producer,
    make_consumer,
    produce,
    flush,
)


BROKERS_HOST = "localhost:9094"


@pytest.fixture
def kafka_brokers(env):
    env(KAFKA_BROKERS=BROKERS_HOST)


@pytest.fixture
def temp_topic(kafka_brokers):
    topic = f"test.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BROKERS_HOST})
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for _, f in fs.items():
        f.result(timeout=10)
    yield topic
    fs = admin.delete_topics([topic])
    for _, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception:
            pass


def _consume_one(consumer, deadline_seconds=10):
    deadline = time.time() + deadline_seconds
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        return msg
    return None


def test_produce_consume_round_trip(temp_topic):
    producer = make_producer()
    payload = {"event_id": "abc", "headline": "test"}
    produce(producer, temp_topic, key="abc", payload=payload)
    flush(producer)

    consumer = make_consumer(f"test-cg-{uuid.uuid4()}", [temp_topic])
    try:
        msg = _consume_one(consumer)
        assert msg is not None, "did not receive message within deadline"
        received = json.loads(msg.value().decode())
        assert received == payload
    finally:
        consumer.close()


def test_two_consumer_groups_each_see_all_messages(temp_topic):
    """Two distinct consumer groups should each receive all produced messages
    (the 'fanout' pattern that make_unique_consumer relies on for the dashboard)."""
    producer = make_producer()
    produce(producer, temp_topic, key="a", payload={"id": "a"})
    produce(producer, temp_topic, key="b", payload={"id": "b"})
    flush(producer)

    c1 = make_consumer(f"viewer-1-{uuid.uuid4()}", [temp_topic])
    c2 = make_consumer(f"viewer-2-{uuid.uuid4()}", [temp_topic])

    def collect(consumer, n=2, deadline_s=10):
        seen = set()
        deadline = time.time() + deadline_s
        while len(seen) < n and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                seen.add(json.loads(msg.value().decode())["id"])
        return seen

    try:
        assert collect(c1) == {"a", "b"}
        assert collect(c2) == {"a", "b"}
    finally:
        c1.close()
        c2.close()
