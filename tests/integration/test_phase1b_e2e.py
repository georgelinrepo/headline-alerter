"""Phase 1b end-to-end integration test.

Real Kafka + real Postgres, in-process alerter. Twilio is faked via
TWILIO_FAKE=1 (set per-test).

Requires: docker compose up -d (kafka + postgres + migrate done).
"""
import json
import time
import uuid
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer

from services.alerter.main import process_one_alert
from services.alerter.twilio_client import build_client
from services.shared.db import connect
from services.shared.kafka_client import make_producer
from services.shared.logging import configure_logging, get_logger


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9094")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://rates:changeme@localhost:5432/rates")
    monkeypatch.setenv("TWILIO_FAKE", "1")
    yield


def _seed_received_event(event_id: str, headline: str = "test alert"):
    """Seed an events_archive row in 'scored' status that the alerter can find."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, ts_ingested, ts_scored, status,
                   headline, body, url, metadata,
                   score, direction, confidence, reasoning, model)
                VALUES (%s, %s, %s, %s, %s, 'scored',
                        %s, %s, %s, %s::jsonb,
                        %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event_id, "cnbc_rss",
                    datetime.now(timezone.utc), datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    headline, "body", "https://example.com/x", "{}",
                    7, "rates_lower", 0.72, "test reasoning", "claude-haiku-4-5",
                ),
            )
        conn.commit()


def _scored_dict(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "score": 7,
        "direction": "rates_lower",
        "confidence": 0.72,
        "reasoning": "test reasoning",
        "model": "claude-haiku-4-5",
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }


def _archive_status(event_id: str) -> str | None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM events_archive WHERE id = %s", (event_id,))
            row = cur.fetchone()
            return row[0] if row else None


def _alert_history_count(event_id: str) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM alert_history WHERE event_id = %s", (event_id,))
            return cur.fetchone()[0]


# ---- success path --------------------------------------------------------

def test_success_writes_alert_history_and_flips_archive(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-success-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id, headline="Powell dovish pivot")

    process_one_alert(
        _scored_dict(event_id),
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    assert _archive_status(event_id) == "alerted"
    assert _alert_history_count(event_id) == 1


# ---- idempotency --------------------------------------------------------

def test_idempotency_second_call_is_no_op(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-idem-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)
    kwargs = dict(
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    # First call: alerts.
    process_one_alert(_scored_dict(event_id), **kwargs)
    first_count = _alert_history_count(event_id)
    assert first_count == 1

    # Second call: no-op (idempotency check kicks in).
    process_one_alert(_scored_dict(event_id), **kwargs)
    second_count = _alert_history_count(event_id)
    assert second_count == 1, "alert_history should not have grown on the second call"


# ---- below threshold -----------------------------------------------------

def test_below_threshold_writes_nothing(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-low-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)

    low = _scored_dict(event_id) | {"score": 2}
    process_one_alert(
        low,
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    assert _archive_status(event_id) == "scored"  # not flipped
    assert _alert_history_count(event_id) == 0


# ---- DLQ on throttle -----------------------------------------------------

def test_throttle_failure_routes_to_dlq(env, monkeypatch):
    monkeypatch.setenv("TWILIO_FAIL_MODE", "throttle")
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-dlq-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)

    process_one_alert(
        _scored_dict(event_id),
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    # Archive NOT flipped, alert_history empty.
    assert _archive_status(event_id) == "scored"
    assert _alert_history_count(event_id) == 0

    # DLQ row present with stage='alerter_throttle'.
    consumer = Consumer({
        "bootstrap.servers": "localhost:9094",
        "group.id": f"itest-alerter-dlq-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["events.dlq"])
    found = None
    deadline = time.time() + 15
    while time.time() < deadline and not found:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            p = json.loads(msg.value().decode("utf-8"))
            if p.get("original_event", {}).get("event_id") == event_id:
                found = p
                break
    consumer.close()

    assert found is not None
    assert found["stage"] == "alerter_throttle"
    assert found["service"] == "alerter"
