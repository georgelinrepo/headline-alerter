"""Phase 1a end-to-end integration test.

Real Kafka + real Postgres, in-process scorer. Anthropic is faked via the
SCORER_FAKE_RESPONSE_PATH env var that the scorer's build_anthropic_client
reads when constructing its client.

Requires: docker compose up -d (kafka + postgres + migrate done).
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Consumer

from services.scorer.main import (
    build_anthropic_client, process_one_event,
)
from services.shared.db import connect
from services.shared.kafka_client import flush, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.models import NormalizedEvent

FIXTURE_PATH = str(
    (Path(__file__).parents[1] / "fixtures" / "anthropic_score_response.json").resolve()
)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9094")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://rates:changeme@localhost:5432/rates")
    monkeypatch.setenv("SCORER_MODEL", "claude-haiku-4-5")
    monkeypatch.setenv("SCORER_FAKE_RESPONSE_PATH", FIXTURE_PATH)
    yield


def _seed_event_in_archive(event: NormalizedEvent):
    """Pretend the ingestor wrote a 'received' row, so the scorer's UPDATE has a target."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, ts_ingested, status, headline, body, url, metadata)
                VALUES (%s, %s, %s, %s, 'received', %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event.event_id, event.source, event.ts_source, event.ts_ingested,
                    event.headline, event.body, event.url, "{}",
                ),
            )
        conn.commit()


def _fetch_archive_row(event_id):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, score, direction, confidence FROM events_archive WHERE id = %s",
                        (event_id,))
            return cur.fetchone()


# ---- success path --------------------------------------------------------

def test_success_path_writes_score_and_produces(env):
    configure_logging("itest")
    log = get_logger()
    ev = NormalizedEvent(
        event_id=f"itest-success-{uuid.uuid4().hex[:8]}",
        source="cnbc_rss",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="Powell signals dovish pivot",
        body="Body text.",
        url="https://example.com/x",
        metadata={},
    )
    _seed_event_in_archive(ev)

    client = build_anthropic_client()
    producer = make_producer()
    process_one_event(
        ev.to_dict(), anthropic_client=client, producer=producer,
        log=log, model="claude-haiku-4-5",
    )

    # Postgres updated
    row = _fetch_archive_row(ev.event_id)
    assert row is not None
    status, score, direction, confidence = row
    assert status == "scored"
    assert score == 7
    assert direction == "rates_lower"
    assert float(confidence) == pytest.approx(0.72)


# ---- DLQ path ------------------------------------------------------------

def test_dlq_path_routes_failed_event(env, monkeypatch):
    monkeypatch.setenv("SCORER_FAKE_FAIL_MODE", "rate_limit")
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    configure_logging("itest")
    log = get_logger()
    ev = NormalizedEvent(
        event_id=f"itest-dlq-{uuid.uuid4().hex[:8]}",
        source="cnbc_rss",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="Some unimportant tweet",
        body="b",
        url="https://example.com/y",
        metadata={},
    )
    _seed_event_in_archive(ev)

    client = build_anthropic_client()
    producer = make_producer()
    process_one_event(
        ev.to_dict(), anthropic_client=client, producer=producer,
        log=log, model="claude-haiku-4-5",
    )

    # Archive marked failed
    row = _fetch_archive_row(ev.event_id)
    assert row is not None and row[0] == "failed"

    # DLQ message present — search for ours specifically.
    consumer = Consumer({
        "bootstrap.servers": "localhost:9094",
        "group.id": f"itest-dlq-{uuid.uuid4()}",
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
            if p.get("original_event", {}).get("event_id") == ev.event_id:
                found = p
                break
    consumer.close()

    assert found is not None
    assert found["stage"] == "scorer_throttle"
    assert found["service"] == "scorer"
