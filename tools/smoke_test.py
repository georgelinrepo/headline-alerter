"""Phase 0 smoke test.

End-to-end: produce a synthetic event to events.normalized, consume it back,
write a row to events_archive, read it back. Verifies all of:
- Kafka producer + consumer factory
- Postgres connection
- Schema (events_archive present)
- Models serialization

Run from host: `python tools/smoke_test.py`
Requires: `docker compose up -d` (kafka + postgres + topics + schema applied).
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.shared.kafka_client import (
    make_producer, make_consumer, produce, flush,
)
from services.shared.db import connect
from services.shared.models import NormalizedEvent
from services.shared.logging import configure_logging, get_logger


def _ensure_env():
    os.environ.setdefault("KAFKA_BROKERS", "localhost:9094")
    os.environ.setdefault(
        "POSTGRES_URL",
        "postgresql://rates:changeme@localhost:5432/rates",
    )


def main() -> int:
    _ensure_env()
    configure_logging("smoke")
    log = get_logger()

    event = NormalizedEvent(
        event_id=f"smoke-{uuid.uuid4().hex[:12]}",
        source="smoke",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="smoke test event",
        body="hello from smoke test",
        url=None,
        metadata={"phase": 0},
    )

    log.info("producing", event_id=event.event_id)
    producer = make_producer()
    produce(producer, "events.normalized", key=event.source, payload=event.to_dict())
    flush(producer)
    log.info("produced")

    log.info("consuming")
    consumer = make_consumer(f"smoke-cg-{uuid.uuid4()}", ["events.normalized"])
    deadline = time.time() + 15
    received = None
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value().decode())
            if payload.get("event_id") == event.event_id:
                received = payload
                break
    finally:
        consumer.close()
    assert received is not None, "did not receive our event back from Kafka"
    log.info("consumed", event_id=received["event_id"])

    log.info("writing to events_archive")
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, status, headline, body, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    received["event_id"],
                    received["source"],
                    received["ts_source"],
                    "received",
                    received["headline"],
                    received["body"],
                    json.dumps(received.get("metadata") or {}),
                ),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, source, status FROM events_archive WHERE id = %s",
                (event.event_id,),
            )
            row = cur.fetchone()
    assert row is not None, "row not found in events_archive"
    log.info("verified", id=row[0], source=row[1], status=row[2])

    print("OK — Phase 0 smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
