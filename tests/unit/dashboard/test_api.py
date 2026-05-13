"""Unit tests for dashboard api.py pure functions.

Integration tests (needs Postgres) are in the same file but marked with
pytest.mark.integration and skipped unless POSTGRES_URL is set.
"""
from __future__ import annotations
import os
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal

import pytest


# ---------------------------------------------------------------------------
# Unit: _ring_upsert
# ---------------------------------------------------------------------------

def test_ring_upsert_adds_new_entry():
    from services.dashboard.api import _ring_upsert
    ring: OrderedDict = OrderedDict()
    _ring_upsert(ring, "id-1", {"event_id": "id-1", "score": None})
    assert "id-1" in ring
    assert ring["id-1"]["score"] is None


def test_ring_upsert_updates_existing_entry():
    from services.dashboard.api import _ring_upsert
    ring: OrderedDict = OrderedDict()
    _ring_upsert(ring, "id-1", {"event_id": "id-1", "score": None, "headline": "foo"})
    _ring_upsert(ring, "id-1", {"score": 7, "direction": "rates_lower"})
    assert ring["id-1"]["score"] == 7
    assert ring["id-1"]["headline"] == "foo"  # preserved
    assert ring["id-1"]["direction"] == "rates_lower"


def test_ring_upsert_evicts_oldest_when_full():
    from services.dashboard.api import _ring_upsert
    ring: OrderedDict = OrderedDict()
    for i in range(500):
        _ring_upsert(ring, f"id-{i}", {"event_id": f"id-{i}"}, max_size=500)
    assert len(ring) == 500
    _ring_upsert(ring, "id-500", {"event_id": "id-500"}, max_size=500)
    assert len(ring) == 500
    assert "id-0" not in ring  # oldest evicted
    assert "id-500" in ring


# ---------------------------------------------------------------------------
# Unit: _row_to_dict
# ---------------------------------------------------------------------------

def _make_row(
    id_="evt-1",
    source="fed_rss",
    ts_source=None,
    ts_ingested=None,
    ts_scored=None,
    ts_alerted=None,
    status="scored",
    headline="Test headline",
    body="Test body",
    url="https://example.com",
    metadata=None,
    score=7,
    direction="rates_lower",
    confidence=Decimal("0.82"),
    reasoning="Test reasoning.",
    model="claude-haiku-4-5",
):
    now = datetime.now(timezone.utc)
    return (
        id_, source,
        ts_source or now, ts_ingested or now, ts_scored or now, ts_alerted,
        status, headline, body, url, metadata or {},
        score, direction, confidence, reasoning, model,
    )


def test_row_to_dict_happy_path():
    from services.dashboard.api import _row_to_dict
    row = _make_row()
    d = _row_to_dict(row)
    assert d["event_id"] == "evt-1"
    assert d["source"] == "fed_rss"
    assert d["score"] == 7
    assert d["direction"] == "rates_lower"
    assert isinstance(d["confidence"], float)
    assert d["confidence"] == pytest.approx(0.82)
    assert d["reasoning"] == "Test reasoning."
    assert d["status"] == "scored"
    assert d["ts_alerted"] is None


def test_row_to_dict_null_score():
    from services.dashboard.api import _row_to_dict
    row = _make_row(score=None, direction=None, confidence=None, reasoning=None,
                    ts_scored=None, status="received")
    d = _row_to_dict(row)
    assert d["score"] is None
    assert d["direction"] is None
    assert d["confidence"] is None
    assert d["reasoning"] is None


def test_row_to_dict_null_body_and_url():
    from services.dashboard.api import _row_to_dict
    row = _make_row(body=None, url=None)
    d = _row_to_dict(row)
    assert d["body"] is None
    assert d["url"] is None


# ---------------------------------------------------------------------------
# Integration: REST endpoints (requires Postgres)
# ---------------------------------------------------------------------------

integration = pytest.mark.skipif(
    not os.environ.get("POSTGRES_URL"),
    reason="requires POSTGRES_URL"
)


def _seed_event(event_id: str, status: str = "scored", score: int = 7,
                source: str = "fed_rss") -> None:
    from services.shared.db import connect
    now = datetime.now(timezone.utc)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, ts_ingested, ts_scored, status,
                   headline, body, url, metadata, score, direction,
                   confidence, reasoning, model)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event_id, source, now, now, now, status,
                    "Test headline", "Test body", "https://example.com",
                    "{}", score, "rates_lower", 0.72, "Test reasoning.",
                    "claude-haiku-4-5",
                ),
            )
        conn.commit()


@integration
def test_api_events_returns_rows_since(monkeypatch):
    import uuid
    from datetime import timedelta
    monkeypatch.setenv("DASHBOARD_KAFKA_ENABLED", "0")
    from fastapi.testclient import TestClient
    from services.dashboard.api import app

    event_id = f"dash-test-{uuid.uuid4().hex[:8]}"
    _seed_event(event_id)

    since = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    with TestClient(app) as client:
        resp = client.get(f"/api/events?since={since}")
    assert resp.status_code == 200
    ids = [r["event_id"] for r in resp.json()]
    assert event_id in ids


@integration
def test_api_events_filters_by_until(monkeypatch):
    import uuid
    from datetime import timedelta
    monkeypatch.setenv("DASHBOARD_KAFKA_ENABLED", "0")
    from fastapi.testclient import TestClient
    from services.dashboard.api import app

    event_id = f"dash-until-{uuid.uuid4().hex[:8]}"
    _seed_event(event_id)

    # until = 10 minutes ago — event seeded just now should NOT appear
    until = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    with TestClient(app) as client:
        resp = client.get(f"/api/events?since={since}&until={until}")
    assert resp.status_code == 200
    ids = [r["event_id"] for r in resp.json()]
    assert event_id not in ids


@integration
def test_api_status_returns_source_health(monkeypatch):
    import uuid
    monkeypatch.setenv("DASHBOARD_KAFKA_ENABLED", "0")
    from fastapi.testclient import TestClient
    from services.dashboard.api import app

    _seed_event(f"dash-status-{uuid.uuid4().hex[:8]}", source="fed_rss")

    with TestClient(app) as client:
        resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "sources" in data
    assert "dlq_today" in data
    sources = {s["source"] for s in data["sources"]}
    assert "fed_rss" in sources
