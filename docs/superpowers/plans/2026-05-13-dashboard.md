# Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `services/dashboard/` service — a FastAPI backend with Kafka consumers + SSE fan-out and a Tabulator.js frontend with a split table/detail-panel layout, per-column filtering, and live/historical date mode.

**Architecture:** FastAPI serves `static/index.html` and three API endpoints. Two background threads poll `events.normalized` and `events.scored` Kafka topics and broadcast parsed messages to per-connection `asyncio.Queue` objects; each SSE client gets its own queue. The frontend is a single HTML file using Tabulator.js (CDN) with no build step.

**Tech Stack:** FastAPI, sse-starlette, uvicorn, confluent-kafka (sync, threaded), psycopg3 (sync), Tabulator.js 6.3.0 (CDN), vanilla JS EventSource

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `services/dashboard/__init__.py` | Create | Empty package marker |
| `services/dashboard/api.py` | Create | FastAPI app: ring buffer, Kafka threads, SSE fan-out, REST endpoints |
| `services/dashboard/static/index.html` | Create | Full frontend: Tabulator, SSE, detail panel, date picker |
| `services/dashboard/Dockerfile` | Create | Container image |
| `tests/unit/dashboard/__init__.py` | Create | Empty package marker |
| `tests/unit/dashboard/test_api.py` | Create | Unit tests for pure functions; integration tests for REST endpoints |
| `docker-compose.yml` | Modify | Add `dashboard` service |

---

## Task 1: Backend foundation — ring buffer, REST endpoints, tests

**Files:**
- Create: `services/dashboard/__init__.py`
- Create: `services/dashboard/api.py`
- Create: `tests/unit/dashboard/__init__.py`
- Create: `tests/unit/dashboard/test_api.py`

- [ ] **Step 1.1: Create package markers**

```bash
touch services/dashboard/__init__.py tests/unit/dashboard/__init__.py
```

- [ ] **Step 1.2: Write failing unit tests for `_ring_upsert` and `_row_to_dict`**

Create `tests/unit/dashboard/test_api.py`:

```python
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
```

- [ ] **Step 1.3: Run tests — confirm they fail with ImportError**

```bash
pytest tests/unit/dashboard/test_api.py -v 2>&1 | head -30
```

Expected: `ImportError: cannot import name '_ring_upsert' from 'services.dashboard.api'`

- [ ] **Step 1.4: Implement `services/dashboard/api.py`**

Create `services/dashboard/api.py`:

```python
"""Dashboard API: ring buffer, Kafka consumers, SSE fan-out, REST endpoints."""
from __future__ import annotations

import asyncio
import json
import os
import threading
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

from services.shared.db import connect
from services.shared.kafka_client import make_unique_consumer
from services.shared.logging import configure_logging, get_logger

app = FastAPI()

# ---------------------------------------------------------------------------
# Ring buffer
# ---------------------------------------------------------------------------

_ring: OrderedDict[str, dict] = OrderedDict()
_RING_MAX = 500


def _ring_upsert(ring: OrderedDict, event_id: str, data: dict,
                 max_size: int = _RING_MAX) -> None:
    """Insert or merge data into ring, evicting oldest entry when full."""
    if event_id in ring:
        ring[event_id].update(data)
        ring.move_to_end(event_id)
    else:
        ring[event_id] = data
        if len(ring) > max_size:
            ring.popitem(last=False)


# ---------------------------------------------------------------------------
# Row serialization
# ---------------------------------------------------------------------------

def _row_to_dict(row: tuple) -> dict[str, Any]:
    """Convert a Postgres events_archive row tuple to a JSON-serializable dict.

    Column order must match the SELECT in _warm_ring_buffer and /api/events:
    id, source, ts_source, ts_ingested, ts_scored, ts_alerted, status,
    headline, body, url, metadata, score, direction, confidence, reasoning, model
    """
    (id_, source, ts_source, ts_ingested, ts_scored, ts_alerted, status,
     headline, body, url, metadata, score, direction, confidence, reasoning,
     model) = row
    return {
        "event_id": id_,
        "source": source,
        "ts_source": ts_source.isoformat() if ts_source else None,
        "ts_ingested": ts_ingested.isoformat() if ts_ingested else None,
        "ts_scored": ts_scored.isoformat() if ts_scored else None,
        "ts_alerted": ts_alerted.isoformat() if ts_alerted else None,
        "status": status,
        "headline": headline,
        "body": body,
        "url": url,
        "metadata": metadata or {},
        "score": score,
        "direction": direction,
        "confidence": float(confidence) if isinstance(confidence, Decimal) else confidence,
        "reasoning": reasoning,
        "model": model,
    }


_EVENTS_SELECT = """
    SELECT id, source, ts_source, ts_ingested, ts_scored, ts_alerted, status,
           headline, body, url, metadata,
           score, direction, confidence, reasoning, model
    FROM events_archive
"""


# ---------------------------------------------------------------------------
# SSE fan-out
# ---------------------------------------------------------------------------

_subscribers: set[asyncio.Queue] = set()
_loop: asyncio.AbstractEventLoop | None = None
_log = None


def _broadcast(msg: dict) -> None:
    """Push a message to every connected SSE client (called from Kafka thread)."""
    if _loop is None:
        return
    for q in list(_subscribers):
        asyncio.run_coroutine_threadsafe(q.put(msg), _loop)


# ---------------------------------------------------------------------------
# Kafka consumers (background thread)
# ---------------------------------------------------------------------------

def _kafka_thread() -> None:
    log = get_logger()
    consumer_n = make_unique_consumer(["events.normalized"])
    consumer_s = make_unique_consumer(["events.scored"])
    try:
        while True:
            for consumer, msg_type in [
                (consumer_n, "event"),
                (consumer_s, "score"),
            ]:
                msg = consumer.poll(0.05)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    _ring_upsert(_ring, payload["event_id"], payload)
                    _broadcast({"type": msg_type, "data": payload})
                except Exception as exc:
                    log.warning("dashboard kafka decode error", error=str(exc))
    finally:
        consumer_n.close()
        consumer_s.close()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def _warm_ring_buffer() -> None:
    """Load today's events from Postgres into the ring buffer at startup."""
    today_midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _EVENTS_SELECT + " WHERE ts_ingested >= %s ORDER BY ts_ingested ASC",
                (today_midnight,),
            )
            for row in cur.fetchall():
                d = _row_to_dict(row)
                _ring_upsert(_ring, d["event_id"], d)


@app.on_event("startup")
async def startup() -> None:
    global _loop, _log
    configure_logging("dashboard")
    _log = get_logger()
    _loop = asyncio.get_event_loop()
    _warm_ring_buffer()
    if os.environ.get("DASHBOARD_KAFKA_ENABLED", "1") != "0":
        t = threading.Thread(target=_kafka_thread, daemon=True)
        t.start()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

_STATIC = Path(__file__).parent / "static" / "index.html"


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC)


@app.get("/api/events")
async def events(since: str, until: str | None = None) -> JSONResponse:
    since_dt = datetime.fromisoformat(since)
    where = ["ts_ingested >= %s"]
    params: list[Any] = [since_dt]
    if until:
        until_dt = datetime.fromisoformat(until)
        where.append("ts_ingested <= %s")
        params.append(until_dt)
    sql = _EVENTS_SELECT + f" WHERE {' AND '.join(where)} ORDER BY ts_ingested DESC"
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return JSONResponse([_row_to_dict(r) for r in rows])


@app.get("/api/stream")
async def stream(request: Request) -> EventSourceResponse:
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.add(q)

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {"event": msg["type"], "data": json.dumps(msg["data"])}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            _subscribers.discard(q)

    return EventSourceResponse(generator())


@app.get("/api/status")
async def status() -> JSONResponse:
    today_midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source,
                       MAX(ts_ingested) AS last_seen,
                       COUNT(*) FILTER (WHERE ts_ingested >= NOW() - INTERVAL '5 minutes')
                           AS recent_count
                FROM events_archive
                GROUP BY source
                ORDER BY source
            """)
            sources = [
                {
                    "source": r[0],
                    "last_seen": r[1].isoformat() if r[1] else None,
                    "live": r[2] > 0,
                }
                for r in cur.fetchall()
            ]
            cur.execute(
                "SELECT COUNT(*) FROM events_archive WHERE status = 'failed' AND ts_ingested >= %s",
                (today_midnight,),
            )
            dlq_today = cur.fetchone()[0]
    return JSONResponse({"sources": sources, "dlq_today": dlq_today})
```

- [ ] **Step 1.5: Run unit tests — confirm pure-function tests pass**

```bash
pytest tests/unit/dashboard/test_api.py::test_ring_upsert_adds_new_entry \
       tests/unit/dashboard/test_api.py::test_ring_upsert_updates_existing_entry \
       tests/unit/dashboard/test_api.py::test_ring_upsert_evicts_oldest_when_full \
       tests/unit/dashboard/test_api.py::test_row_to_dict_happy_path \
       tests/unit/dashboard/test_api.py::test_row_to_dict_null_score \
       tests/unit/dashboard/test_api.py::test_row_to_dict_null_body_and_url \
       -v
```

Expected: all 6 PASS

- [ ] **Step 1.6: Run integration tests (requires `docker compose up -d`)**

```bash
POSTGRES_URL="postgresql://rates:changeme@localhost:5432/rates" \
KAFKA_BROKERS="localhost:9094" \
DASHBOARD_KAFKA_ENABLED=0 \
pytest tests/unit/dashboard/test_api.py -v -k "integration or api_events or api_status"
```

Expected: `test_api_events_returns_rows_since` PASS, `test_api_events_filters_by_until` PASS, `test_api_status_returns_source_health` PASS

- [ ] **Step 1.7: Commit**

```bash
git add services/dashboard/__init__.py services/dashboard/api.py \
        tests/unit/dashboard/__init__.py tests/unit/dashboard/test_api.py
git commit -m "feat(dashboard): add api.py with ring buffer, REST endpoints, unit+integration tests"
```

---

## Task 2: Frontend — `static/index.html`

**Files:**
- Create: `services/dashboard/static/index.html`

- [ ] **Step 2.1: Create the static directory**

```bash
mkdir -p services/dashboard/static
```

- [ ] **Step 2.2: Create `services/dashboard/static/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Headline Alerter</title>
  <link rel="stylesheet" href="https://unpkg.com/tabulator-tables@6.3.0/dist/css/tabulator_midnight.min.css">
  <script src="https://unpkg.com/tabulator-tables@6.3.0/dist/js/tabulator.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #111827;
      color: #e5e7eb;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 13px;
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    /* ---- Header ---- */
    #header {
      background: #0d1117;
      border-bottom: 1px solid #1f2937;
      padding: 8px 16px;
      display: flex;
      align-items: center;
      gap: 16px;
      flex-shrink: 0;
    }
    #header h1 {
      font-size: 13px;
      font-weight: 700;
      letter-spacing: .08em;
      color: #f9fafb;
    }
    #date-picker {
      background: #1f2937;
      border: 1px solid #374151;
      color: #e5e7eb;
      padding: 3px 8px;
      border-radius: 4px;
      font-size: 12px;
    }
    #mode-badge {
      font-size: 11px;
      font-weight: 600;
      padding: 2px 10px;
      border-radius: 12px;
    }
    #mode-badge.live  { background: #022c22; color: #10b981; }
    #mode-badge.hist  { background: #1f2937; color: #9ca3af; }

    /* ---- Body split ---- */
    #body {
      display: flex;
      flex: 1;
      overflow: hidden;
    }

    /* ---- Left panel ---- */
    #left {
      display: flex;
      flex-direction: column;
      flex: 0 0 56%;
      border-right: 1px solid #1f2937;
      overflow: hidden;
    }
    #table-wrap { flex: 1; overflow: hidden; }

    /* ---- Tabulator overrides ---- */
    .tabulator {
      background: #111827 !important;
      border: none !important;
      height: 100% !important;
    }
    .tabulator .tabulator-header {
      background: #0d1117 !important;
      border-bottom: 1px solid #1f2937 !important;
    }
    .tabulator .tabulator-header .tabulator-col {
      background: #0d1117 !important;
      border-right: 1px solid #1f2937 !important;
      color: #6b7280 !important;
      font-size: 10px !important;
      font-weight: 600 !important;
      text-transform: uppercase;
      letter-spacing: .05em;
    }
    .tabulator .tabulator-header .tabulator-col input,
    .tabulator .tabulator-header .tabulator-col select {
      background: #1f2937 !important;
      border: 1px solid #374151 !important;
      color: #e5e7eb !important;
      font-size: 11px !important;
      padding: 2px 4px !important;
      border-radius: 3px !important;
      width: 100%;
    }
    .tabulator-row {
      background: #111827 !important;
      border-bottom: 1px solid #1a2332 !important;
      cursor: pointer;
    }
    .tabulator-row:hover { background: #1f2937 !important; }
    .tabulator-row.tabulator-selected { background: #1e3a5f !important; }
    .tabulator-cell { color: #9ca3af !important; font-size: 12px !important; }
    .tabulator-row.row-dim { opacity: .4; }

    /* ---- Status bar ---- */
    #status-bar {
      background: #0d1117;
      border-top: 1px solid #1f2937;
      padding: 5px 14px;
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
      flex-wrap: wrap;
    }
    .src-pill {
      font-size: 9px;
      padding: 2px 8px;
      border-radius: 10px;
    }
    .src-pill.live { background: #022c22; color: #10b981; }
    .src-pill.stale { background: #1f2937; color: #4b5563; }
    #dlq-badge {
      margin-left: auto;
      font-size: 9px;
      color: #6b7280;
    }

    /* ---- Right panel ---- */
    #right {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    #detail-empty {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #374151;
      font-size: 13px;
    }
    #detail-content {
      display: none;
      flex-direction: column;
      flex: 1;
      overflow: hidden;
    }

    /* Detail: score strip */
    #detail-score-strip {
      background: #0d1117;
      border-bottom: 1px solid #1f2937;
      padding: 12px 16px;
      flex-shrink: 0;
    }
    #detail-score-row {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 6px;
    }
    #detail-score-badge {
      font-size: 18px;
      font-weight: 800;
      padding: 2px 10px;
      border-radius: 4px;
      color: #fff;
    }
    #detail-direction { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; }
    #detail-meta { font-size: 10px; color: #6b7280; }
    #detail-source-link { margin-left: auto; font-size: 11px; color: #3b82f6; text-decoration: none; }
    #detail-headline { font-size: 13px; font-weight: 500; color: #e5e7eb; line-height: 1.5; }

    /* Detail: sections */
    .detail-section { padding: 10px 16px; border-bottom: 1px solid #1f2937; flex-shrink: 0; }
    .detail-section-label {
      font-size: 9px;
      text-transform: uppercase;
      letter-spacing: .08em;
      color: #4b5563;
      margin-bottom: 6px;
    }
    .detail-section-body { font-size: 12px; color: #9ca3af; line-height: 1.8; }

    #detail-body-section { flex: 1; overflow-y: auto; padding: 10px 16px; }
    #detail-body-label { font-size: 9px; text-transform: uppercase; letter-spacing: .08em; color: #4b5563; margin-bottom: 6px; }
    #detail-body-text { font-size: 12px; color: #6b7280; line-height: 1.8; }
  </style>
</head>
<body>

<!-- Header -->
<div id="header">
  <h1>HEADLINE ALERTER</h1>
  <input type="date" id="date-picker">
  <span id="mode-badge" class="live">● LIVE</span>
</div>

<!-- Body -->
<div id="body">

  <!-- Left: table + status bar -->
  <div id="left">
    <div id="table-wrap">
      <div id="events-table"></div>
    </div>
    <div id="status-bar">
      <span id="sources-container"></span>
      <span id="dlq-badge"></span>
    </div>
  </div>

  <!-- Right: detail panel -->
  <div id="right">
    <div id="detail-empty">Select a row to view details</div>
    <div id="detail-content">
      <div id="detail-score-strip">
        <div id="detail-score-row">
          <span id="detail-score-badge">—</span>
          <div>
            <div id="detail-direction">—</div>
            <div id="detail-meta"></div>
          </div>
          <a id="detail-source-link" href="#" target="_blank" rel="noopener">↗ source</a>
        </div>
        <div id="detail-headline"></div>
      </div>
      <div class="detail-section" id="detail-reasoning-section">
        <div class="detail-section-label">AI Reasoning</div>
        <div class="detail-section-body" id="detail-reasoning-text"></div>
      </div>
      <div id="detail-body-section">
        <div id="detail-body-label" class="detail-section-label">Full Content</div>
        <div id="detail-body-text"></div>
      </div>
    </div>
  </div>

</div>

<script>
// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toISOString().slice(11, 16) + "Z";
}

function scoreColor(v) {
  if (v === null || v === undefined) return { bg: "#374151", text: "#6b7280" };
  if (v >= 8) return { bg: "#dc2626", text: "#fff" };
  if (v >= 5) return { bg: "#d97706", text: "#fff" };
  return { bg: "#374151", text: "#6b7280" };
}

function directionLabel(v) {
  const map = {
    rates_higher: { label: "↑ rates higher", color: "#f87171" },
    rates_lower:  { label: "↓ rates lower",  color: "#60a5fa" },
    neutral:      { label: "— neutral",       color: "#9ca3af" },
    unclear:      { label: "? unclear",       color: "#6b7280" },
  };
  return map[v] || { label: v || "—", color: "#6b7280" };
}

// ---------------------------------------------------------------------------
// Tabulator
// ---------------------------------------------------------------------------

const table = new Tabulator("#events-table", {
  data: [],
  index: "event_id",
  layout: "fitColumns",
  height: "100%",
  placeholder: "No events — is the pipeline running?",
  initialSort: [{ column: "ts_ingested", dir: "desc" }],
  columns: [
    {
      title: "Time",
      field: "ts_ingested",
      width: 68,
      headerFilter: "input",
      sorter: "string",
      formatter: (cell) => fmtTime(cell.getValue()),
    },
    {
      title: "Source",
      field: "source",
      width: 90,
      headerFilter: "input",
    },
    {
      title: "Score",
      field: "score",
      width: 62,
      hozAlign: "center",
      headerFilter: "number",
      headerFilterFunc: ">=",
      formatter: (cell) => {
        const v = cell.getValue();
        if (v === null || v === undefined) return '<span style="color:#4b5563">—</span>';
        const c = scoreColor(v);
        return `<span style="background:${c.bg};color:${c.text};padding:1px 7px;border-radius:4px;font-weight:700;font-size:11px">${v}</span>`;
      },
    },
    {
      title: "Direction",
      field: "direction",
      width: 130,
      headerFilter: "select",
      headerFilterParams: {
        values: ["", "rates_higher", "rates_lower", "neutral", "unclear"],
        clearable: true,
      },
      formatter: (cell) => {
        const v = cell.getValue();
        if (!v) return '<span style="color:#4b5563">—</span>';
        const d = directionLabel(v);
        return `<span style="color:${d.color}">${d.label}</span>`;
      },
    },
    {
      title: "Conf",
      field: "confidence",
      width: 54,
      hozAlign: "right",
      headerFilter: "number",
      headerFilterFunc: ">=",
      formatter: (cell) => {
        const v = cell.getValue();
        return (v !== null && v !== undefined) ? v.toFixed(2) : '<span style="color:#4b5563">—</span>';
      },
    },
    {
      title: "Headline",
      field: "headline",
      headerFilter: "input",
      formatter: "plaintext",
    },
    {
      title: "Status",
      field: "status",
      width: 78,
      headerFilter: "select",
      headerFilterParams: {
        values: ["", "received", "scored", "alerted", "failed"],
        clearable: true,
      },
    },
  ],
  rowFormatter: (row) => {
    const d = row.getData();
    const el = row.getElement();
    const isLow = (d.score === null || d.score === undefined || d.score < 5);
    el.classList.toggle("row-dim", isLow);
  },
});

table.on("rowClick", (_e, row) => {
  table.getSelectedRows().forEach(r => r.deselect());
  row.select();
  showDetail(row.getData());
});

// ---------------------------------------------------------------------------
// Detail panel
// ---------------------------------------------------------------------------

let _selectedId = null;

function showDetail(data) {
  _selectedId = data.event_id;

  const strip = document.getElementById("detail-score-strip");
  const badge = document.getElementById("detail-score-badge");
  const dirEl = document.getElementById("detail-direction");
  const metaEl = document.getElementById("detail-meta");
  const link = document.getElementById("detail-source-link");
  const headEl = document.getElementById("detail-headline");
  const reasonEl = document.getElementById("detail-reasoning-text");
  const bodyEl = document.getElementById("detail-body-text");

  const v = data.score;
  const c = scoreColor(v);
  badge.style.background = c.bg;
  badge.style.color = c.text;
  badge.textContent = (v !== null && v !== undefined) ? v : "—";

  const dir = directionLabel(data.direction);
  dirEl.textContent = dir.label;
  dirEl.style.color = dir.color;

  const conf = (data.confidence !== null && data.confidence !== undefined)
    ? ` · conf ${(data.confidence * 100).toFixed(0)}%` : "";
  metaEl.textContent = `${data.source} · ${fmtTime(data.ts_ingested)}${conf}`;

  if (data.url) {
    link.href = data.url;
    link.style.display = "";
  } else {
    link.style.display = "none";
  }

  headEl.textContent = data.headline || "—";
  reasonEl.textContent = data.reasoning || "—";
  bodyEl.textContent = data.body || "No content available";

  document.getElementById("detail-empty").style.display = "none";
  document.getElementById("detail-content").style.display = "flex";
}

function updateDetailIfSelected(data) {
  if (data.event_id === _selectedId) {
    showDetail(Object.assign({}, table.getRow(data.event_id)?.getData() || {}, data));
  }
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

function todayISO() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

async function loadEvents(since, until) {
  let url = `/api/events?since=${encodeURIComponent(since)}`;
  if (until) url += `&until=${encodeURIComponent(until)}`;
  const resp = await fetch(url);
  if (!resp.ok) return;
  const rows = await resp.json();
  table.setData(rows);
}

// ---------------------------------------------------------------------------
// SSE (live mode)
// ---------------------------------------------------------------------------

let _sse = null;

function connectSSE() {
  if (_sse) { _sse.close(); _sse = null; }
  _sse = new EventSource("/api/stream");

  _sse.addEventListener("event", (e) => {
    const data = JSON.parse(e.data);
    table.updateOrAddData([data]);
    table.getRow(data.event_id)?.reformat();
  });

  _sse.addEventListener("score", (e) => {
    const data = JSON.parse(e.data);
    table.updateData([data]);
    table.getRow(data.event_id)?.reformat();
    updateDetailIfSelected(data);
  });

  _sse.onerror = () => {
    // EventSource auto-reconnects; on reconnect refresh today's data
    setTimeout(async () => {
      const today = document.getElementById("date-picker").value;
      if (today === todayISO()) {
        await loadEvents(`${today}T00:00:00Z`);
      }
    }, 2000);
  };
}

function disconnectSSE() {
  if (_sse) { _sse.close(); _sse = null; }
}

// ---------------------------------------------------------------------------
// Date picker + mode switching
// ---------------------------------------------------------------------------

const datePicker = document.getElementById("date-picker");
const modeBadge = document.getElementById("mode-badge");

// Set max to today, default to today
const todayStr = todayISO();
datePicker.max = todayStr;
datePicker.value = todayStr;

datePicker.addEventListener("change", async () => {
  const selected = datePicker.value;
  const isToday = (selected === todayStr);

  modeBadge.textContent = isToday ? "● LIVE" : `📅 ${selected} · Historical`;
  modeBadge.className = "live";
  if (!isToday) modeBadge.className = "hist";

  table.clearData();
  _selectedId = null;
  document.getElementById("detail-empty").style.display = "";
  document.getElementById("detail-content").style.display = "none";

  if (isToday) {
    await loadEvents(`${selected}T00:00:00Z`);
    connectSSE();
  } else {
    disconnectSSE();
    await loadEvents(`${selected}T00:00:00Z`, `${selected}T23:59:59Z`);
  }
});

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

async function refreshStatus() {
  const resp = await fetch("/api/status");
  if (!resp.ok) return;
  const data = await resp.json();

  const container = document.getElementById("sources-container");
  container.innerHTML = data.sources.map(s => {
    const cls = s.live ? "live" : "stale";
    const dot = s.live ? "●" : "○";
    return `<span class="src-pill ${cls}">${dot} ${s.source}</span> `;
  }).join("");

  const dlq = document.getElementById("dlq-badge");
  dlq.textContent = data.dlq_today > 0 ? `DLQ: ${data.dlq_today}` : "";
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

(async () => {
  await loadEvents(`${todayStr}T00:00:00Z`);
  connectSSE();
  await refreshStatus();
  setInterval(refreshStatus, 30_000);
})();
</script>
</body>
</html>
```

- [ ] **Step 2.3: Smoke-test the frontend manually**

Run from the repo root (requires Postgres + env vars):

```bash
POSTGRES_URL="postgresql://rates:changeme@localhost:5432/rates" \
KAFKA_BROKERS="localhost:9094" \
DASHBOARD_KAFKA_ENABLED=0 \
python -m uvicorn services.dashboard.api:app --reload --port 8000
```

Open `http://localhost:8000` in a browser. Verify:
- Table renders with today's events from Postgres
- Date picker defaults to today, `● LIVE` badge shows
- Clicking a row populates the detail panel (headline, reasoning, body)
- Selecting a past date switches to `📅 Historical` mode, table reloads, SSE disconnects

- [ ] **Step 2.4: Commit**

```bash
git add services/dashboard/static/index.html
git commit -m "feat(dashboard): add Tabulator.js frontend with split panel, SSE, date picker"
```

---

## Task 3: Dockerfile + docker-compose integration

**Files:**
- Create: `services/dashboard/Dockerfile`
- Modify: `docker-compose.yml`

- [ ] **Step 3.1: Create `services/dashboard/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "fastapi>=0.115.0" \
        "uvicorn[standard]>=0.30.0" \
        "sse-starlette>=2.0.0"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "uvicorn", "services.dashboard.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 3.2: Add `dashboard` service to `docker-compose.yml`**

Add the following block inside the `services:` section, before the `volumes:` block at the bottom:

```yaml
  dashboard:
    build:
      context: .
      dockerfile: services/dashboard/Dockerfile
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
    environment:
      KAFKA_BROKERS: "kafka:9092"
      POSTGRES_URL: "postgresql://rates:${POSTGRES_PASSWORD:-changeme}@postgres:5432/rates"
    ports:
      - "8080:8000"
    restart: unless-stopped
```

- [ ] **Step 3.3: Build and start the dashboard container**

```bash
docker compose up --build dashboard -d
```

- [ ] **Step 3.4: Confirm the service is healthy**

```bash
docker compose ps dashboard
docker compose logs dashboard --tail=20
```

Expected: status `running`, logs show `INFO: Application startup complete.`

- [ ] **Step 3.5: Open dashboard in browser**

Open `http://localhost:8080` (or `http://<pi-ip>:8080` from Windows). Verify:
- Table loads with today's events
- Live badge shows `● LIVE`
- New events appear without page refresh (requires ingestors running)
- Clicking a row shows reasoning + body in the right panel
- Per-column filters work (text, numeric, select dropdowns for Direction and Status)
- Date picker switches to historical mode for a past date

- [ ] **Step 3.6: Commit**

```bash
git add services/dashboard/Dockerfile docker-compose.yml
git commit -m "feat(dashboard): add Dockerfile and docker-compose service on port 8080"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| FastAPI + SSE backend | Task 1 `api.py` |
| `GET /`, `/api/events`, `/api/stream`, `/api/status` | Task 1 `api.py` |
| In-memory ring buffer, capped at 500 | Task 1 `_ring_upsert` |
| Warm ring buffer from Postgres at startup | Task 1 `_warm_ring_buffer` |
| Unique consumer group per dashboard instance | `make_unique_consumer` (already in `kafka_client.py`) |
| SSE fan-out to all connected clients | Task 1 `_broadcast` + `_subscribers` |
| Dark clean table, Tabulator.js CDN | Task 2 `index.html` |
| 7 table columns with correct filter types | Task 2 column definitions |
| Select dropdowns for direction and status | Task 2 `headerFilter: "select"` |
| Numeric filters for score and conf | Task 2 `headerFilter: "number"` |
| Row click → detail panel (score, reasoning, body, URL) | Task 2 `showDetail()` |
| Detail panel updates live on score SSE message | Task 2 `updateDetailIfSelected()` |
| Live mode: SSE stream + today's events | Task 2 `connectSSE()` + `loadEvents()` |
| Historical mode: date picker, no SSE | Task 2 date picker handler |
| SSE reconnect backfills missed events | Task 2 `_sse.onerror` handler |
| Status bar: per-source live/stale pills + DLQ count | Task 2 `refreshStatus()` |
| Dockerfile + docker-compose on port 8080 | Task 3 |
| `DASHBOARD_KAFKA_ENABLED=0` for tests | Task 1 `api.py` startup |
