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
    since_dt = datetime.fromisoformat(since.replace(" ", "+"))
    where = ["ts_ingested >= %s"]
    params: list[Any] = [since_dt]
    if until:
        until_dt = datetime.fromisoformat(until.replace(" ", "+"))
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
