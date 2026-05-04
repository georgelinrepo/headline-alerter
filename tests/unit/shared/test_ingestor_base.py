"""Unit tests for the Ingestor ABC.

We build a tiny FakeIngestor subclass and inject:
- a `_fetch_raw_items` that we control
- a fake Kafka producer (MagicMock)
- a fake `connect()` context manager (monkeypatched) backed by an in-memory store
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from services.shared.ingestor_base import Ingestor
from services.shared.models import NormalizedEvent


class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self.last_query = None
        self.last_params = None

    def execute(self, query, params=()):
        self.last_query = query
        self.last_params = params
        if "MAX(ts_source)" in query:
            source = params[0]
            rows = [r for r in self.store["events"] if r["source"] == source]
            self._result = [(max((r["ts_source"] for r in rows), default=None),)]
        elif "INSERT INTO events_archive" in query:
            row = {
                "id": params[0], "source": params[1],
                "ts_source": params[2], "ts_ingested": params[3],
                "headline": params[4], "body": params[5], "url": params[6],
                "metadata": params[7], "status": "received",
            }
            existing = next((r for r in self.store["events"] if r["id"] == row["id"]), None)
            if existing is None:
                self.store["events"].append(row)
            self._result = []
        else:
            self._result = []

    def fetchone(self):
        return self._result[0] if self._result else None

    def __enter__(self): return self
    def __exit__(self, *a): pass


class _FakeConn:
    def __init__(self, store):
        self.store = store
    def cursor(self):
        return _FakeCursor(self.store)
    def commit(self):
        pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


@pytest.fixture
def fake_db(monkeypatch):
    store = {"events": []}
    def _connect():
        return _FakeConn(store)
    # The base class uses `from .db import connect` — so we patch the symbol on the module.
    monkeypatch.setattr("services.shared.ingestor_base.connect", _connect)
    return store


class _FakeIngestor(Ingestor):
    source_name = "fake_src"
    poll_interval_seconds = 60

    def __init__(self, items_per_iteration: list[list[dict]], producer=None,
                 fetch_raises: list[Exception | None] | None = None):
        super().__init__(producer=producer or MagicMock())
        self._items_queue = list(items_per_iteration)
        self._raises = list(fetch_raises) if fetch_raises else []

    def _fetch_raw_items(self):
        if self._raises:
            err = self._raises.pop(0)
            if err is not None:
                raise err
        return self._items_queue.pop(0) if self._items_queue else []

    def _normalize_item(self, raw):
        if "bad" in raw:
            raise ValueError("malformed item")
        return NormalizedEvent(
            event_id=raw["id"],
            source=self.source_name,
            ts_source=datetime.fromisoformat(raw["ts"]),
            ts_ingested=datetime.now(timezone.utc),
            headline=raw["headline"],
            body=raw.get("body"),
            url=raw.get("url"),
            metadata={},
        )


def _item(eid, ts_iso, headline="h"):
    return {"id": eid, "ts": ts_iso, "headline": headline, "url": f"https://x/{eid}"}


# --- hydration ------------------------------------------------------------

def test_hydrate_uses_24h_fallback_when_table_empty(fake_db):
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(hours=23, minutes=59) < delta < timedelta(hours=24, minutes=1)


def test_hydrate_uses_db_max_when_recent(fake_db):
    fake_db["events"].append({
        "id": "x", "source": "fake_src",
        "ts_source": datetime.now(timezone.utc) - timedelta(hours=1),
    })
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(minutes=59) < delta < timedelta(minutes=61)


def test_hydrate_uses_24h_fallback_when_max_too_old(fake_db):
    fake_db["events"].append({
        "id": "x", "source": "fake_src",
        "ts_source": datetime.now(timezone.utc) - timedelta(days=5),
    })
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(hours=23, minutes=59) < delta < timedelta(hours=24, minutes=1)


# --- normal-path emission -------------------------------------------------

def test_emits_only_items_newer_than_last_ts(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        _item("old", (now - timedelta(hours=48)).isoformat()),     # filtered (>24h old)
        _item("recent", (now - timedelta(minutes=10)).isoformat()), # passes
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    emitted = ing.run_one_iteration()
    assert emitted == 1
    # Producer was called once with topic events.normalized
    assert ing.producer.produce.call_count == 1
    kwargs = ing.producer.produce.call_args.kwargs
    assert kwargs["topic"] == "events.normalized"
    payload = json.loads(kwargs["value"].decode())
    assert payload["event_id"] == "recent"


def test_archives_emitted_events(fake_db):
    now = datetime.now(timezone.utc)
    items = [_item("e1", (now - timedelta(minutes=5)).isoformat())]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    rows = [r for r in fake_db["events"] if r["id"] == "e1"]
    assert len(rows) == 1


def test_advances_last_ts_to_max_emitted(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        _item("a", (now - timedelta(minutes=5)).isoformat()),
        _item("b", (now - timedelta(minutes=2)).isoformat()),
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    delta = now - ing._last_ts_source
    assert delta < timedelta(minutes=3)


# --- error paths ----------------------------------------------------------

def test_fetch_failure_doubles_backoff(fake_db):
    ing = _FakeIngestor([], fetch_raises=[RuntimeError("rss 503")])
    ing.hydrate_last_ts()
    assert ing._backoff_seconds == 60
    ing.run_one_iteration()
    assert ing._backoff_seconds == 120


def test_fetch_failure_caps_backoff_at_600(fake_db):
    ing = _FakeIngestor([], fetch_raises=[RuntimeError("e")] * 12)
    ing.hydrate_last_ts()
    for _ in range(12):
        ing.run_one_iteration()
    assert ing._backoff_seconds == 600


def test_fetch_success_resets_backoff(fake_db):
    now = datetime.now(timezone.utc)
    ing = _FakeIngestor(
        [[], [_item("ok", (now - timedelta(minutes=1)).isoformat())]],
        fetch_raises=[RuntimeError("first fails"), None],
    )
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    assert ing._backoff_seconds == 120
    ing.run_one_iteration()
    assert ing._backoff_seconds == 60


def test_normalize_failure_routes_to_dlq_and_continues(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        {"id": "bad", "bad": True},
        _item("good", (now - timedelta(minutes=1)).isoformat()),
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    emitted = ing.run_one_iteration()
    # 'good' was emitted; 'bad' went to DLQ.
    assert emitted == 1
    topics = [c.kwargs["topic"] for c in ing.producer.produce.call_args_list]
    assert "events.dlq" in topics
    assert "events.normalized" in topics
