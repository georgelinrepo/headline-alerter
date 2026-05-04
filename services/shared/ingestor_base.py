"""Ingestor base class shared by every source.

Subclass contract:
- Set the class attribute `source_name`.
- Override `_fetch_raw_items() -> list[dict]`. Should not raise on transient
  network errors — log and return []. Raise only on truly unexpected failures
  (these will trigger the backoff in `run_one_iteration()`).
- Override `_normalize_item(raw: dict) -> NormalizedEvent`. Raise on parse
  errors; the base routes them to events.dlq.

The base owns: the polling loop, last-ts hydration on startup, archival
into events_archive, Kafka produce to events.normalized, DLQ routing for
parse errors, and exponential fetch backoff.
"""
from __future__ import annotations
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer

from .db import connect
from .dlq import send_to_dlq
from .kafka_client import flush, make_producer, produce
from .logging import get_logger
from .models import NormalizedEvent


_BACKOFF_CAP_SECONDS = 600


class Ingestor(ABC):
    source_name: str  # override
    poll_interval_seconds: int = 60

    def __init__(self, *, producer: Producer | None = None) -> None:
        self.log = get_logger().bind(source=self.source_name)
        self.producer = producer if producer is not None else make_producer()
        self._backoff_seconds = self.poll_interval_seconds
        self._last_ts_source: datetime | None = None

    # ---- subclass contract ----

    @abstractmethod
    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        """Return zero or more raw items (any dict shape; passed to _normalize_item)."""

    @abstractmethod
    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        """Convert one raw item to a NormalizedEvent. Raise on parse errors."""

    # ---- public lifecycle ----

    def hydrate_last_ts(self) -> None:
        """Load `MAX(ts_source) FROM events_archive WHERE source = ?`.
        Falls back to (now - 24h) if no rows or the max is older than 24h."""
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(ts_source) FROM events_archive WHERE source = %s",
                    (self.source_name,),
                )
                row = cur.fetchone()
        max_ts = row[0] if row else None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        if max_ts is None or max_ts < cutoff:
            self._last_ts_source = cutoff
            self.log.info("hydrated last_ts (24h fallback)",
                          last_ts=self._last_ts_source.isoformat())
        else:
            self._last_ts_source = max_ts
            self.log.info("hydrated last_ts", last_ts=self._last_ts_source.isoformat())

    def run_one_iteration(self) -> int:
        """One poll cycle. Returns the number of events emitted."""
        try:
            raw_items = self._fetch_raw_items()
        except Exception as e:
            self._backoff_seconds = min(self._backoff_seconds * 2, _BACKOFF_CAP_SECONDS)
            self.log.warning("fetch failed; backing off",
                             error=str(e), next_interval_seconds=self._backoff_seconds)
            return 0

        # Reset backoff on successful fetch.
        self._backoff_seconds = self.poll_interval_seconds

        emitted = 0
        for raw in raw_items:
            try:
                event = self._normalize_item(raw)
            except Exception as e:
                self.log.warning("ingest_parse failed", error=str(e), raw=str(raw)[:300])
                send_to_dlq(
                    self.producer,
                    stage="ingest_parse",
                    service=f"ingestor-{self.source_name}",
                    error=e,
                    original_event=raw if isinstance(raw, dict) else {"raw": str(raw)},
                )
                continue

            if self._last_ts_source is not None and event.ts_source <= self._last_ts_source:
                continue  # already seen

            self._archive(event)
            produce(self.producer, "events.normalized",
                    key=event.source, payload=event.to_dict())
            self._last_ts_source = (
                max(self._last_ts_source, event.ts_source)
                if self._last_ts_source else event.ts_source
            )
            emitted += 1
            self.log.info("emitted", event_id=event.event_id, headline=event.headline[:80])

        flush(self.producer)
        return emitted

    def run(self) -> None:
        """Forever loop. Hydrates last_ts on first call."""
        self.hydrate_last_ts()
        while True:
            self.run_one_iteration()
            time.sleep(self._backoff_seconds)

    # ---- helpers ----

    def _archive(self, ev: NormalizedEvent) -> None:
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
                        ev.event_id, ev.source, ev.ts_source, ev.ts_ingested,
                        ev.headline, ev.body, ev.url, json.dumps(ev.metadata),
                    ),
                )
            conn.commit()
