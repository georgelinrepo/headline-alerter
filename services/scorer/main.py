"""Scorer service: consume events.normalized, call Anthropic, produce events.scored.

A failed scoring call is routed to events.dlq so a single bad event cannot
stall the pipeline. The Postgres row is also updated to status='failed'.
"""
from __future__ import annotations
import json
import os
import sys
import threading
import time
import zoneinfo
from datetime import datetime, timezone
from typing import Any

import anthropic

from services.scorer.context_builder import seconds_until_midnight_et, build_macro_context
from services.shared.anthropic_client import ScorerError, score_event
from services.shared.db import connect
from services.shared.dlq import send_to_dlq
from services.shared.kafka_client import flush, make_consumer, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.macro_context import get_latest_context, save_context
from services.shared.models import NormalizedEvent
from services.shared.scorer_prompts import build_system_prompt


# ---- Thread-safe macro context prompt ------------------------------------

_prompt_lock = threading.Lock()
_current_system_prompt: list | None = None


def get_system_prompt() -> list | None:
    with _prompt_lock:
        return _current_system_prompt


def set_system_prompt(prompt: list) -> None:
    global _current_system_prompt
    with _prompt_lock:
        _current_system_prompt = prompt


# ---- Postgres helpers -----------------------------------------------------

def update_archive_with_score(scored) -> None:
    """UPDATE the existing events_archive row with score fields and status='scored'.

    Caller must ensure the row exists (the ingestor inserts it on receipt).
    Raises RuntimeError if no row matches — this surfaces ingestor/scorer ordering
    bugs instead of silently dropping the score.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status = 'scored',
                    ts_scored = %s,
                    score = %s,
                    direction = %s,
                    confidence = %s,
                    reasoning = %s,
                    model = %s
                WHERE id = %s
                """,
                (
                    scored.scored_at, scored.score, scored.direction,
                    scored.confidence, scored.reasoning, scored.model,
                    scored.event_id,
                ),
            )
            if cur.rowcount == 0:
                raise RuntimeError(
                    f"events_archive row not found for event_id={scored.event_id}; "
                    "ingestor must insert before scorer updates"
                )
        conn.commit()


def mark_archive_failed(event_id: str, error_msg: str) -> None:
    """Mark events_archive row as status='failed' (preserves the original payload)."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status = 'failed',
                    reasoning = %s
                WHERE id = %s
                """,
                (error_msg[:1000], event_id),
            )
        conn.commit()


# ---- per-event processing -------------------------------------------------

def process_one_event(event_dict: dict[str, Any], *, anthropic_client,
                      producer, log, model: str,
                      timeout_seconds: int = 30,
                      system_prompt: list | None = None) -> None:
    """Score a single normalized event. Routes failures to DLQ."""
    event = NormalizedEvent.from_dict(event_dict)
    started = time.monotonic()
    try:
        scored = score_event(
            anthropic_client,
            normalized_event=event,
            model=model,
            timeout_seconds=timeout_seconds,
            system_prompt=system_prompt,
        )
    except ScorerError as e:
        latency_ms = int((time.monotonic() - started) * 1000)
        log.warning("score failed", event_id=event.event_id, stage=e.stage,
                    error=str(e.original), retry_count=e.retry_count,
                    latency_ms=latency_ms)
        send_to_dlq(producer, stage=e.stage, service="scorer",
                    error=e.original or e, original_event=event_dict,
                    retry_count=e.retry_count)
        flush(producer)
        mark_archive_failed(event.event_id, f"{e.stage}: {e.original}")
        return

    # success path
    try:
        update_archive_with_score(scored)
        produce(producer, "events.scored", key=scored.event_id, payload=scored.to_dict())
        flush(producer)
    except Exception as e:
        # Unexpected error after Anthropic returned a valid score.
        # Most likely cause: events_archive row missing (RuntimeError from
        # update_archive_with_score when row was manually deleted + Kafka replayed).
        # Route to DLQ rather than crashing the scorer.
        latency_ms = int((time.monotonic() - started) * 1000)
        log.error("post-score write failed",
                  event_id=event.event_id, error=str(e), latency_ms=latency_ms)
        send_to_dlq(producer, stage="scorer_unknown", service="scorer",
                    error=e, original_event=event_dict, retry_count=0)
        flush(producer)
        return
    latency_ms = int((time.monotonic() - started) * 1000)
    log.info("scored",
             event_id=scored.event_id, score=scored.score,
             direction=scored.direction, confidence=scored.confidence,
             latency_ms=latency_ms)


# ---- Anthropic client construction ---------------------------------------

class _FakeAnthropicClient:
    """Replays a captured Anthropic response from a JSON fixture.
    Activated by SCORER_FAKE_RESPONSE_PATH; used by integration tests."""
    def __init__(self, fixture_path: str, fail_mode: str | None = None):
        with open(fixture_path) as f:
            self._payload = json.load(f)
        self._fail_mode = fail_mode
        self.messages = self  # so .messages.create works

    def create(self, **kwargs):  # pragma: no cover - exercised via integration tests
        if self._fail_mode == "rate_limit":
            # Build a minimal httpx.Response-like object for RateLimitError
            class _FakeResponse:
                status_code = 429
                headers = {}
                request = type("R", (), {"method": "POST", "url": "https://api.anthropic.com/messages"})()
            raise anthropic.RateLimitError(
                "forced 429", response=_FakeResponse(), body=None
            )
        # Build a Message-like object from the JSON payload.
        class _Block:
            def __init__(self, d): self.type = d["type"]; self.input = d.get("input", {})
        class _Resp:
            def __init__(self, d): self.content = [_Block(b) for b in d["content"]]
        return _Resp(self._payload)


def build_anthropic_client():
    fake_path = os.environ.get("SCORER_FAKE_RESPONSE_PATH")
    if fake_path:
        fail_mode = os.environ.get("SCORER_FAKE_FAIL_MODE")  # 'rate_limit' or unset
        return _FakeAnthropicClient(fake_path, fail_mode=fail_mode)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var is required")
    return anthropic.Anthropic(api_key=api_key)


# ---- macro context startup + refresh ------------------------------------

def _load_initial_context(log) -> None:
    """Load the most recent macro context from Postgres on startup."""
    try:
        with connect() as conn:
            summary = get_latest_context(conn)
        if summary:
            set_system_prompt(build_system_prompt(summary))
            log.info("macro context loaded", chars=len(summary))
        else:
            log.info("no macro context found, using bare system prompt")
    except Exception as e:
        log.warning("failed to load macro context on startup", error=str(e))


def _context_refresh_loop(anthropic_client, context_model: str, log) -> None:
    """Background daemon: rebuild macro context at midnight ET each night."""
    while True:
        delay = seconds_until_midnight_et()
        log.info("context refresh sleeping until midnight ET", seconds=int(delay))
        time.sleep(delay)
        try:
            today = datetime.now(zoneinfo.ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
            summary = build_macro_context(anthropic_client, context_model, today)
            with connect() as conn:
                save_context(conn, summary, context_model)
                conn.commit()
            set_system_prompt(build_system_prompt(summary))
            log.info("macro context refreshed", model=context_model, chars=len(summary))
        except Exception as e:
            log.error("context refresh failed", error=str(e))


# ---- main loop -----------------------------------------------------------

def _consumer_group_id() -> str:
    return os.environ.get("SCORER_GROUP_ID", "scorer-cg")


def main() -> int:
    configure_logging("scorer")
    log = get_logger()
    log.info("starting scorer")

    model = os.environ.get("SCORER_MODEL", "claude-haiku-4-5")
    timeout_s = int(os.environ.get("SCORER_TIMEOUT_SECONDS", "30"))

    client = build_anthropic_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.normalized"])

    context_model = os.environ.get("CONTEXT_MODEL", "claude-sonnet-4-6")
    _load_initial_context(log)
    t = threading.Thread(
        target=_context_refresh_loop,
        args=(client, context_model, log),
        daemon=True,
    )
    t.start()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error", error=str(msg.error()))
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error("payload decode failed", error=str(e))
                # Skip undecodeable bytes; commit so we don't loop forever.
                consumer.commit(message=msg, asynchronous=False)
                continue

            process_one_event(
                payload,
                anthropic_client=client,
                producer=producer,
                log=log,
                model=model,
                timeout_seconds=timeout_s,
                system_prompt=get_system_prompt(),
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        flush(producer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
