"""Unit tests for the scorer's per-event processing function.

We test `process_one_event(event_dict, anthropic_client, producer, log, model)` directly.
The function is the heart of the scorer; the main loop just polls Kafka, calls
process_one_event, and commits the offset.
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import anthropic
import pytest

from services.shared.anthropic_client import ScorerError
from services.scorer.main import process_one_event


@pytest.fixture
def normalized_event_dict():
    return {
        "event_id": "evt-1",
        "source": "cnbc_rss",
        "ts_source": "2026-05-04T14:00:00+00:00",
        "ts_ingested": "2026-05-04T14:00:05+00:00",
        "headline": "Powell signals dovish pivot",
        "body": "Body text here.",
        "url": "https://x",
        "metadata": {},
    }


def _success_anthropic_client():
    client = MagicMock()
    block = MagicMock()
    block.type = "tool_use"
    block.input = {"score": 7, "direction": "rates_lower",
                   "confidence": 0.72, "reasoning": "...dovish..."}
    response = MagicMock()
    response.content = [block]
    client.messages.create.return_value = response
    return client


def _failing_anthropic_client(exc):
    client = MagicMock()
    client.messages.create.side_effect = exc
    return client


@pytest.fixture
def fake_pg(monkeypatch):
    """Capture every call to update_archive_with_score / update_archive_status_failed."""
    calls = []
    def upsert_score(scored):
        calls.append(("score", scored))
    def mark_failed(event_id, error_msg):
        calls.append(("failed", event_id, error_msg))
    monkeypatch.setattr("services.scorer.main.update_archive_with_score", upsert_score)
    monkeypatch.setattr("services.scorer.main.mark_archive_failed", mark_failed)
    return calls


def test_success_produces_scored_and_upserts_archive(fake_pg, normalized_event_dict):
    producer = MagicMock()
    log = MagicMock()
    client = _success_anthropic_client()

    process_one_event(
        normalized_event_dict, anthropic_client=client,
        producer=producer, log=log, model="claude-haiku-4-5",
    )

    # produced to events.scored
    topics = [c.kwargs["topic"] for c in producer.produce.call_args_list]
    assert "events.scored" in topics
    scored_call = next(c for c in producer.produce.call_args_list
                       if c.kwargs["topic"] == "events.scored")
    payload = json.loads(scored_call.kwargs["value"].decode())
    assert payload["event_id"] == "evt-1"
    assert payload["score"] == 7
    # upserted with score
    assert any(c[0] == "score" for c in fake_pg)
    # success log includes latency_ms
    log.info.assert_any_call("scored", event_id="evt-1", score=7, direction="rates_lower",
                             confidence=0.72, latency_ms=pytest.approx(0, abs=10000))


def test_throttle_routes_to_dlq_and_marks_failed(fake_pg, normalized_event_dict, monkeypatch):
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    producer = MagicMock()
    log = MagicMock()
    err = anthropic.RateLimitError("429", response=MagicMock(status_code=429), body=None)
    client = _failing_anthropic_client(err)

    process_one_event(
        normalized_event_dict, anthropic_client=client,
        producer=producer, log=log, model="m",
    )

    topics = [c.kwargs["topic"] for c in producer.produce.call_args_list]
    assert "events.dlq" in topics
    assert "events.scored" not in topics
    dlq_call = next(c for c in producer.produce.call_args_list
                    if c.kwargs["topic"] == "events.dlq")
    payload = json.loads(dlq_call.kwargs["value"].decode())
    assert payload["stage"] == "scorer_throttle"
    assert payload["service"] == "scorer"
    assert payload["original_event"]["event_id"] == "evt-1"
    # PG marked failed
    assert any(c[0] == "failed" and c[1] == "evt-1" for c in fake_pg)


def test_missing_archive_row_routes_to_dlq_unknown(fake_pg, normalized_event_dict, monkeypatch):
    """If update_archive_with_score raises (e.g., row missing), route to DLQ as scorer_unknown."""
    def boom(scored):
        raise RuntimeError("events_archive row not found for event_id=evt-1")
    monkeypatch.setattr("services.scorer.main.update_archive_with_score", boom)

    producer = MagicMock()
    log = MagicMock()
    client = _success_anthropic_client()

    process_one_event(
        normalized_event_dict, anthropic_client=client,
        producer=producer, log=log, model="m",
    )

    topics = [c.kwargs["topic"] for c in producer.produce.call_args_list]
    assert "events.dlq" in topics
    assert "events.scored" not in topics  # we never produced the score
    dlq_call = next(c for c in producer.produce.call_args_list
                    if c.kwargs["topic"] == "events.dlq")
    payload = json.loads(dlq_call.kwargs["value"].decode())
    assert payload["stage"] == "scorer_unknown"
    assert payload["service"] == "scorer"


def test_process_one_event_passes_system_prompt_to_score_event(
    fake_pg, normalized_event_dict, monkeypatch
):
    """process_one_event forwards system_prompt to score_event."""
    from unittest.mock import MagicMock
    captured = {}

    def fake_score_event(client, *, normalized_event, model, timeout_seconds, system_prompt=None):
        from services.shared.models import ScoredEvent
        from datetime import datetime, timezone
        captured["system_prompt"] = system_prompt
        return ScoredEvent(
            event_id=normalized_event.event_id,
            score=7, direction="rates_lower", confidence=0.72,
            reasoning="test", model=model,
            scored_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr("services.scorer.main.score_event", fake_score_event)

    from services.scorer.main import process_one_event
    custom_prompt = [{"type": "text", "text": "custom context"}]
    process_one_event(
        normalized_event_dict,
        anthropic_client=MagicMock(),
        producer=MagicMock(),
        log=MagicMock(),
        model="m",
        system_prompt=custom_prompt,
    )
    assert captured["system_prompt"] == custom_prompt


def test_get_set_system_prompt_thread_safe():
    """get_system_prompt returns whatever set_system_prompt last set."""
    from services.scorer.main import get_system_prompt, set_system_prompt
    prompt = [{"type": "text", "text": "test"}]
    set_system_prompt(prompt)
    assert get_system_prompt() == prompt
