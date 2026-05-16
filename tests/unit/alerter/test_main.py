"""Unit tests for the alerter's per-event processing function."""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from services.alerter.main import process_one_alert
from services.alerter.twilio_client import AlerterError


@pytest.fixture
def scored_dict():
    return {
        "event_id": "evt-1",
        "score": 7,
        "direction": "rates_lower",
        "confidence": 0.72,
        "reasoning": "Powell tone notably more dovish",
        "model": "claude-haiku-4-5",
        "scored_at": "2026-05-06T14:32:11+00:00",
    }


@pytest.fixture
def below_threshold_dict():
    return {
        "event_id": "evt-low",
        "score": 2,
        "direction": "neutral",
        "confidence": 0.85,
        "reasoning": "noise",
        "model": "claude-haiku-4-5",
        "scored_at": "2026-05-06T14:32:11+00:00",
    }


@pytest.fixture
def fake_pg(monkeypatch):
    """Mock the PG helper functions in services.alerter.main."""
    state = {
        "alerted_ids": set(),
        "archive": {},  # event_id → (headline, source, ts_source, url)
        "alert_history": [],  # appended on insert
        "marked_alerted": [],  # appended on update
    }

    def _has_been_alerted(event_id):
        return event_id in state["alerted_ids"]

    def _fetch_archive_context(event_id):
        if event_id not in state["archive"]:
            raise RuntimeError(f"archive row missing for {event_id}")
        return state["archive"][event_id]

    def _insert_alert_history(event_id, *, channel, recipient, twilio_sid):
        state["alert_history"].append({
            "event_id": event_id, "channel": channel,
            "recipient": recipient, "twilio_sid": twilio_sid,
        })
        state["alerted_ids"].add(event_id)

    def _mark_alerted(event_id):
        state["marked_alerted"].append(event_id)

    monkeypatch.setattr("services.alerter.main.has_been_alerted", _has_been_alerted)
    monkeypatch.setattr("services.alerter.main.fetch_archive_context", _fetch_archive_context)
    monkeypatch.setattr("services.alerter.main.insert_alert_history", _insert_alert_history)
    monkeypatch.setattr("services.alerter.main.mark_alerted", _mark_alerted)
    monkeypatch.setattr("services.alerter.main.has_alerted_on_headline_recently",
                        lambda headline, source, hours=24: False)

    # Default archive entry for evt-1
    state["archive"]["evt-1"] = (
        "Powell remarks on inflation",
        "cnbc_rss",
        datetime(2026, 5, 6, 14, 32, tzinfo=timezone.utc),
        "https://example.com/x",
    )
    return state


def _kwargs(**overrides):
    base = dict(
        twilio_client=MagicMock(),
        producer=MagicMock(),
        log=MagicMock(),
        channel="whatsapp",
        recipient="whatsapp:+44...",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )
    base.update(overrides)
    return base


def test_below_threshold_is_silently_skipped(fake_pg, below_threshold_dict, monkeypatch):
    """No Twilio call, no PG writes, no Kafka produce."""
    sent = []
    monkeypatch.setattr("services.alerter.main.send_message",
                        lambda *a, **kw: (sent.append(kw), "SMfake")[1])
    kwargs = _kwargs()

    process_one_alert(below_threshold_dict, **kwargs)

    assert sent == []
    assert fake_pg["alert_history"] == []
    assert fake_pg["marked_alerted"] == []
    assert kwargs["producer"].produce.called is False


def test_idempotency_skip_when_already_alerted(fake_pg, scored_dict, monkeypatch):
    """If alert_history already has a row for this event_id, skip everything."""
    fake_pg["alerted_ids"].add("evt-1")
    sent = []
    monkeypatch.setattr("services.alerter.main.send_message",
                        lambda *a, **kw: sent.append(kw))
    kwargs = _kwargs()

    process_one_alert(scored_dict, **kwargs)

    assert sent == []
    assert fake_pg["alert_history"] == []
    assert kwargs["producer"].produce.called is False


def test_success_path_sends_records_and_audits(fake_pg, scored_dict, monkeypatch):
    monkeypatch.setattr("services.alerter.main.send_message",
                        lambda *a, **kw: "SM_real_sid")
    kwargs = _kwargs()

    process_one_alert(scored_dict, **kwargs)

    # alert_history written with all fields
    assert fake_pg["alert_history"] == [{
        "event_id": "evt-1", "channel": "whatsapp",
        "recipient": "whatsapp:+44...", "twilio_sid": "SM_real_sid",
    }]
    # events_archive marked alerted
    assert fake_pg["marked_alerted"] == ["evt-1"]
    # alerts.outgoing audit produced
    topics = [c.kwargs["topic"] for c in kwargs["producer"].produce.call_args_list]
    assert "alerts.outgoing" in topics
    audit_call = next(c for c in kwargs["producer"].produce.call_args_list
                      if c.kwargs["topic"] == "alerts.outgoing")
    audit = json.loads(audit_call.kwargs["value"].decode())
    assert audit["event_id"] == "evt-1"
    assert audit["twilio_sid"] == "SM_real_sid"
    assert audit["channel"] == "whatsapp"
    # success log
    kwargs["log"].info.assert_any_call(
        "alerted", event_id="evt-1", score=7,
        direction="rates_lower", confidence=0.72, twilio_sid="SM_real_sid"
    )


def test_alerter_error_routes_to_dlq_no_pg_writes(fake_pg, scored_dict, monkeypatch):
    err = AlerterError("alerter_throttle", original=RuntimeError("429"), retry_count=3)
    monkeypatch.setattr("services.alerter.main.send_message",
                        MagicMock(side_effect=err))
    kwargs = _kwargs()

    process_one_alert(scored_dict, **kwargs)

    # No PG writes
    assert fake_pg["alert_history"] == []
    assert fake_pg["marked_alerted"] == []
    # DLQ produce
    topics = [c.kwargs["topic"] for c in kwargs["producer"].produce.call_args_list]
    assert "events.dlq" in topics
    assert "alerts.outgoing" not in topics
    dlq_call = next(c for c in kwargs["producer"].produce.call_args_list
                    if c.kwargs["topic"] == "events.dlq")
    payload = json.loads(dlq_call.kwargs["value"].decode())
    assert payload["stage"] == "alerter_throttle"
    assert payload["service"] == "alerter"
    assert payload["original_event"]["event_id"] == "evt-1"
    assert payload["retry_count"] == 3


def test_archive_row_missing_routes_to_dlq_unknown(fake_pg, scored_dict, monkeypatch):
    """fetch_archive_context raising should route to DLQ as alerter_unknown — not crash."""
    fake_pg["archive"].clear()  # remove evt-1 from archive
    sent = []
    monkeypatch.setattr("services.alerter.main.send_message",
                        lambda *a, **kw: sent.append(kw))
    kwargs = _kwargs()

    process_one_alert(scored_dict, **kwargs)

    assert sent == []
    topics = [c.kwargs["topic"] for c in kwargs["producer"].produce.call_args_list]
    assert "events.dlq" in topics
    dlq_call = next(c for c in kwargs["producer"].produce.call_args_list
                    if c.kwargs["topic"] == "events.dlq")
    payload = json.loads(dlq_call.kwargs["value"].decode())
    assert payload["stage"] == "alerter_unknown"


def test_post_send_pg_failure_routes_to_dlq(fake_pg, scored_dict, monkeypatch):
    """If PG write fails AFTER Twilio sent the message, route to DLQ — don't crash."""
    monkeypatch.setattr("services.alerter.main.send_message",
                        lambda *a, **kw: "SMok")
    def _boom(*a, **kw): raise RuntimeError("pg down")
    monkeypatch.setattr("services.alerter.main.insert_alert_history", _boom)
    kwargs = _kwargs()

    process_one_alert(scored_dict, **kwargs)

    topics = [c.kwargs["topic"] for c in kwargs["producer"].produce.call_args_list]
    assert "events.dlq" in topics
    dlq_call = next(c for c in kwargs["producer"].produce.call_args_list
                    if c.kwargs["topic"] == "events.dlq")
    payload = json.loads(dlq_call.kwargs["value"].decode())
    assert payload["stage"] == "alerter_unknown"
    # The DLQ envelope's original_event includes the twilio_sid so the operator
    # can manually reconcile (Twilio already delivered; PG record is missing).
    assert payload["original_event"]["_twilio_sid"] == "SMok"
