import json
from unittest.mock import MagicMock
from services.shared.dlq import send_to_dlq, build_envelope


def test_envelope_has_required_fields():
    err = ValueError("boom")
    env = build_envelope(
        stage="scorer_throttle",
        service="scorer",
        error=err,
        original_event={"event_id": "abc", "source": "cnbc_rss"},
        retry_count=3,
    )
    assert env["stage"] == "scorer_throttle"
    assert env["service"] == "scorer"
    assert env["retry_count"] == 3
    assert env["original_event"] == {"event_id": "abc", "source": "cnbc_rss"}
    assert env["error"] == "ValueError: boom"
    assert "ts_dlq" in env
    # ts_dlq must be ISO-8601 UTC
    assert env["ts_dlq"].endswith("Z") or "+00:00" in env["ts_dlq"]


def test_envelope_with_no_event_uses_empty_dict():
    env = build_envelope(
        stage="ingest_parse",
        service="ingestor-cnbc",
        error=KeyError("title"),
        original_event=None,
    )
    assert env["original_event"] == {}
    assert env["error"] == "KeyError: 'title'"
    assert env["retry_count"] == 0


def test_send_to_dlq_produces_with_event_id_key():
    producer = MagicMock()
    send_to_dlq(
        producer,
        stage="scorer_5xx",
        service="scorer",
        error=RuntimeError("fail"),
        original_event={"event_id": "evt-42", "headline": "x"},
        retry_count=2,
    )
    assert producer.produce.called
    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "events.dlq"
    assert kwargs["key"] == b"evt-42"
    payload = json.loads(kwargs["value"].decode())
    assert payload["stage"] == "scorer_5xx"
    assert payload["original_event"]["event_id"] == "evt-42"


def test_send_to_dlq_uses_unknown_key_when_event_id_missing():
    producer = MagicMock()
    send_to_dlq(
        producer,
        stage="ingest_parse",
        service="ingestor-cnbc",
        error=ValueError("malformed"),
        original_event={"raw": "no event_id here"},
    )
    kwargs = producer.produce.call_args.kwargs
    assert kwargs["key"] == b"unknown"
