from datetime import datetime, timezone
from services.shared.models import NormalizedEvent, ScoredEvent


def test_normalized_event_round_trip():
    ev = NormalizedEvent(
        event_id="x-123",
        source="fed_rss",
        ts_source=datetime(2026, 5, 3, 14, 32, tzinfo=timezone.utc),
        ts_ingested=datetime(2026, 5, 3, 14, 32, 8, tzinfo=timezone.utc),
        headline="Test headline",
        body="Test body",
        url="https://example.com",
        metadata={"raw_id": "abc"},
    )
    d = ev.to_dict()
    assert d["event_id"] == "x-123"
    assert d["source"] == "fed_rss"
    assert d["metadata"] == {"raw_id": "abc"}
    restored = NormalizedEvent.from_dict(d)
    assert restored == ev


def test_normalized_event_optional_fields_default():
    ev = NormalizedEvent(
        event_id="x-1",
        source="x",
        ts_source=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ts_ingested=datetime(2026, 1, 1, tzinfo=timezone.utc),
        headline="h",
    )
    assert ev.body is None
    assert ev.url is None
    assert ev.metadata == {}
    restored = NormalizedEvent.from_dict(ev.to_dict())
    assert restored == ev


def test_scored_event_round_trip():
    sc = ScoredEvent(
        event_id="x-123",
        score=8,
        direction="rates_lower",
        confidence=0.75,
        reasoning="Powell tone notably dovish",
        model="claude-haiku-4-5",
        scored_at=datetime(2026, 5, 3, 14, 32, 11, tzinfo=timezone.utc),
    )
    d = sc.to_dict()
    assert d["score"] == 8
    assert d["confidence"] == 0.75
    restored = ScoredEvent.from_dict(d)
    assert restored == sc
