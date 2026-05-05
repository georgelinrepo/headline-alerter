"""Unit tests for the CNBC ingestor.

Strategy: we don't hit the network. We replace `feedparser.parse` with a stub
that returns the parsed contents of our captured XML fixture, then assert the
NormalizedEvent shape.
"""
from datetime import timezone
from pathlib import Path
from unittest.mock import MagicMock

import feedparser
import pytest

from services.ingestors.cnbc_rss.main import CnbcIngestor

FIXTURE_PATH = Path(__file__).parents[2] / "fixtures" / "cnbc_sample.xml"


@pytest.fixture
def parsed_fixture():
    return feedparser.parse(FIXTURE_PATH.read_bytes())


def test_normalize_powell_item_maps_all_fields(parsed_fixture):
    ing = CnbcIngestor(urls=["http://x"], producer=MagicMock())
    raw = parsed_fixture.entries[0].__dict__ | {"_feed_url": "http://x"}
    raw["entry"] = parsed_fixture.entries[0]

    event = ing._normalize_item(raw)

    assert event.source == "cnbc_rss"
    assert "Powell" in event.headline
    assert "ease policy" in (event.body or "")
    assert event.url.startswith("https://www.cnbc.com/")
    assert event.ts_source.tzinfo is not None
    assert event.ts_source.year == 2026
    assert event.metadata["raw_id"] == "108300001"
    assert event.metadata["feed_url"] == "http://x"


def test_event_id_is_deterministic(parsed_fixture):
    ing = CnbcIngestor(urls=["http://x"], producer=MagicMock())
    raw = {"entry": parsed_fixture.entries[0], "_feed_url": "http://x"}

    e1 = ing._normalize_item(raw)
    e2 = ing._normalize_item(raw)

    assert e1.event_id == e2.event_id
    # Changing url changes event_id.
    raw_alt = {"entry": parsed_fixture.entries[1], "_feed_url": "http://x"}
    e3 = ing._normalize_item(raw_alt)
    assert e3.event_id != e1.event_id


def test_normalize_raises_when_required_field_missing(parsed_fixture):
    ing = CnbcIngestor(urls=["http://x"], producer=MagicMock())
    bad_entry = MagicMock()
    bad_entry.title = "no link or pubdate"
    bad_entry.get = MagicMock(return_value=None)
    raw = {"entry": bad_entry, "_feed_url": "http://x"}
    with pytest.raises(Exception):
        ing._normalize_item(raw)


def test_fetch_calls_feedparser_for_each_url(monkeypatch):
    calls = []
    def fake_parse(url):
        calls.append(url)
        # Return an empty parsed object — we just want to verify wiring.
        out = MagicMock()
        out.bozo = False
        out.entries = []
        return out
    monkeypatch.setattr("services.ingestors.cnbc_rss.main.feedparser.parse", fake_parse)

    ing = CnbcIngestor(urls=["http://a", "http://b", "http://c"], producer=MagicMock())
    items = ing._fetch_raw_items()

    assert calls == ["http://a", "http://b", "http://c"]
    assert items == []


def test_fetch_skips_url_that_errors_at_feed_level(monkeypatch):
    """If one URL is bozo (parse error), we log and skip it, not raise."""
    bozo = MagicMock()
    bozo.bozo = True
    bozo.bozo_exception = ValueError("xml broken")
    bozo.entries = []
    good = MagicMock()
    good.bozo = False
    good.entries = [MagicMock(title="T", link="https://x", id="g1")]
    good.entries[0].get = MagicMock(side_effect=lambda k, d=None: {"published_parsed": None}.get(k, d))

    seq = iter([bozo, good])
    monkeypatch.setattr("services.ingestors.cnbc_rss.main.feedparser.parse",
                        lambda u: next(seq))

    ing = CnbcIngestor(urls=["http://bad", "http://good"], producer=MagicMock())
    items = ing._fetch_raw_items()
    # We don't raise; we get the 1 entry from the good feed.
    assert len(items) == 1
    assert items[0]["_feed_url"] == "http://good"
