import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from services.ingestors.truth_social.main import TruthSocialIngestor, _strip_html

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


@pytest.fixture
def post():
    return _load("truth_social_post.json")


@pytest.fixture
def reblog_post():
    return _load("truth_social_reblog.json")


@pytest.fixture
def ingestor():
    with patch("services.ingestors.truth_social.main.Api"):
        return TruthSocialIngestor(usernames=["realDonaldTrump"])


# ---- _strip_html ----

def test_strip_html_removes_tags():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"


def test_strip_html_unescapes_entities():
    assert _strip_html("AT&amp;T") == "AT&T"


def test_strip_html_collapses_whitespace():
    assert _strip_html("<p>one</p>\n<p>two</p>") == "one two"


# ---- _normalize_item ----

def test_normalize_item(ingestor, post):
    event = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    assert event.source == "truth_social"
    assert event.headline == (
        "Big announcement: We are cutting tariffs on China by 50%! "
        "Great deal for America! #MAGA"
    )
    assert event.body == event.headline
    assert event.url == "https://truthsocial.com/@realDonaldTrump/114494826741302456"
    assert event.ts_source == datetime(2026, 5, 15, 12, 34, 56, tzinfo=timezone.utc)
    assert event.metadata["post_id"] == "114494826741302456"
    assert event.metadata["account"] == "realDonaldTrump"
    assert event.metadata["is_reblog"] is False


def test_normalize_item_event_id_is_deterministic(ingestor, post):
    e1 = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    e2 = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    assert e1.event_id == e2.event_id


def test_normalize_item_reblog_uses_reblog_content(ingestor, reblog_post):
    event = ingestor._normalize_item({"post": reblog_post, "_username": "realDonaldTrump"})
    assert event.metadata["is_reblog"] is True
    assert "Treasury yields" in event.body


def test_normalize_item_missing_content_raises(ingestor, post):
    post["content"] = ""
    post["reblog"] = None
    with pytest.raises(ValueError, match="no content"):
        ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})


def test_normalize_item_missing_created_at_raises(ingestor, post):
    del post["created_at"]
    with pytest.raises(ValueError, match="no created_at"):
        ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})


# ---- _fetch_raw_items ----

def test_fetch_raw_items_updates_since_id():
    posts = [
        {
            "id": "200",
            "content": "<p>newer</p>",
            "created_at": "2026-05-15T14:00:00.000Z",
            "url": "https://truthsocial.com/p/200",
            "reblog": None,
        },
        {
            "id": "100",
            "content": "<p>older</p>",
            "created_at": "2026-05-15T13:00:00.000Z",
            "url": "https://truthsocial.com/p/100",
            "reblog": None,
        },
    ]
    with patch("services.ingestors.truth_social.main.Api") as MockApi:
        MockApi.return_value.pull_statuses.return_value = iter(posts)
        ing = TruthSocialIngestor(usernames=["realDonaldTrump"])
        assert ing._since_ids["realDonaldTrump"] is None

        result = ing._fetch_raw_items()

        assert len(result) == 2
        assert ing._since_ids["realDonaldTrump"] == "200"
        MockApi.return_value.pull_statuses.assert_called_once_with(
            "realDonaldTrump", since_id=None, replies=False
        )

        # Second call passes since_id
        MockApi.return_value.pull_statuses.return_value = iter([])
        ing._fetch_raw_items()
        MockApi.return_value.pull_statuses.assert_called_with(
            "realDonaldTrump", since_id="200", replies=False
        )


def test_fetch_raw_items_per_account_isolation():
    good_post = {
        "id": "100",
        "content": "<p>ok</p>",
        "created_at": "2026-05-15T12:00:00.000Z",
        "url": "https://truthsocial.com/p/100",
        "reblog": None,
    }
    with patch("services.ingestors.truth_social.main.Api") as MockApi:
        def _side(username, **kwargs):
            if username == "badaccount":
                raise RuntimeError("auth failed")
            return iter([good_post])

        MockApi.return_value.pull_statuses.side_effect = _side
        ing = TruthSocialIngestor(usernames=["badaccount", "realDonaldTrump"])
        items = ing._fetch_raw_items()

    assert len(items) == 1
    assert items[0]["_username"] == "realDonaldTrump"
