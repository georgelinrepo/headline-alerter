"""Unit tests for alert message formatting."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.alerter.format import format_alert
from services.shared.models import ScoredEvent


FIXTURE_PATH = Path(__file__).parents[2] / "fixtures" / "scored_event.json"


@pytest.fixture
def scored() -> ScoredEvent:
    return ScoredEvent.from_dict(json.loads(FIXTURE_PATH.read_text()))


@pytest.fixture
def archive_ctx() -> dict:
    return {
        "headline": "Fed Chair Powell remarks on inflation outlook",
        "source": "cnbc_rss",
        "ts_source": datetime(2026, 5, 6, 14, 32, tzinfo=timezone.utc),
        "url": "https://www.cnbc.com/2026/05/06/powell.html",
    }


def test_format_includes_score_and_direction_glyph(scored, archive_ctx):
    body = format_alert(scored, **archive_ctx)
    assert "[7/10 ↓ rates_lower" in body
    assert "conf 72%" in body


def test_format_includes_source_and_short_timestamp(scored, archive_ctx):
    body = format_alert(scored, **archive_ctx)
    assert "cnbc_rss · 14:32Z" in body


def test_format_includes_headline_reasoning_and_url(scored, archive_ctx):
    body = format_alert(scored, **archive_ctx)
    assert "Fed Chair Powell remarks on inflation outlook" in body
    assert "Powell tone notably" in body
    assert "https://www.cnbc.com/2026/05/06/powell.html" in body


def test_direction_glyph_for_each_value(scored, archive_ctx):
    glyphs = {"rates_higher": "↑", "rates_lower": "↓", "neutral": "→", "unclear": "?"}
    for direction, glyph in glyphs.items():
        scored.direction = direction
        body = format_alert(scored, **archive_ctx)
        assert glyph in body, f"glyph {glyph!r} missing for direction {direction!r}"


def test_format_omits_url_section_when_url_is_none(scored, archive_ctx):
    archive_ctx["url"] = None
    body = format_alert(scored, **archive_ctx)
    assert "https://" not in body


def test_format_total_under_4096_chars(scored, archive_ctx):
    """WhatsApp soft cap is 4096 — make sure we never exceed."""
    body = format_alert(scored, **archive_ctx)
    assert len(body) < 4096
