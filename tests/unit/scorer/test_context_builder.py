"""Unit tests for context_builder.py."""
from __future__ import annotations
from datetime import datetime
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# seconds_until_midnight_et
# ---------------------------------------------------------------------------

def testseconds_until_midnight_et_from_noon():
    from services.scorer.context_builder import seconds_until_midnight_et
    import zoneinfo
    et = zoneinfo.ZoneInfo("America/New_York")
    # 12:00 ET → midnight is 12h away = 43200s
    noon_et = datetime(2026, 5, 16, 12, 0, 0, tzinfo=et)
    delay = seconds_until_midnight_et(now=noon_et)
    assert abs(delay - 43200) < 2


def testseconds_until_midnight_et_from_11pm():
    from services.scorer.context_builder import seconds_until_midnight_et
    import zoneinfo
    et = zoneinfo.ZoneInfo("America/New_York")
    # 23:00 ET → midnight is 1h away = 3600s
    late_et = datetime(2026, 5, 16, 23, 0, 0, tzinfo=et)
    delay = seconds_until_midnight_et(now=late_et)
    assert abs(delay - 3600) < 2


def testseconds_until_midnight_et_never_negative():
    from services.scorer.context_builder import seconds_until_midnight_et
    delay = seconds_until_midnight_et()
    assert delay > 0


# ---------------------------------------------------------------------------
# build_macro_context
# ---------------------------------------------------------------------------

def _fake_client_with_text(text: str):
    block = MagicMock()
    block.text = text
    response = MagicMock()
    response.content = [block]
    client = MagicMock()
    client.messages.create.return_value = response
    return client


def _fake_client_no_text():
    block = MagicMock(spec=[])  # no .text attribute
    response = MagicMock()
    response.content = [block]
    client = MagicMock()
    client.messages.create.return_value = response
    return client


def test_build_macro_context_returns_text():
    from services.scorer.context_builder import build_macro_context
    client = _fake_client_with_text("**US Macro Context — 2026-05-16**\n\nFed holds.")
    result = build_macro_context(client, "claude-sonnet-4-6", "2026-05-16")
    assert "US Macro Context" in result
    assert "Fed holds" in result


def test_build_macro_context_calls_web_search_tool():
    from services.scorer.context_builder import build_macro_context
    client = _fake_client_with_text("summary")
    build_macro_context(client, "claude-sonnet-4-6", "2026-05-16")
    call_kwargs = client.messages.create.call_args[1]
    tools = call_kwargs["tools"]
    assert any(t.get("type") == "web_search_20250305" for t in tools)


def test_build_macro_context_raises_on_empty_response():
    from services.scorer.context_builder import build_macro_context
    import pytest
    client = _fake_client_no_text()
    with pytest.raises(ValueError, match="no text"):
        build_macro_context(client, "claude-sonnet-4-6", "2026-05-16")


def test_build_macro_context_injects_date_into_prompt():
    from services.scorer.context_builder import build_macro_context
    client = _fake_client_with_text("summary")
    build_macro_context(client, "claude-sonnet-4-6", "2026-05-16")
    call_kwargs = client.messages.create.call_args[1]
    messages = call_kwargs["messages"]
    user_content = messages[0]["content"]
    assert "2026-05-16" in user_content
