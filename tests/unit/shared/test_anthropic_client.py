"""Unit tests for the Anthropic client wrapper.

Strategy: inject a fake `client` object that mimics the anthropic SDK's
`messages.create()` interface. Each test sets up the fake to behave a specific
way (success, throttle, timeout, ...) and asserts the wrapper's response.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import anthropic

from services.shared.anthropic_client import (
    score_event,
    ScorerError,
    SYSTEM_PROMPT_CACHE_BLOCK,
)
from services.shared.models import NormalizedEvent


FIXTURE_RESPONSE = json.loads(
    (Path(__file__).parents[2] / "fixtures" / "anthropic_score_response.json").read_text()
)


def _normalized_event() -> NormalizedEvent:
    return NormalizedEvent(
        event_id="evt-1",
        source="cnbc_rss",
        ts_source=datetime(2026, 5, 4, 14, 0, tzinfo=timezone.utc),
        ts_ingested=datetime(2026, 5, 4, 14, 0, 5, tzinfo=timezone.utc),
        headline="Powell signals dovish pivot at Brookings",
        body="The Fed Chair indicated a willingness to ease policy if disinflation persists.",
        url="https://example.com/x",
        metadata={},
    )


def _fake_response_obj():
    """Build an object that quacks like an anthropic Message response."""
    block = MagicMock()
    block.type = "tool_use"
    block.input = FIXTURE_RESPONSE["content"][0]["input"]
    response = MagicMock()
    response.content = [block]
    return response


# --- success path ----------------------------------------------------------

def test_success_returns_scored_event():
    client = MagicMock()
    client.messages.create.return_value = _fake_response_obj()
    ev = _normalized_event()

    scored = score_event(client, normalized_event=ev, model="claude-haiku-4-5")

    assert scored.event_id == "evt-1"
    assert scored.score == 7
    assert scored.direction == "rates_lower"
    assert scored.confidence == 0.72
    assert "Powell" in scored.reasoning
    assert scored.model == "claude-haiku-4-5"
    assert scored.scored_at.tzinfo is not None


def test_success_uses_prompt_caching_block():
    client = MagicMock()
    client.messages.create.return_value = _fake_response_obj()
    ev = _normalized_event()

    score_event(client, normalized_event=ev, model="claude-haiku-4-5")

    args = client.messages.create.call_args
    system = args.kwargs["system"]
    # System prompt is sent as a list of blocks with cache_control on the prompt.
    assert isinstance(system, list)
    assert system[0]["type"] == "text"
    assert system[0]["cache_control"] == {"type": "ephemeral"}
    # Tool choice forces the score_event tool.
    assert args.kwargs["tool_choice"] == {"type": "tool", "name": "score_event"}
    assert args.kwargs["temperature"] == 0.0
    assert args.kwargs["timeout"] == 30


# --- failure paths ---------------------------------------------------------

def _rate_limit_err():
    return anthropic.RateLimitError("429", response=MagicMock(status_code=429), body=None)


def _api_status_err(status_code):
    return anthropic.APIStatusError(
        f"{status_code}", response=MagicMock(status_code=status_code), body=None
    )


def _auth_err():
    return anthropic.AuthenticationError("401", response=MagicMock(status_code=401), body=None)


def test_throttle_retries_three_times_then_dlq(monkeypatch):
    sleeps = []
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: sleeps.append(s))

    client = MagicMock()
    client.messages.create.side_effect = [_rate_limit_err()] * 4

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_throttle"
    assert exc_info.value.retry_count == 3
    assert sleeps == [1, 4, 16]


def test_5xx_retries_three_times_then_dlq(monkeypatch):
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    client = MagicMock()
    client.messages.create.side_effect = [_api_status_err(503)] * 4

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_5xx"


def test_auth_error_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _auth_err()

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_auth"
    assert client.messages.create.call_count == 1


def test_timeout_retries_once_then_dlq(monkeypatch):
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    client = MagicMock()
    client.messages.create.side_effect = [
        anthropic.APITimeoutError(request=MagicMock()),
        anthropic.APITimeoutError(request=MagicMock()),
    ]

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_timeout"
    assert client.messages.create.call_count == 2  # 1 initial + 1 retry


def test_malformed_response_retries_once_then_dlq(monkeypatch):
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    bad = MagicMock()
    bad.content = []  # no tool_use block
    client = MagicMock()
    client.messages.create.return_value = bad

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_schema_violation"
    assert client.messages.create.call_count == 2


def test_unknown_exception_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = OSError("network gone")

    with pytest.raises(ScorerError) as exc_info:
        score_event(client, normalized_event=_normalized_event(), model="m")

    assert exc_info.value.stage == "scorer_unknown"
    assert client.messages.create.call_count == 1


def test_recovery_after_one_throttle(monkeypatch):
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    client = MagicMock()
    client.messages.create.side_effect = [_rate_limit_err(), _fake_response_obj()]

    scored = score_event(client, normalized_event=_normalized_event(), model="m")

    assert scored.score == 7
    assert client.messages.create.call_count == 2


def test_system_prompt_cache_block_is_a_constant():
    """The wrapper exposes the cache block so tests can verify it without re-deriving."""
    assert SYSTEM_PROMPT_CACHE_BLOCK[0]["type"] == "text"
    assert SYSTEM_PROMPT_CACHE_BLOCK[0]["cache_control"] == {"type": "ephemeral"}
    assert "interest rates" in SYSTEM_PROMPT_CACHE_BLOCK[0]["text"].lower()
