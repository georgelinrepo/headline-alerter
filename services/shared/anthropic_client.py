"""Wraps the anthropic SDK with prompt caching, retries, timeout, and typed errors.

The scorer's main loop calls `score_event(client, normalized_event=ev, model=...)`
and either gets a `ScoredEvent` back or catches `ScorerError` (whose `.stage`
attribute drives DLQ routing).
"""
from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Any

import anthropic

from .models import NormalizedEvent, ScoredEvent
from .scorer_prompts import SYSTEM_PROMPT, SCORE_EVENT_TOOL


SYSTEM_PROMPT_CACHE_BLOCK: list[dict[str, Any]] = [
    {
        "type": "text",
        "text": SYSTEM_PROMPT,
        "cache_control": {"type": "ephemeral"},
    }
]

_BACKOFF_DELAYS_SECONDS = [1, 4, 16]
_DEFAULT_TIMEOUT_SECONDS = 30


class ScorerError(Exception):
    """Raised when the Anthropic call fails terminally. `stage` drives DLQ routing."""

    def __init__(self, stage: str, original: BaseException | None = None, retry_count: int = 0):
        self.stage = stage
        self.original = original
        self.retry_count = retry_count
        super().__init__(f"{stage}: {type(original).__name__ if original else ''}: {original}")


def _build_user_message(ev: NormalizedEvent) -> str:
    body = (ev.body or "")[:5000]
    return (
        f"Source: {ev.source}\n"
        f"Published: {ev.ts_source.isoformat()}\n"
        f"Headline: {ev.headline}\n\n"
        f"Body:\n{body}"
    )


def _extract_tool_use(response) -> dict[str, Any]:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use":
            return block.input
    raise ValueError("response.content has no tool_use block")


def _validate_score_data(data: dict[str, Any]) -> None:
    required = {"score", "direction", "confidence", "reasoning"}
    missing = required - set(data.keys())
    if missing:
        raise ValueError(f"missing tool_use input fields: {missing}")
    if not (0 <= int(data["score"]) <= 10):
        raise ValueError(f"score out of range: {data['score']}")
    if data["direction"] not in {"rates_higher", "rates_lower", "neutral", "unclear"}:
        raise ValueError(f"invalid direction: {data['direction']}")
    confidence = float(data["confidence"])
    if not (0.0 <= confidence <= 1.0):
        raise ValueError(f"confidence out of range: {data['confidence']}")


def score_event(
    client,
    *,
    normalized_event: NormalizedEvent,
    model: str,
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
) -> ScoredEvent:
    """Call Anthropic to score the event. Retries transient failures.

    On terminal failure raises ScorerError with the `stage` set per spec §6.1.
    """
    user_msg = _build_user_message(normalized_event)
    transient_attempt = 0  # for throttle/5xx (3 retries)
    timeout_attempt = 0    # for timeout (1 retry)
    schema_attempt = 0     # for malformed response (1 retry)

    while True:
        try:
            response = client.messages.create(
                model=model,
                max_tokens=500,
                system=SYSTEM_PROMPT_CACHE_BLOCK,
                messages=[{"role": "user", "content": user_msg}],
                tools=[SCORE_EVENT_TOOL],
                tool_choice={"type": "tool", "name": "score_event"},
                temperature=0.0,
                timeout=timeout_seconds,
            )
        except anthropic.RateLimitError as e:
            if transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                transient_attempt += 1
                continue
            raise ScorerError("scorer_throttle", e, retry_count=transient_attempt)
        except anthropic.AuthenticationError as e:
            raise ScorerError("scorer_auth", e, retry_count=0)
        except anthropic.APITimeoutError as e:
            if timeout_attempt < 1:
                timeout_attempt += 1
                continue
            raise ScorerError("scorer_timeout", e, retry_count=timeout_attempt)
        except anthropic.APIStatusError as e:
            status = getattr(getattr(e, "response", None), "status_code", None) or 0
            if 500 <= status < 600 and transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                transient_attempt += 1
                continue
            raise ScorerError("scorer_5xx", e, retry_count=transient_attempt)
        except Exception as e:  # pragma: no cover - safety net
            raise ScorerError("scorer_unknown", e, retry_count=0)

        try:
            data = _extract_tool_use(response)
            _validate_score_data(data)
        except (KeyError, ValueError, TypeError) as e:
            if schema_attempt < 1:
                schema_attempt += 1
                continue
            raise ScorerError("scorer_schema_violation", e, retry_count=schema_attempt)

        return ScoredEvent(
            event_id=normalized_event.event_id,
            score=int(data["score"]),
            direction=data["direction"],
            confidence=float(data["confidence"]),
            reasoning=str(data["reasoning"])[:1000],
            model=model,
            scored_at=datetime.now(timezone.utc),
        )
