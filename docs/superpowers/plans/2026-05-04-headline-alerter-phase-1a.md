# Headline Alerter — Phase 1a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first vertical slice of the headline-alerter pipeline — a CNBC RSS ingestor and an Anthropic-backed scorer that together produce real scored events end-to-end into Postgres.

**Architecture:** Two new Compose services (`ingestor-cnbc`, `scorer`) on the existing Phase 0 Kafka + Postgres stack. Shared library gains an `Ingestor` ABC, a DLQ helper, and an Anthropic client wrapper (with prompt caching, retries, 30s timeout). The scorer reads `events.normalized`, calls Claude Haiku 4.5 with a forced tool-use schema, and upserts `events_archive`. Failed scoring calls are routed to `events.dlq` so they cannot stall the pipeline.

**Tech Stack:** Python 3.12, `feedparser`, `confluent-kafka-python`, `psycopg[binary]`, `anthropic` (≥0.39, with prompt caching), `structlog`, Docker Compose v2 (existing). Tests use `pytest` + `respx` (HTTP mocking) + an injected fake Anthropic client.

**Spec:** [`docs/superpowers/specs/2026-05-04-headline-alerter-phase-1a-design.md`](../specs/2026-05-04-headline-alerter-phase-1a-design.md).

**Working directory for all commands:** `C:\Projects\headline-alerter\`. All `pytest` / `python` commands assume the venv is activated: `source .venv/Scripts/activate` (Git Bash) or `.\.venv\Scripts\Activate.ps1` (PowerShell).

**Definition of done for Phase 1a** (all 8 acceptance criteria from spec §9):
1. `docker compose up -d` brings `ingestor-cnbc` + `scorer` up cleanly alongside Phase 0 services
2. `events_archive` rows with `source='cnbc_rss'` appear within ~1 minute
3. Those rows transition to `status='scored'` within ~5s p95 of ingestion (measured via `latency_ms` log field)
4. `python tools/tail.py` shows live events
5. A synthetic broken event lands on `events.dlq` without crashing the scorer
6. `python tools/scorer_smoke.py` passes
7. `pytest -v` passes (unit + integration)
8. `docker compose restart` produces no duplicate rows; scorer resumes from last offset

---

## File Structure (created in this plan)

```
headline-alerter/
├── docker-compose.yml                            # Modified: Task 9
├── .env.example                                  # Modified: Task 1
├── services/
│   ├── shared/
│   │   ├── dlq.py                                # Task 2
│   │   ├── anthropic_client.py                   # Task 3
│   │   └── ingestor_base.py                      # Task 5
│   ├── ingestors/
│   │   ├── Dockerfile                            # Task 6
│   │   └── cnbc_rss/
│   │       ├── __init__.py                       # Task 6
│   │       └── main.py                           # Task 6
│   └── scorer/
│       ├── __init__.py                           # Task 7
│       ├── Dockerfile                            # Task 7
│       ├── main.py                               # Task 7
│       └── prompts.py                            # Task 4
├── tools/
│   ├── tail.py                                   # Task 10
│   └── scorer_smoke.py                           # Task 11
└── tests/
    ├── unit/
    │   ├── shared/
    │   │   ├── test_dlq.py                       # Task 2
    │   │   ├── test_anthropic_client.py          # Task 3
    │   │   └── test_ingestor_base.py             # Task 5
    │   ├── ingestors/
    │   │   ├── __init__.py                       # Task 6
    │   │   └── test_cnbc_rss.py                  # Task 6
    │   └── scorer/
    │       ├── __init__.py                       # Task 4
    │       ├── test_prompts.py                   # Task 4
    │       └── test_main.py                      # Task 7
    ├── integration/
    │   └── test_phase1a_e2e.py                   # Task 12
    └── fixtures/
        ├── __init__.py                           # Task 6
        ├── cnbc_sample.xml                       # Task 6
        └── anthropic_score_response.json         # Task 3
```

---

## Task 1: Environment + dependency setup

Phase 0 already pulled in `anthropic`, `feedparser`, `respx`. This task captures the new Phase 1a env vars in `.env.example` and confirms `pip install -e .[dev]` is up to date.

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Update `.env.example`**

Replace the entire contents of `.env.example` with:

```
# Postgres
POSTGRES_PASSWORD=changeme

# Anthropic (Phase 1a+)
ANTHROPIC_API_KEY=

# Scorer (Phase 1a+)
SCORER_MODEL=claude-haiku-4-5
SCORER_TIMEOUT_SECONDS=30

# CNBC RSS ingestor (Phase 1a+)
CNBC_RSS_URLS=https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664,https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258,https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135
POLL_INTERVAL_SECONDS=60

# Twilio (Phase 1b)
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_FROM=
ALERT_RECIPIENT=
ALERT_CHANNEL=sms

# Alert thresholds (Phase 1b)
ALERT_THRESHOLD=7
MIN_CONFIDENCE=0.6

# X API (Phase 4+)
X_API_KEY=
```

- [ ] **Step 2: Update local `.env` to match**

Run:
```bash
diff .env .env.example
```

Bring `.env` in sync — at minimum add the `ANTHROPIC_API_KEY=<your key>`, `SCORER_MODEL=claude-haiku-4-5`, `SCORER_TIMEOUT_SECONDS=30`, `CNBC_RSS_URLS=...` (paste the same default), `POLL_INTERVAL_SECONDS=60` lines.

- [ ] **Step 3: Confirm dev deps still install cleanly**

Run:
```bash
source .venv/Scripts/activate
pip install -e ".[dev]"
```

Expected: `Successfully installed ...` (no errors). All required deps already in `pyproject.toml` from Phase 0.

- [ ] **Step 4: Confirm full Phase 0 test suite still passes**

Run:
```bash
docker compose up -d
pytest -v
```

Expected: 10 passed (the Phase 0 baseline). If anything fails, stop and investigate before proceeding.

- [ ] **Step 5: Commit**

```bash
git add .env.example
git commit -m "chore(phase-1a): env vars for anthropic + cnbc rss + scorer"
```

---

## Task 2: DLQ helper (services/shared/dlq.py)

Build the shared utility every later component will use to route failed events to `events.dlq`.

**Files:**
- Create: `services/shared/dlq.py`
- Create: `tests/unit/shared/test_dlq.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/shared/test_dlq.py`:

```python
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
```

- [ ] **Step 2: Run the failing test**

Run:
```bash
pytest tests/unit/shared/test_dlq.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.shared.dlq'`.

- [ ] **Step 3: Implement `services/shared/dlq.py`**

```python
"""Dead-letter-queue helper. Every service uses the same envelope format.

Envelope shape:
    {
      "stage":          "<stage tag, e.g. scorer_throttle>",
      "service":        "<service name, e.g. scorer>",
      "ts_dlq":         "<ISO-8601 UTC timestamp>",
      "error":          "<ExceptionClass: message>",
      "retry_count":    <int>,
      "original_event": <dict — NormalizedEvent or raw RSS item>,
    }
"""
import json
from datetime import datetime, timezone
from typing import Any
from confluent_kafka import Producer


def build_envelope(
    *,
    stage: str,
    service: str,
    error: BaseException,
    original_event: dict[str, Any] | None,
    retry_count: int = 0,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "service": service,
        "ts_dlq": datetime.now(timezone.utc).isoformat(),
        "error": f"{type(error).__name__}: {error}",
        "retry_count": retry_count,
        "original_event": original_event or {},
    }


def send_to_dlq(
    producer: Producer,
    *,
    stage: str,
    service: str,
    error: BaseException,
    original_event: dict[str, Any] | None,
    retry_count: int = 0,
) -> None:
    envelope = build_envelope(
        stage=stage,
        service=service,
        error=error,
        original_event=original_event,
        retry_count=retry_count,
    )
    key = (original_event or {}).get("event_id", "unknown")
    producer.produce(
        topic="events.dlq",
        key=str(key).encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/shared/test_dlq.py -v
```

Expected: 4 PASSED.

- [ ] **Step 5: Commit**

```bash
git add services/shared/dlq.py tests/unit/shared/test_dlq.py
git commit -m "feat(shared): dlq helper with typed envelope"
```

---

## Task 3: Anthropic client wrapper (services/shared/anthropic_client.py)

Build the wrapper that handles prompt caching, retry/backoff, the 30s timeout, and converts Anthropic SDK exceptions into a typed `ScorerError` whose `stage` attribute drives DLQ routing.

**Files:**
- Create: `services/shared/anthropic_client.py`
- Create: `tests/unit/shared/test_anthropic_client.py`
- Create: `tests/fixtures/__init__.py` (empty marker)
- Create: `tests/fixtures/anthropic_score_response.json`

- [ ] **Step 1: Create test fixtures directory marker**

Run:
```bash
mkdir -p tests/fixtures
touch tests/fixtures/__init__.py
```

- [ ] **Step 2: Create `tests/fixtures/anthropic_score_response.json`**

A captured Anthropic tool-use response we can replay in tests:

```json
{
  "id": "msg_test_01",
  "type": "message",
  "role": "assistant",
  "model": "claude-haiku-4-5",
  "content": [
    {
      "type": "tool_use",
      "id": "toolu_test_01",
      "name": "score_event",
      "input": {
        "score": 7,
        "direction": "rates_lower",
        "confidence": 0.72,
        "reasoning": "Powell's tone notably more dovish than recent statements; market is likely to price in a near-term cut."
      }
    }
  ],
  "stop_reason": "tool_use",
  "usage": {
    "input_tokens": 250,
    "cache_creation_input_tokens": 0,
    "cache_read_input_tokens": 0,
    "output_tokens": 80
  }
}
```

- [ ] **Step 3: Write the failing test**

Create `tests/unit/shared/test_anthropic_client.py`:

```python
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
    # The SDK constructor signature is RateLimitError(message, *, response, body)
    # but we don't need a real httpx Response — MagicMock works.
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
```

- [ ] **Step 4: Run the failing test**

Run:
```bash
pytest tests/unit/shared/test_anthropic_client.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 5: Implement `services/shared/anthropic_client.py`**

```python
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
                system=[
                    {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
                ],
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
```

Note: this module imports `SYSTEM_PROMPT` and `SCORE_EVENT_TOOL` from `services.shared.scorer_prompts`. We will create that module in Task 4 — but the import needs to resolve at test-collection time. To avoid a chicken-and-egg, create a minimal stub now that Task 4 will replace.

- [ ] **Step 6: Create stub `services/shared/scorer_prompts.py`**

```python
"""STUB — replaced by full implementation in Task 4."""
SYSTEM_PROMPT = "Score events for impact on US interest rates markets."
SCORE_EVENT_TOOL = {
    "name": "score_event",
    "description": "Score the rates-market relevance of an event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer"},
            "direction": {"type": "string"},
            "confidence": {"type": "number"},
            "reasoning": {"type": "string"},
        },
        "required": ["score", "direction", "confidence", "reasoning"],
    },
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/shared/test_anthropic_client.py -v
```

Expected: 9 PASSED.

- [ ] **Step 8: Commit**

```bash
git add services/shared/anthropic_client.py services/shared/scorer_prompts.py \
        tests/fixtures/__init__.py tests/fixtures/anthropic_score_response.json \
        tests/unit/shared/test_anthropic_client.py
git commit -m "feat(shared): anthropic client wrapper with caching, retries, typed errors"
```

---

## Task 4: Scorer prompts (services/shared/scorer_prompts.py)

Replace the Task 3 stub with the full system prompt + forced-tool schema lifted verbatim from parent spec § 5.

**Files:**
- Modify: `services/shared/scorer_prompts.py`
- Create: `tests/unit/scorer/__init__.py`
- Create: `tests/unit/scorer/test_prompts.py`

- [ ] **Step 1: Create the test directory marker**

Run:
```bash
mkdir -p tests/unit/scorer
touch tests/unit/scorer/__init__.py
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/scorer/test_prompts.py`:

```python
"""Sanity tests for the system prompt + forced-tool schema."""
from services.shared.scorer_prompts import SYSTEM_PROMPT, SCORE_EVENT_TOOL


def test_system_prompt_mentions_rates_terminology():
    text = SYSTEM_PROMPT.lower()
    for needed in ["sofr", "treasury", "fomc", "rates_higher", "rates_lower"]:
        assert needed in text, f"system prompt missing '{needed}'"


def test_system_prompt_includes_full_rubric():
    """Every score 0-10 must be explained somewhere in the rubric."""
    for n in range(0, 11):
        assert str(n) in SYSTEM_PROMPT, f"rubric missing score {n}"


def test_tool_schema_required_fields():
    schema = SCORE_EVENT_TOOL["input_schema"]
    assert schema["type"] == "object"
    assert set(schema["required"]) == {"score", "direction", "confidence", "reasoning"}


def test_tool_schema_score_bounds():
    score = SCORE_EVENT_TOOL["input_schema"]["properties"]["score"]
    assert score["type"] == "integer"
    assert score["minimum"] == 0
    assert score["maximum"] == 10


def test_tool_schema_direction_enum():
    direction = SCORE_EVENT_TOOL["input_schema"]["properties"]["direction"]
    assert set(direction["enum"]) == {"rates_higher", "rates_lower", "neutral", "unclear"}


def test_tool_schema_confidence_bounds():
    confidence = SCORE_EVENT_TOOL["input_schema"]["properties"]["confidence"]
    assert confidence["type"] == "number"
    assert confidence["minimum"] == 0
    assert confidence["maximum"] == 1


def test_tool_name_is_stable():
    assert SCORE_EVENT_TOOL["name"] == "score_event"
```

- [ ] **Step 3: Run the failing tests**

Run:
```bash
pytest tests/unit/scorer/test_prompts.py -v
```

Expected: most FAIL — the Task 3 stub is missing rubric details, enum, bounds, etc.

- [ ] **Step 4: Replace `services/shared/scorer_prompts.py` with the full implementation**

```python
"""Scorer system prompt + forced-tool schema. Lifted verbatim from parent spec §5."""

SYSTEM_PROMPT = """\
You are scoring a news or social-media event for a US interest rates
trader who watches SOFR futures, Treasury futures (ZN, ZF, ZT, ZB),
Fed Funds futures (ZQ), and Treasury yields (2y, 5y, 10y, 30y).

Decide: would this event likely cause a sizable move in US rates markets
within the next ~2 hours?

Scoring rubric (0-10):
  0-2 = noise / irrelevant to rates
  3-4 = tangential / 2nd-order relevance
  5-6 = relevant but unlikely to move things on its own
  7   = likely to move rates a few basis points
  8   = high confidence of meaningful (>3bp) move on at least one tenor
  9   = strong move expected (>5bp), high confidence
  10  = exceptional / regime-shifting (FOMC surprise, Fed leak, major
        geopolitical, surprise central-bank action)

Direction:
  rates_higher  - yields up, futures down (hawkish, growth-up, supply-up)
  rates_lower   - yields down, futures up (dovish, risk-off, growth-down)
  neutral       - relevant but not directional
  unclear       - relevant but you can't tell which way

Confidence (0.0-1.0): how sure are you of the score and direction?

Reasoning: 2-4 sentences explaining the assessment. Be specific about
why this would (or wouldn't) move rates and which tenor is most affected.

Return your assessment via the score_event tool. Do not respond in any
other format.
"""

SCORE_EVENT_TOOL = {
    "name": "score_event",
    "description": "Score the rates-market relevance of an event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 10},
            "direction": {
                "type": "string",
                "enum": ["rates_higher", "rates_lower", "neutral", "unclear"],
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reasoning": {"type": "string", "maxLength": 1000},
        },
        "required": ["score", "direction", "confidence", "reasoning"],
    },
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/scorer/test_prompts.py tests/unit/shared/test_anthropic_client.py -v
```

Expected: all 7 prompt tests + all 9 anthropic-client tests PASS.

- [ ] **Step 6: Commit**

```bash
git add services/shared/scorer_prompts.py tests/unit/scorer/__init__.py tests/unit/scorer/test_prompts.py
git commit -m "feat(scorer): full system prompt + forced score_event tool schema"
```

---

## Task 5: Ingestor base ABC (services/shared/ingestor_base.py)

Build the polling-loop base class that every ingestor subclasses. Subclasses provide `_fetch_raw_items()` and `_normalize_item()`; the base owns the loop, dedup, restart hydration, archival, Kafka produce, parse-error DLQ routing, and structured logging.

**Files:**
- Create: `services/shared/ingestor_base.py`
- Create: `tests/unit/shared/test_ingestor_base.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/shared/test_ingestor_base.py`:

```python
"""Unit tests for the Ingestor ABC.

We build a tiny FakeIngestor subclass and inject:
- a `_fetch_raw_items` that we control
- a fake Kafka producer (MagicMock)
- a fake `connect()` context manager (monkeypatched) backed by an in-memory store
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from services.shared.ingestor_base import Ingestor
from services.shared.models import NormalizedEvent


class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self.last_query = None
        self.last_params = None

    def execute(self, query, params=()):
        self.last_query = query
        self.last_params = params
        if "MAX(ts_source)" in query:
            source = params[0]
            rows = [r for r in self.store["events"] if r["source"] == source]
            self._result = [(max((r["ts_source"] for r in rows), default=None),)]
        elif "INSERT INTO events_archive" in query:
            row = {
                "id": params[0], "source": params[1],
                "ts_source": params[2], "ts_ingested": params[3],
                "headline": params[4], "body": params[5], "url": params[6],
                "metadata": params[7], "status": "received",
            }
            existing = next((r for r in self.store["events"] if r["id"] == row["id"]), None)
            if existing is None:
                self.store["events"].append(row)
            self._result = []
        else:
            self._result = []

    def fetchone(self):
        return self._result[0] if self._result else None

    def __enter__(self): return self
    def __exit__(self, *a): pass


class _FakeConn:
    def __init__(self, store):
        self.store = store
    def cursor(self):
        return _FakeCursor(self.store)
    def commit(self):
        pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


@pytest.fixture
def fake_db(monkeypatch):
    store = {"events": []}
    def _connect():
        return _FakeConn(store)
    # The base class uses `from .db import connect` — so we patch the symbol on the module.
    monkeypatch.setattr("services.shared.ingestor_base.connect", _connect)
    return store


class _FakeIngestor(Ingestor):
    source_name = "fake_src"
    poll_interval_seconds = 60

    def __init__(self, items_per_iteration: list[list[dict]], producer=None,
                 fetch_raises: list[Exception | None] | None = None):
        super().__init__(producer=producer or MagicMock())
        self._items_queue = list(items_per_iteration)
        self._raises = list(fetch_raises) if fetch_raises else []

    def _fetch_raw_items(self):
        if self._raises:
            err = self._raises.pop(0)
            if err is not None:
                raise err
        return self._items_queue.pop(0) if self._items_queue else []

    def _normalize_item(self, raw):
        if "bad" in raw:
            raise ValueError("malformed item")
        return NormalizedEvent(
            event_id=raw["id"],
            source=self.source_name,
            ts_source=datetime.fromisoformat(raw["ts"]),
            ts_ingested=datetime.now(timezone.utc),
            headline=raw["headline"],
            body=raw.get("body"),
            url=raw.get("url"),
            metadata={},
        )


def _item(eid, ts_iso, headline="h"):
    return {"id": eid, "ts": ts_iso, "headline": headline, "url": f"https://x/{eid}"}


# --- hydration ------------------------------------------------------------

def test_hydrate_uses_24h_fallback_when_table_empty(fake_db):
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(hours=23, minutes=59) < delta < timedelta(hours=24, minutes=1)


def test_hydrate_uses_db_max_when_recent(fake_db):
    fake_db["events"].append({
        "id": "x", "source": "fake_src",
        "ts_source": datetime.now(timezone.utc) - timedelta(hours=1),
    })
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(minutes=59) < delta < timedelta(minutes=61)


def test_hydrate_uses_24h_fallback_when_max_too_old(fake_db):
    fake_db["events"].append({
        "id": "x", "source": "fake_src",
        "ts_source": datetime.now(timezone.utc) - timedelta(days=5),
    })
    ing = _FakeIngestor([])
    ing.hydrate_last_ts()
    delta = datetime.now(timezone.utc) - ing._last_ts_source
    assert timedelta(hours=23, minutes=59) < delta < timedelta(hours=24, minutes=1)


# --- normal-path emission -------------------------------------------------

def test_emits_only_items_newer_than_last_ts(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        _item("old", (now - timedelta(hours=48)).isoformat()),     # filtered (>24h old)
        _item("recent", (now - timedelta(minutes=10)).isoformat()), # passes
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    emitted = ing.run_one_iteration()
    assert emitted == 1
    # Producer was called once with topic events.normalized
    assert ing.producer.produce.call_count == 1
    kwargs = ing.producer.produce.call_args.kwargs
    assert kwargs["topic"] == "events.normalized"
    payload = json.loads(kwargs["value"].decode())
    assert payload["event_id"] == "recent"


def test_archives_emitted_events(fake_db):
    now = datetime.now(timezone.utc)
    items = [_item("e1", (now - timedelta(minutes=5)).isoformat())]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    rows = [r for r in fake_db["events"] if r["id"] == "e1"]
    assert len(rows) == 1


def test_advances_last_ts_to_max_emitted(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        _item("a", (now - timedelta(minutes=5)).isoformat()),
        _item("b", (now - timedelta(minutes=2)).isoformat()),
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    delta = now - ing._last_ts_source
    assert delta < timedelta(minutes=3)


# --- error paths ----------------------------------------------------------

def test_fetch_failure_doubles_backoff(fake_db):
    ing = _FakeIngestor([], fetch_raises=[RuntimeError("rss 503")])
    ing.hydrate_last_ts()
    assert ing._backoff_seconds == 60
    ing.run_one_iteration()
    assert ing._backoff_seconds == 120


def test_fetch_failure_caps_backoff_at_600(fake_db):
    ing = _FakeIngestor([], fetch_raises=[RuntimeError("e")] * 12)
    ing.hydrate_last_ts()
    for _ in range(12):
        ing.run_one_iteration()
    assert ing._backoff_seconds == 600


def test_fetch_success_resets_backoff(fake_db):
    now = datetime.now(timezone.utc)
    ing = _FakeIngestor(
        [[], [_item("ok", (now - timedelta(minutes=1)).isoformat())]],
        fetch_raises=[RuntimeError("first fails"), None],
    )
    ing.hydrate_last_ts()
    ing.run_one_iteration()
    assert ing._backoff_seconds == 120
    ing.run_one_iteration()
    assert ing._backoff_seconds == 60


def test_normalize_failure_routes_to_dlq_and_continues(fake_db):
    now = datetime.now(timezone.utc)
    items = [
        {"id": "bad", "bad": True},
        _item("good", (now - timedelta(minutes=1)).isoformat()),
    ]
    ing = _FakeIngestor([items])
    ing.hydrate_last_ts()
    emitted = ing.run_one_iteration()
    # 'good' was emitted; 'bad' went to DLQ.
    assert emitted == 1
    topics = [c.kwargs["topic"] for c in ing.producer.produce.call_args_list]
    assert "events.dlq" in topics
    assert "events.normalized" in topics
```

- [ ] **Step 2: Run the failing test**

Run:
```bash
pytest tests/unit/shared/test_ingestor_base.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `services/shared/ingestor_base.py`**

```python
"""Ingestor base class shared by every source.

Subclass contract:
- Set the class attribute `source_name`.
- Override `_fetch_raw_items() -> list[dict]`. Should not raise on transient
  network errors — log and return []. Raise only on truly unexpected failures
  (these will trigger the backoff in `run_one_iteration()`).
- Override `_normalize_item(raw: dict) -> NormalizedEvent`. Raise on parse
  errors; the base routes them to events.dlq.

The base owns: the polling loop, last-ts hydration on startup, archival
into events_archive, Kafka produce to events.normalized, DLQ routing for
parse errors, and exponential fetch backoff.
"""
from __future__ import annotations
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Producer

from .db import connect
from .dlq import send_to_dlq
from .kafka_client import flush, make_producer, produce
from .logging import get_logger
from .models import NormalizedEvent


_BACKOFF_CAP_SECONDS = 600


class Ingestor(ABC):
    source_name: str  # override
    poll_interval_seconds: int = 60

    def __init__(self, *, producer: Producer | None = None) -> None:
        self.log = get_logger().bind(source=self.source_name)
        self.producer = producer if producer is not None else make_producer()
        self._backoff_seconds = self.poll_interval_seconds
        self._last_ts_source: datetime | None = None

    # ---- subclass contract ----

    @abstractmethod
    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        """Return zero or more raw items (any dict shape; passed to _normalize_item)."""

    @abstractmethod
    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        """Convert one raw item to a NormalizedEvent. Raise on parse errors."""

    # ---- public lifecycle ----

    def hydrate_last_ts(self) -> None:
        """Load `MAX(ts_source) FROM events_archive WHERE source = ?`.
        Falls back to (now - 24h) if no rows or the max is older than 24h."""
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(ts_source) FROM events_archive WHERE source = %s",
                    (self.source_name,),
                )
                row = cur.fetchone()
        max_ts = row[0] if row else None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        if max_ts is None or max_ts < cutoff:
            self._last_ts_source = cutoff
            self.log.info("hydrated last_ts (24h fallback)",
                          last_ts=self._last_ts_source.isoformat())
        else:
            self._last_ts_source = max_ts
            self.log.info("hydrated last_ts", last_ts=self._last_ts_source.isoformat())

    def run_one_iteration(self) -> int:
        """One poll cycle. Returns the number of events emitted."""
        try:
            raw_items = self._fetch_raw_items()
        except Exception as e:
            self._backoff_seconds = min(self._backoff_seconds * 2, _BACKOFF_CAP_SECONDS)
            self.log.warning("fetch failed; backing off",
                             error=str(e), next_interval_seconds=self._backoff_seconds)
            return 0

        # Reset backoff on successful fetch.
        self._backoff_seconds = self.poll_interval_seconds

        emitted = 0
        for raw in raw_items:
            try:
                event = self._normalize_item(raw)
            except Exception as e:
                self.log.warning("ingest_parse failed", error=str(e), raw=str(raw)[:300])
                send_to_dlq(
                    self.producer,
                    stage="ingest_parse",
                    service=f"ingestor-{self.source_name}",
                    error=e,
                    original_event=raw if isinstance(raw, dict) else {"raw": str(raw)},
                )
                continue

            if self._last_ts_source is not None and event.ts_source <= self._last_ts_source:
                continue  # already seen

            self._archive(event)
            produce(self.producer, "events.normalized",
                    key=event.source, payload=event.to_dict())
            self._last_ts_source = (
                max(self._last_ts_source, event.ts_source)
                if self._last_ts_source else event.ts_source
            )
            emitted += 1
            self.log.info("emitted", event_id=event.event_id, headline=event.headline[:80])

        flush(self.producer)
        return emitted

    def run(self) -> None:
        """Forever loop. Hydrates last_ts on first call."""
        self.hydrate_last_ts()
        while True:
            self.run_one_iteration()
            time.sleep(self._backoff_seconds)

    # ---- helpers ----

    def _archive(self, ev: NormalizedEvent) -> None:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO events_archive
                      (id, source, ts_source, ts_ingested, status, headline, body, url, metadata)
                    VALUES (%s, %s, %s, %s, 'received', %s, %s, %s, %s::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        ev.event_id, ev.source, ev.ts_source, ev.ts_ingested,
                        ev.headline, ev.body, ev.url, json.dumps(ev.metadata),
                    ),
                )
            conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/shared/test_ingestor_base.py -v
```

Expected: 9 PASSED.

- [ ] **Step 5: Commit**

```bash
git add services/shared/ingestor_base.py tests/unit/shared/test_ingestor_base.py
git commit -m "feat(shared): ingestor ABC with polling loop, hydration, DLQ on parse errors"
```

---

## Task 6: CNBC ingestor + Dockerfile (services/ingestors/cnbc_rss/main.py)

Concrete subclass of `Ingestor` that polls the CNBC RSS feed URLs. Plus the shared Dockerfile that all ingestors will use (Phase 2's BLS will reuse it unmodified).

**Files:**
- Create: `services/ingestors/__init__.py` (empty)
- Create: `services/ingestors/Dockerfile`
- Create: `services/ingestors/cnbc_rss/__init__.py` (empty)
- Create: `services/ingestors/cnbc_rss/main.py`
- Create: `tests/unit/ingestors/__init__.py`
- Create: `tests/unit/ingestors/test_cnbc_rss.py`
- Create: `tests/fixtures/cnbc_sample.xml`

- [ ] **Step 1: Create directory markers**

Run:
```bash
mkdir -p services/ingestors/cnbc_rss tests/unit/ingestors
touch services/ingestors/__init__.py services/ingestors/cnbc_rss/__init__.py tests/unit/ingestors/__init__.py
```

- [ ] **Step 2: Create the test fixture `tests/fixtures/cnbc_sample.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>CNBC Economy</title>
    <link>https://www.cnbc.com/economy/</link>
    <description>Test fixture</description>
    <item>
      <link>https://www.cnbc.com/2026/05/04/powell-dovish-pivot.html</link>
      <guid isPermaLink="false">108300001</guid>
      <title>Powell signals dovish pivot at Brookings</title>
      <description><![CDATA[The Fed Chair indicated a willingness to ease policy if disinflation persists.]]></description>
      <pubDate>Mon, 04 May 2026 14:32:00 GMT</pubDate>
    </item>
    <item>
      <link>https://www.cnbc.com/2026/05/04/cpi-print-cool.html</link>
      <guid isPermaLink="false">108300002</guid>
      <title>April CPI prints cooler than expected at 3.0% vs 3.2% est</title>
      <description><![CDATA[Headline inflation came in below the consensus, with core CPI also softer at 3.4% vs 3.5% est.]]></description>
      <pubDate>Mon, 04 May 2026 12:30:00 GMT</pubDate>
    </item>
    <item>
      <link>https://www.cnbc.com/2026/05/04/btc-100k.html</link>
      <guid isPermaLink="false">108300003</guid>
      <title>Bitcoin tops $100k as crypto rallies</title>
      <description><![CDATA[Bitcoin reached a new high amid renewed institutional inflows.]]></description>
      <pubDate>Mon, 04 May 2026 11:15:00 GMT</pubDate>
    </item>
  </channel>
</rss>
```

- [ ] **Step 3: Write the failing test**

Create `tests/unit/ingestors/test_cnbc_rss.py`:

```python
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
    import pytest as _pt
    with _pt.raises(Exception):
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
```

- [ ] **Step 4: Run the failing test**

Run:
```bash
pytest tests/unit/ingestors/test_cnbc_rss.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 5: Implement `services/ingestors/cnbc_rss/main.py`**

```python
"""CNBC RSS ingestor.

Polls one or more CNBC RSS feed URLs, normalizes each <item> to a
NormalizedEvent, and emits it via the shared Ingestor base class.
"""
from __future__ import annotations
import hashlib
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import feedparser

from services.shared.ingestor_base import Ingestor
from services.shared.logging import configure_logging
from services.shared.models import NormalizedEvent


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _parsed_to_utc(struct_time) -> datetime:
    """feedparser gives published_parsed as a time.struct_time in UTC."""
    if struct_time is None:
        raise ValueError("missing pubDate")
    return datetime(*struct_time[:6], tzinfo=timezone.utc)


class CnbcIngestor(Ingestor):
    source_name = "cnbc_rss"

    def __init__(self, *, urls: list[str], producer=None,
                 poll_interval_seconds: int | None = None) -> None:
        if poll_interval_seconds is not None:
            self.poll_interval_seconds = poll_interval_seconds
        super().__init__(producer=producer)
        self.urls = urls

    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for url in self.urls:
            parsed = feedparser.parse(url)
            if parsed.bozo:
                self.log.warning("rss feed parse error", url=url,
                                 error=str(getattr(parsed, "bozo_exception", "")))
                continue
            for entry in parsed.entries:
                out.append({"entry": entry, "_feed_url": url})
        return out

    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        entry = raw["entry"]
        feed_url = raw.get("_feed_url", "")

        url = getattr(entry, "link", None) or entry.get("link") if hasattr(entry, "get") else None
        if not url:
            raise ValueError("entry has no link")
        title = getattr(entry, "title", None) or (entry.get("title") if hasattr(entry, "get") else None)
        if not title:
            raise ValueError("entry has no title")

        # feedparser parses pubDate to entry.published_parsed (struct_time, UTC).
        published_parsed = getattr(entry, "published_parsed", None) or (
            entry.get("published_parsed") if hasattr(entry, "get") else None
        )
        ts_source = _parsed_to_utc(published_parsed)

        body = (
            getattr(entry, "description", None)
            or (entry.get("description") if hasattr(entry, "get") else None)
            or getattr(entry, "summary", None)
        )
        raw_id = (
            getattr(entry, "id", None)
            or (entry.get("id") if hasattr(entry, "get") else None)
            or url
        )

        event_id = _sha256_hex(f"cnbc_rss|{url}|{ts_source.isoformat()}")
        return NormalizedEvent(
            event_id=event_id,
            source=self.source_name,
            ts_source=ts_source,
            ts_ingested=datetime.now(timezone.utc),
            headline=str(title),
            body=str(body) if body else None,
            url=url,
            metadata={"raw_id": str(raw_id), "feed_url": feed_url},
        )


def _urls_from_env() -> list[str]:
    raw = os.environ.get("CNBC_RSS_URLS", "").strip()
    if not raw:
        raise RuntimeError("CNBC_RSS_URLS env var is required")
    return [u.strip() for u in raw.split(",") if u.strip()]


def main() -> int:
    configure_logging("ingestor-cnbc")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    ing = CnbcIngestor(urls=_urls_from_env(), poll_interval_seconds=interval)
    ing.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: Create `services/ingestors/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "feedparser>=6.0.11"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# The ingestor module is selected at runtime via INGESTOR_SOURCE.
# In Phase 1a we only ship cnbc_rss; later phases add more.
CMD ["python", "-m", "services.ingestors.cnbc_rss.main"]
```

- [ ] **Step 7: Run unit tests to verify they pass**

Run:
```bash
pytest tests/unit/ingestors/test_cnbc_rss.py -v
```

Expected: 5 PASSED.

- [ ] **Step 8: Build the ingestor image to confirm the Dockerfile is valid**

Run:
```bash
docker build -f services/ingestors/Dockerfile -t headline-alerter-ingestor:phase-1a .
```

Expected: builds successfully, no errors.

- [ ] **Step 9: Commit**

```bash
git add services/ingestors/ tests/unit/ingestors/ tests/fixtures/cnbc_sample.xml
git commit -m "feat(ingestor): cnbc rss ingestor + shared dockerfile"
```

---

## Task 7: Scorer service (services/scorer/main.py + Dockerfile)

The scorer reads `events.normalized`, calls Anthropic via the Task 3 wrapper, produces the result to `events.scored`, and upserts `events_archive`. On terminal failure it routes to `events.dlq`.

To enable integration testing without hitting the real Anthropic API, the scorer reads an env var `SCORER_FAKE_RESPONSE_PATH` — when set, it loads a fake client that replays a fixture instead of calling Anthropic.

**Files:**
- Create: `services/scorer/__init__.py` (empty)
- Create: `services/scorer/main.py`
- Create: `services/scorer/Dockerfile`
- Create: `tests/unit/scorer/test_main.py`

- [ ] **Step 1: Create directory marker**

Run:
```bash
mkdir -p services/scorer
touch services/scorer/__init__.py
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/scorer/test_main.py`:

```python
"""Unit tests for the scorer's per-event processing function.

We test `process_one_event(event_dict, anthropic_client, producer, logger,
model)` directly. The function is the heart of the scorer; the main loop
just polls Kafka, calls process_one_event, and commits the offset.
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
    monkeypatch.setattr("services.scorer.main.upsert_archive_with_score", upsert_score)
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
```

- [ ] **Step 3: Run the failing tests**

Run:
```bash
pytest tests/unit/scorer/test_main.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 4: Implement `services/scorer/main.py`**

```python
"""Scorer service: consume events.normalized, call Anthropic, produce events.scored.

A failed scoring call is routed to events.dlq so a single bad event cannot
stall the pipeline. The Postgres row is also updated to status='failed'.
"""
from __future__ import annotations
import json
import os
import sys
import time
import uuid
from typing import Any

import anthropic

from services.shared.anthropic_client import ScorerError, score_event
from services.shared.db import connect
from services.shared.dlq import send_to_dlq
from services.shared.kafka_client import flush, make_consumer, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.models import NormalizedEvent


# ---- Postgres helpers -----------------------------------------------------

def upsert_archive_with_score(scored) -> None:
    """UPDATE events_archive with score fields and status='scored'."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status = 'scored',
                    ts_scored = %s,
                    score = %s,
                    direction = %s,
                    confidence = %s,
                    reasoning = %s,
                    model = %s
                WHERE id = %s
                """,
                (
                    scored.scored_at, scored.score, scored.direction,
                    scored.confidence, scored.reasoning, scored.model,
                    scored.event_id,
                ),
            )
        conn.commit()


def mark_archive_failed(event_id: str, error_msg: str) -> None:
    """Mark events_archive row as status='failed' (preserves the original payload)."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status = 'failed',
                    reasoning = %s
                WHERE id = %s
                """,
                (error_msg[:1000], event_id),
            )
        conn.commit()


# ---- per-event processing -------------------------------------------------

def process_one_event(event_dict: dict[str, Any], *, anthropic_client,
                      producer, log, model: str,
                      timeout_seconds: int = 30) -> None:
    """Score a single normalized event. Routes failures to DLQ."""
    event = NormalizedEvent.from_dict(event_dict)
    started = time.monotonic()
    try:
        scored = score_event(
            anthropic_client,
            normalized_event=event,
            model=model,
            timeout_seconds=timeout_seconds,
        )
    except ScorerError as e:
        latency_ms = int((time.monotonic() - started) * 1000)
        log.warning("score failed", event_id=event.event_id, stage=e.stage,
                    error=str(e.original), retry_count=e.retry_count,
                    latency_ms=latency_ms)
        send_to_dlq(producer, stage=e.stage, service="scorer",
                    error=e.original or e, original_event=event_dict,
                    retry_count=e.retry_count)
        flush(producer)
        mark_archive_failed(event.event_id, f"{e.stage}: {e.original}")
        return

    # success path
    upsert_archive_with_score(scored)
    produce(producer, "events.scored", key=scored.event_id, payload=scored.to_dict())
    flush(producer)
    latency_ms = int((time.monotonic() - started) * 1000)
    log.info("scored",
             event_id=scored.event_id, score=scored.score,
             direction=scored.direction, confidence=scored.confidence,
             latency_ms=latency_ms)


# ---- Anthropic client construction ---------------------------------------

class _FakeAnthropicClient:
    """Replays a captured Anthropic response from a JSON fixture.
    Activated by SCORER_FAKE_RESPONSE_PATH; used by integration tests."""
    def __init__(self, fixture_path: str, fail_mode: str | None = None):
        with open(fixture_path) as f:
            self._payload = json.load(f)
        self._fail_mode = fail_mode
        self.messages = self  # so .messages.create works

    def create(self, **kwargs):  # pragma: no cover - exercised via integration tests
        if self._fail_mode == "rate_limit":
            raise anthropic.RateLimitError(
                "forced 429", response=type("R", (), {"status_code": 429})(), body=None
            )
        # Build a Message-like object from the JSON payload.
        class _Block:
            def __init__(self, d): self.type = d["type"]; self.input = d.get("input", {})
        class _Resp:
            def __init__(self, d): self.content = [_Block(b) for b in d["content"]]
        return _Resp(self._payload)


def build_anthropic_client():
    fake_path = os.environ.get("SCORER_FAKE_RESPONSE_PATH")
    if fake_path:
        fail_mode = os.environ.get("SCORER_FAKE_FAIL_MODE")  # 'rate_limit' or unset
        return _FakeAnthropicClient(fake_path, fail_mode=fail_mode)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY env var is required")
    return anthropic.Anthropic(api_key=api_key)


# ---- main loop -----------------------------------------------------------

def _consumer_group_id() -> str:
    return os.environ.get("SCORER_GROUP_ID", "scorer-cg")


def main() -> int:
    configure_logging("scorer")
    log = get_logger()
    log.info("starting scorer")

    model = os.environ.get("SCORER_MODEL", "claude-haiku-4-5")
    timeout_s = int(os.environ.get("SCORER_TIMEOUT_SECONDS", "30"))

    client = build_anthropic_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.normalized"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error", error=str(msg.error()))
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error("payload decode failed", error=str(e))
                # Skip undecodeable bytes; commit so we don't loop forever.
                consumer.commit(message=msg, asynchronous=False)
                continue

            process_one_event(
                payload,
                anthropic_client=client,
                producer=producer,
                log=log,
                model=model,
                timeout_seconds=timeout_s,
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        flush(producer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Create `services/scorer/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "anthropic>=0.39.0" \
        "httpx>=0.27.0"

COPY services /app/services
COPY tests/fixtures /app/tests/fixtures

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.scorer.main"]
```

(We copy `tests/fixtures` into the image so integration tests can point `SCORER_FAKE_RESPONSE_PATH` at `/app/tests/fixtures/anthropic_score_response.json`.)

- [ ] **Step 6: Run unit tests to verify they pass**

Run:
```bash
pytest tests/unit/scorer/test_main.py -v
```

Expected: 2 PASSED.

- [ ] **Step 7: Build the scorer image to confirm Dockerfile is valid**

Run:
```bash
docker build -f services/scorer/Dockerfile -t headline-alerter-scorer:phase-1a .
```

Expected: builds successfully.

- [ ] **Step 8: Commit**

```bash
git add services/scorer/ tests/unit/scorer/test_main.py
git commit -m "feat(scorer): consumer service with anthropic call + dlq routing"
```

---

## Task 8: Wire ingestor + scorer into docker-compose.yml

Add the two new services to the existing Compose file. They depend on `kafka` healthy + `migrate` completed.

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add `ingestor-cnbc` and `scorer` services**

Open `docker-compose.yml`. After the `migrate:` block (at the same indentation level — under `services:`), add:

```yaml
  ingestor-cnbc:
    build:
      context: .
      dockerfile: services/ingestors/Dockerfile
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
    environment:
      KAFKA_BROKERS: "kafka:9092"
      POSTGRES_URL: "postgresql://rates:${POSTGRES_PASSWORD:-changeme}@postgres:5432/rates"
      CNBC_RSS_URLS: "${CNBC_RSS_URLS}"
      POLL_INTERVAL_SECONDS: "${POLL_INTERVAL_SECONDS:-60}"
    restart: unless-stopped

  scorer:
    build:
      context: .
      dockerfile: services/scorer/Dockerfile
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
    environment:
      KAFKA_BROKERS: "kafka:9092"
      POSTGRES_URL: "postgresql://rates:${POSTGRES_PASSWORD:-changeme}@postgres:5432/rates"
      ANTHROPIC_API_KEY: "${ANTHROPIC_API_KEY}"
      SCORER_MODEL: "${SCORER_MODEL:-claude-haiku-4-5}"
      SCORER_TIMEOUT_SECONDS: "${SCORER_TIMEOUT_SECONDS:-30}"
    restart: unless-stopped
```

- [ ] **Step 2: Build the new services and bring up the stack**

Run:
```bash
docker compose build ingestor-cnbc scorer
docker compose up -d
```

Expected: pulls/builds, all services start. `docker compose ps` shows `kafka`, `postgres` healthy; `ingestor-cnbc`, `scorer` running.

- [ ] **Step 3: Verify CNBC events arrive within ~1 minute**

Run (after waiting 60–90s):
```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT count(*), max(ts_ingested) FROM events_archive WHERE source = 'cnbc_rss';"
```

Expected: `count(*)` is non-zero, `max(ts_ingested)` is recent (within last minute or two).

- [ ] **Step 4: Verify scorer is producing scored rows**

Run:
```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT count(*) FILTER (WHERE status = 'scored') AS scored,
          count(*) FILTER (WHERE status = 'received') AS pending,
          count(*) FILTER (WHERE status = 'failed') AS failed
   FROM events_archive WHERE source = 'cnbc_rss';"
```

Expected: `scored` is climbing toward total. `failed` is 0 (or very small).

If `scored` stays at 0 after 30s, check `docker compose logs scorer --tail 50` — most likely an `ANTHROPIC_API_KEY` issue.

- [ ] **Step 5: Tail logs to inspect a single scored event**

Run:
```bash
docker compose logs scorer --tail 20 | grep '"event": "scored"'
```

Expected: at least one structured-log line with `event_id`, `score`, `direction`, `confidence`, `latency_ms` fields. Confirm `latency_ms` is in the low thousands or less.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(compose): add ingestor-cnbc and scorer services"
```

---

## Task 9: CLI tail tool (tools/tail.py)

A simple polling CLI that prints a refreshed table of recent events. No curses — just clear-and-reprint.

**Files:**
- Create: `tools/tail.py`

- [ ] **Step 1: Implement `tools/tail.py`**

```python
"""tail.py — live view of events_archive while we don't have a dashboard.

Polls Postgres every 2 seconds and prints the most recent N events.
Updates by clearing the screen with ANSI escapes (works on Git Bash, modern
PowerShell, and any UNIX terminal).

Usage:
    python tools/tail.py                       # last 20 rows, refresh every 2s
    python tools/tail.py --limit 50            # last 50 rows
    python tools/tail.py --source cnbc_rss     # filter by source
    python tools/tail.py --min-score 7         # only events scoring >= 7
"""
from __future__ import annotations
import argparse
import os
import sys
import time
from datetime import datetime

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.shared.db import connect

CLEAR = "\x1b[2J\x1b[H"   # ANSI clear screen + cursor home


def _ensure_env():
    os.environ.setdefault(
        "POSTGRES_URL",
        "postgresql://rates:changeme@localhost:5432/rates",
    )


def _fetch_rows(limit: int, source: str | None, min_score: int | None,
                status: str | None) -> list[tuple]:
    where = []
    params: list = []
    if source:
        where.append("source = %s")
        params.append(source)
    if status:
        where.append("status = %s")
        params.append(status)
    if min_score is not None:
        where.append("score >= %s")
        params.append(min_score)
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT ts_ingested, source, status, score, direction, confidence, headline
        FROM events_archive
        {where_clause}
        ORDER BY ts_ingested DESC
        LIMIT %s
    """
    params.append(limit)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _render(rows) -> str:
    if not rows:
        return "(no events yet — is the ingestor running?)"
    header = f"{'time':19s}  {'source':10s}  {'status':8s}  {'sc':>2s}  {'dir':12s}  {'conf':>4s}  headline"
    sep = "-" * 110
    out = [header, sep]
    for ts, source, status, score, direction, confidence, headline in rows:
        when = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else str(ts)
        sc_str = f"{score:2d}" if score is not None else " -"
        dir_str = (direction or "-")[:12]
        conf_str = f"{float(confidence):.2f}" if confidence is not None else " -  "
        head = (headline or "")[:60]
        out.append(f"{when}  {source:10s}  {status:8s}  {sc_str}  {dir_str:12s}  {conf_str}  {head}")
    return "\n".join(out)


def main() -> int:
    p = argparse.ArgumentParser(description="Live tail of events_archive.")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--source", default=None)
    p.add_argument("--status", default=None)
    p.add_argument("--min-score", type=int, default=None)
    p.add_argument("--interval", type=float, default=2.0,
                   help="Refresh interval in seconds (default: 2.0)")
    args = p.parse_args()

    _ensure_env()
    try:
        while True:
            rows = _fetch_rows(args.limit, args.source, args.min_score, args.status)
            sys.stdout.write(CLEAR)
            sys.stdout.write(_render(rows))
            sys.stdout.write(f"\n\n(refreshing every {args.interval:.1f}s — Ctrl-C to exit)\n")
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run it against the live stack**

Run (in a separate terminal):
```bash
python tools/tail.py
```

Expected: a refreshing table showing recent CNBC events with `status`, `score`, `direction`. Press Ctrl-C to exit.

- [ ] **Step 3: Verify filters work**

Run:
```bash
python tools/tail.py --status scored --limit 5
```

Expected: only rows with `status = scored` appear; up to 5 rows.

- [ ] **Step 4: Commit**

```bash
git add tools/tail.py
git commit -m "feat(tools): tail.py — live cli view of events_archive"
```

---

## Task 10: Real-API smoke test (tools/scorer_smoke.py)

One-shot tool that fetches one CNBC headline and runs it through the real Anthropic API. Used to verify keys + network end-to-end after deploy. Costs ~$0.0014 per run.

**Files:**
- Create: `tools/scorer_smoke.py`

- [ ] **Step 1: Implement `tools/scorer_smoke.py`**

```python
"""scorer_smoke.py — one-shot end-to-end test against the real Anthropic API.

Fetches one CNBC headline, normalizes it, and asks Claude Haiku 4.5 to score it.
Prints the result. Costs ~$0.0014. Not run in CI.

Usage: python tools/scorer_smoke.py
Requires: ANTHROPIC_API_KEY in env, CNBC_RSS_URLS optional (defaults to one feed).
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
import feedparser

from services.ingestors.cnbc_rss.main import CnbcIngestor
from services.shared.anthropic_client import score_event
from services.shared.logging import configure_logging, get_logger


DEFAULT_FEED = (
    "https://search.cnbc.com/rs/search/combinedcms/view.xml"
    "?partnerId=wrss01&id=10000664"
)


def main() -> int:
    configure_logging("scorer-smoke")
    log = get_logger()

    feed_url = os.environ.get("CNBC_RSS_URLS", DEFAULT_FEED).split(",")[0].strip()
    log.info("fetching", url=feed_url)
    parsed = feedparser.parse(feed_url)
    if parsed.bozo or not parsed.entries:
        log.error("feed unusable", error=str(getattr(parsed, "bozo_exception", "no entries")))
        return 1

    # Use the same _normalize_item the production ingestor uses.
    ing = CnbcIngestor(urls=[feed_url], producer=type("P", (), {"produce": lambda *a, **k: None,
                                                                 "flush": lambda *a, **k: None})())
    raw = {"entry": parsed.entries[0], "_feed_url": feed_url}
    event = ing._normalize_item(raw)
    log.info("event normalized", event_id=event.event_id, headline=event.headline[:80])

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY env var is required")
        return 1
    client = anthropic.Anthropic(api_key=api_key)

    log.info("scoring (real Anthropic call)")
    scored = score_event(client, normalized_event=event,
                         model=os.environ.get("SCORER_MODEL", "claude-haiku-4-5"),
                         timeout_seconds=30)

    print(
        f"OK — Phase 1a smoke test passed (event scored: "
        f"{scored.score}/{scored.direction}/{scored.confidence:.2f})"
    )
    print(f"Headline: {event.headline}")
    print(f"Reasoning: {scored.reasoning}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the real-API smoke test**

Run:
```bash
python tools/scorer_smoke.py
```

Expected last lines:
```
OK — Phase 1a smoke test passed (event scored: 4/neutral/0.55)
Headline: Stocks making the biggest moves midday: ...
Reasoning: This is a generic markets recap...
```

(Exact score/direction will vary based on the headline.)

If you see "ANTHROPIC_API_KEY env var is required", export it (e.g., `export ANTHROPIC_API_KEY=$(grep '^ANTHROPIC_API_KEY=' .env | cut -d= -f2)`).

- [ ] **Step 3: Commit**

```bash
git add tools/scorer_smoke.py
git commit -m "feat(tools): scorer_smoke.py — one-shot real-anthropic verification"
```

---

## Task 11: Integration test (tests/integration/test_phase1a_e2e.py)

Real Kafka + real Postgres, mocked Anthropic via `SCORER_FAKE_RESPONSE_PATH`. We run the scorer's `process_one_event` directly against the running infra and verify both the success and DLQ paths.

**Files:**
- Create: `tests/integration/test_phase1a_e2e.py`

- [ ] **Step 1: Write the integration test**

Create `tests/integration/test_phase1a_e2e.py`:

```python
"""Phase 1a end-to-end integration test.

Real Kafka + real Postgres, in-process scorer. Anthropic is faked via the
SCORER_FAKE_RESPONSE_PATH env var that the scorer's build_anthropic_client
reads when constructing its client.

Requires: docker compose up -d (kafka + postgres + migrate done).
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Consumer

from services.scorer.main import (
    build_anthropic_client, process_one_event,
)
from services.shared.db import connect
from services.shared.kafka_client import flush, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.models import NormalizedEvent

FIXTURE_PATH = str(
    (Path(__file__).parents[1] / "fixtures" / "anthropic_score_response.json").resolve()
)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9094")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://rates:changeme@localhost:5432/rates")
    monkeypatch.setenv("SCORER_MODEL", "claude-haiku-4-5")
    monkeypatch.setenv("SCORER_FAKE_RESPONSE_PATH", FIXTURE_PATH)
    yield


def _seed_event_in_archive(event: NormalizedEvent):
    """Pretend the ingestor wrote a 'received' row, so the scorer's UPDATE has a target."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, ts_ingested, status, headline, body, url, metadata)
                VALUES (%s, %s, %s, %s, 'received', %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event.event_id, event.source, event.ts_source, event.ts_ingested,
                    event.headline, event.body, event.url, "{}",
                ),
            )
        conn.commit()


def _fetch_archive_row(event_id):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, score, direction, confidence FROM events_archive WHERE id = %s",
                        (event_id,))
            return cur.fetchone()


def _consume_one(topic, deadline_seconds=10):
    consumer = Consumer({
        "bootstrap.servers": "localhost:9094",
        "group.id": f"itest-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    deadline = time.time() + deadline_seconds
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                return json.loads(msg.value().decode("utf-8"))
        return None
    finally:
        consumer.close()


# ---- success path --------------------------------------------------------

def test_success_path_writes_score_and_produces(env):
    configure_logging("itest")
    log = get_logger()
    ev = NormalizedEvent(
        event_id=f"itest-success-{uuid.uuid4().hex[:8]}",
        source="cnbc_rss",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="Powell signals dovish pivot",
        body="Body text.",
        url="https://example.com/x",
        metadata={},
    )
    _seed_event_in_archive(ev)

    client = build_anthropic_client()
    producer = make_producer()
    process_one_event(
        ev.to_dict(), anthropic_client=client, producer=producer,
        log=log, model="claude-haiku-4-5",
    )

    # Postgres updated
    row = _fetch_archive_row(ev.event_id)
    assert row is not None
    status, score, direction, confidence = row
    assert status == "scored"
    assert score == 7
    assert direction == "rates_lower"
    assert float(confidence) == pytest.approx(0.72)


# ---- DLQ path ------------------------------------------------------------

def test_dlq_path_routes_failed_event(env, monkeypatch):
    monkeypatch.setenv("SCORER_FAKE_FAIL_MODE", "rate_limit")
    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)
    configure_logging("itest")
    log = get_logger()
    ev = NormalizedEvent(
        event_id=f"itest-dlq-{uuid.uuid4().hex[:8]}",
        source="cnbc_rss",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="Some unimportant tweet",
        body="b",
        url="https://example.com/y",
        metadata={},
    )
    _seed_event_in_archive(ev)

    client = build_anthropic_client()
    producer = make_producer()
    process_one_event(
        ev.to_dict(), anthropic_client=client, producer=producer,
        log=log, model="claude-haiku-4-5",
    )

    # Archive marked failed
    row = _fetch_archive_row(ev.event_id)
    assert row is not None and row[0] == "failed"

    # DLQ message present
    payload = _consume_one("events.dlq", deadline_seconds=10)
    # Note: the DLQ may have older messages; loop until we find ours.
    if payload and payload.get("original_event", {}).get("event_id") == ev.event_id:
        found = payload
    else:
        # Replay-from-beginning consume to find ours.
        consumer = Consumer({
            "bootstrap.servers": "localhost:9094",
            "group.id": f"itest-dlq-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe(["events.dlq"])
        found = None
        deadline = time.time() + 10
        while time.time() < deadline and not found:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                p = json.loads(msg.value().decode("utf-8"))
                if p.get("original_event", {}).get("event_id") == ev.event_id:
                    found = p
                    break
        consumer.close()

    assert found is not None
    assert found["stage"] == "scorer_throttle"
    assert found["service"] == "scorer"
```

- [ ] **Step 2: Run the integration test**

Ensure the stack is up first:
```bash
docker compose up -d
```

Then:
```bash
pytest tests/integration/test_phase1a_e2e.py -v
```

Expected: 2 PASSED. Total runtime ~10–20s.

If `test_dlq_path_routes_failed_event` fails to find the DLQ message, increase the deadline to 20s — Kafka producer flushing under load can occasionally take a moment.

- [ ] **Step 3: Run the full test suite**

Run:
```bash
pytest -v
```

Expected: all unit + all integration tests PASS. Approximate breakdown:
- Phase 0 baseline: 10 tests
- Phase 1a unit: ~30 tests
- Phase 1a integration: 2 tests

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_phase1a_e2e.py
git commit -m "test(integration): phase 1a end-to-end (real kafka+pg, mocked anthropic)"
```

---

## Task 12: Acceptance run-through

Verify all 8 acceptance criteria from spec § 9 in sequence. This task creates no files but documents the verification ritual; the steps must all pass for Phase 1a to be considered done.

- [ ] **Step 1: Bring the full stack up cleanly**

Run:
```bash
docker compose down
docker compose up -d
docker compose ps
```

Expected: `kafka`, `postgres` are `healthy`. `kafka-init`, `migrate` exited 0. `ingestor-cnbc`, `scorer` are `Up` (no restart loops).

- [ ] **Step 2: Verify CNBC events arrive (criterion 2)**

Wait ~90s, then run:
```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT count(*) FROM events_archive WHERE source = 'cnbc_rss';"
```

Expected: count >= 1.

- [ ] **Step 3: Verify scoring p95 latency (criterion 3)**

Run:
```bash
docker compose logs scorer --tail 200 | grep latency_ms | \
  python -c "import sys, json, statistics; \
             ms=[json.loads(l.split(' ', 0)[0] if l.startswith('{') else l[l.find('{'):])['latency_ms'] \
                  for l in sys.stdin if 'latency_ms' in l]; \
             print('count', len(ms), 'p50', statistics.median(ms), 'p95', sorted(ms)[int(0.95*len(ms))-1])"
```

(If that one-liner is awkward in your shell, just inspect the logs visually with `docker compose logs scorer --tail 50 | grep latency_ms` — most values should be under 5000.)

Expected: p95 under ~5000ms. If well above, investigate before declaring done.

- [ ] **Step 4: Run the live tail (criterion 4)**

Run (in another terminal, leave it running for ~30s):
```bash
python tools/tail.py
```

Expected: events tick through, status flipping `received` → `scored` within seconds. Ctrl-C to exit.

- [ ] **Step 5: Verify DLQ path with a synthetic broken event (criterion 5)**

Inject a synthetic failure by temporarily setting the scorer's fail-mode and producing one event ourselves. The cleanest one-shot is to run the integration test directly:

```bash
pytest tests/integration/test_phase1a_e2e.py::test_dlq_path_routes_failed_event -v
```

Then verify the scorer is still alive and processing real events:
```bash
docker compose ps scorer
docker compose logs scorer --tail 5
```

Expected: scorer is still `Up`; logs show recent `scored` events.

- [ ] **Step 6: Run real-API smoke test (criterion 6)**

```bash
python tools/scorer_smoke.py
```

Expected: prints `OK — Phase 1a smoke test passed (event scored: ...)`.

- [ ] **Step 7: Run full test suite (criterion 7)**

```bash
pytest -v
```

Expected: all green.

- [ ] **Step 8: Verify restart safety (criterion 8)**

```bash
# Note count and latest event_id before restart
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT count(*), max(ts_ingested) FROM events_archive WHERE source='cnbc_rss';"

docker compose restart ingestor-cnbc scorer

# Wait 90s, then re-check — count should grow, no duplicate ids
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT id, count(*) FROM events_archive WHERE source='cnbc_rss'
   GROUP BY id HAVING count(*) > 1;"
```

Expected: the second query returns 0 rows (no duplicates). Pre-restart events still present; new events keep arriving after restart.

- [ ] **Step 9: Phase 1a is complete — final commit**

If you've added anything else along the way (e.g., extra ad-hoc fixes), commit. Otherwise, no commit needed; Phase 1a was already committed task by task.

---

## What this phase does NOT cover (intentional)

These come in subsequent phases (each its own plan):

- **Phase 1b** — Twilio alerter: consume `events.scored`, fire SMS/WhatsApp on `score >= ALERT_THRESHOLD AND confidence >= MIN_CONFIDENCE`, write `alert_history`.
- **Phase 1c** — FastAPI dashboard with SSE for live browser updates; this replaces `tools/tail.py` for daily monitoring.
- **Phase 2+** — additional ingestors (`bls_rss`, `treasury_rss`, `truth_social`, `x_curated`).
- Real-Anthropic CI integration tests (cost + flake risk).
- Soak tests, scaling tests under `--scale scorer=N`.
- All items in parent spec § 9 (cross-source dedup, cooldowns, evaluator, market data, etc.).
