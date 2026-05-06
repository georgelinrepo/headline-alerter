# Headline Alerter — Phase 1b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the alerter service that turns Phase 1a's high-scoring events into WhatsApp messages on the user's phone via Twilio Sandbox.

**Architecture:** A new Compose service `alerter` consumes `events.scored`, gates each event with `should_fire(score, confidence)` and `has_been_alerted(event_id)` (idempotency), sends a WhatsApp message via Twilio's WhatsApp Sandbox, then writes `alert_history` + updates `events_archive.status='alerted'` + produces an audit message to `alerts.outgoing`. Twilio failures route to `events.dlq` with typed stages (`alerter_throttle`, `alerter_5xx`, `alerter_recipient`, `alerter_recipient_not_opted_in`, `alerter_whatsapp_template`, `alerter_auth`, `alerter_timeout`, `alerter_unknown`). The alerter never crashes on a single bad message.

**Tech Stack:** Python 3.12, `twilio>=9.0.0` (SDK, already in pyproject from Phase 0), `confluent-kafka-python`, `psycopg[binary]`, `structlog`, Docker Compose v2. Tests use `pytest` + injected fakes.

**Spec:** [`docs/superpowers/specs/2026-05-06-headline-alerter-phase-1b-design.md`](../specs/2026-05-06-headline-alerter-phase-1b-design.md).

**Working directory:** `C:\Projects\headline-alerter\`. Activate venv first: `source .venv/Scripts/activate` (Git Bash).

**Definition of done** (all 8 acceptance criteria from spec § 9):
1. `docker compose up -d` shows ingestor-cnbc + scorer + **alerter** all running
2. `python tools/alerter_smoke.py` puts a real WhatsApp message on the user's phone
3. A real CNBC event with `score >= 4 AND confidence >= 0.6` triggers a WhatsApp message
4. Below-threshold events are silently skipped (no Twilio, no PG writes)
5. Replay (consumer offset reset) produces zero new WhatsApp messages
6. Forced fake throttle failure routes to DLQ without crashing the alerter
7. `alert_history` row written with all required fields per spec § 5.5
8. `pytest -v` — unit + integration both green

---

## File Structure (created in this plan)

```
headline-alerter/
├── .env.example                                # Modified: Task 1
├── docker-compose.yml                          # Modified: Task 5
├── README.md                                   # Modified: Task 1 (runbook)
├── services/
│   └── alerter/
│       ├── __init__.py                         # Task 4
│       ├── Dockerfile                          # Task 4
│       ├── format.py                           # Task 2
│       ├── twilio_client.py                    # Task 3
│       └── main.py                             # Task 4
├── tools/
│   └── alerter_smoke.py                        # Task 6
└── tests/
    ├── unit/
    │   └── alerter/
    │       ├── __init__.py                     # Task 2
    │       ├── test_format.py                  # Task 2
    │       ├── test_twilio_client.py           # Task 3
    │       └── test_main.py                    # Task 4
    ├── integration/
    │   └── test_phase1b_e2e.py                 # Task 7
    └── fixtures/
        └── scored_event.json                   # Task 2
```

---

## Task 1: Environment + WhatsApp Sandbox runbook

Capture the new env vars in `.env.example` and document the one-time WhatsApp Sandbox opt-in ritual in the README. The user supplies their own Twilio credentials and joins the sandbox.

**Files:**
- Modify: `.env.example`
- Modify: `README.md`

- [ ] **Step 1: Update `.env.example`**

Replace the entire `# Twilio (Phase 1b)` section and the `# Alert thresholds (Phase 1b)` section in `.env.example` with:

```
# Twilio (Phase 1b)
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
# WhatsApp Sandbox FROM number (Twilio's shared sandbox; same for everyone).
TWILIO_FROM=whatsapp:+14155238886
# Your phone number with WhatsApp prefix. You MUST text "join <code>" to the
# sandbox FROM number from this phone first (one-time opt-in — see README).
ALERT_RECIPIENT=whatsapp:+44...

ALERT_CHANNEL=whatsapp
ALERT_THRESHOLD=4
MIN_CONFIDENCE=0.6
```

(The other sections — Postgres, Anthropic, Scorer, CNBC RSS, X API — stay unchanged.)

- [ ] **Step 2: Add a "WhatsApp Sandbox setup" section to `README.md`**

Append this section to the end of `README.md`:

```markdown

## WhatsApp Sandbox setup (one-time, required for Phase 1b alerter)

The alerter uses Twilio's WhatsApp Sandbox. Free number, ~$0.005/message, no
need to exit Twilio trial mode.

1. Twilio Console → Develop → Messaging → Try it out → **Send a WhatsApp
   message**. Note the sandbox number (`+1 415 523 8886`) and the join code
   (a phrase like `join sky-glow`).
2. From your phone's WhatsApp, send `join <your-code>` to `+1 415 523 8886`.
   You should receive `Joined <your-code>. Reply ...`.
3. Copy your Account SID and Auth Token from the Twilio Console
   (Account → API keys & tokens) into `.env`:
   ```
   TWILIO_ACCOUNT_SID=AC...
   TWILIO_AUTH_TOKEN=...
   TWILIO_FROM=whatsapp:+14155238886
   ALERT_RECIPIENT=whatsapp:+44...    # your phone, with country code
   ```
4. `docker compose up -d alerter` to start the alerter.
5. `python tools/alerter_smoke.py` to verify end-to-end (sends one
   hardcoded test message; you should receive it within 5s).

### The 24-hour window caveat

WhatsApp Sandbox sessions expire 24h after your last inbound message. If
the alerter goes silent for >24h (no events meeting threshold over a quiet
weekend), the next outbound message will fail with Twilio code 63016. The
alerter routes that to `events.dlq` with `stage='alerter_whatsapp_template'`
and keeps consuming. Reply anything to the sandbox to reopen the window.
```

- [ ] **Step 3: Update local `.env`**

Run:
```bash
diff .env .env.example
```

Bring `.env` in sync with the `.env.example` updates. Add the lines that don't already exist (`TWILIO_ACCOUNT_SID=<your-sid>`, `TWILIO_AUTH_TOKEN=<your-token>`, `TWILIO_FROM=whatsapp:+14155238886`, `ALERT_RECIPIENT=whatsapp:+...your-number...`, `ALERT_CHANNEL=whatsapp`, `ALERT_THRESHOLD=4`, `MIN_CONFIDENCE=0.6`).

**Important:** do not commit `.env` (it's gitignored).

- [ ] **Step 4: Confirm Phase 1a tests still pass**

Run:
```bash
docker compose up -d
pytest -v
```

Expected: 52 passed (the Phase 1a baseline).

- [ ] **Step 5: Commit**

```bash
git add .env.example README.md
git commit -m "chore(phase-1b): env vars for twilio whatsapp sandbox + runbook"
```

---

## Task 2: Alert formatting (services/alerter/format.py)

Pure functions that build the WhatsApp message body from a `ScoredEvent` plus the looked-up archive context (headline, source, ts_source, url). No I/O. Trivially testable.

**Files:**
- Create: `services/alerter/__init__.py` (empty)
- Create: `services/alerter/format.py`
- Create: `tests/unit/alerter/__init__.py` (empty)
- Create: `tests/unit/alerter/test_format.py`
- Create: `tests/fixtures/scored_event.json`

- [ ] **Step 1: Create directory markers**

```bash
mkdir -p services/alerter tests/unit/alerter
touch services/alerter/__init__.py tests/unit/alerter/__init__.py
```

- [ ] **Step 2: Create `tests/fixtures/scored_event.json`**

```json
{
  "event_id": "evt-fixture-1",
  "score": 7,
  "direction": "rates_lower",
  "confidence": 0.72,
  "reasoning": "Powell tone notably more dovish than recent statements; market is likely to price in a near-term cut. Largest impact expected on the 2y tenor.",
  "model": "claude-haiku-4-5",
  "scored_at": "2026-05-06T14:32:11+00:00"
}
```

- [ ] **Step 3: Write the failing test**

Create `tests/unit/alerter/test_format.py`:

```python
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
```

- [ ] **Step 4: Run failing test**

```bash
pytest tests/unit/alerter/test_format.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.alerter.format'`.

- [ ] **Step 5: Implement `services/alerter/format.py`**

```python
"""Build a WhatsApp/SMS alert message from a ScoredEvent + archive context.

Pure functions only — no I/O. The alerter's main loop calls format_alert()
just before invoking the Twilio client.
"""
from __future__ import annotations
from datetime import datetime

from services.shared.models import ScoredEvent


_DIRECTION_GLYPH = {
    "rates_higher": "↑",
    "rates_lower": "↓",
    "neutral": "→",
    "unclear": "?",
}


def format_alert(
    scored: ScoredEvent,
    *,
    headline: str,
    source: str,
    ts_source: datetime,
    url: str | None,
) -> str:
    """Build the alert message body. Plain text; WhatsApp auto-linkifies the URL."""
    glyph = _DIRECTION_GLYPH.get(scored.direction, "?")
    conf_pct = int(round(scored.confidence * 100))
    ts_short = ts_source.strftime("%H:%MZ")

    lines = [
        f"[{scored.score}/10 {glyph} {scored.direction} · conf {conf_pct}%]",
        f"{source} · {ts_short}",
        "",
        headline,
        "",
        scored.reasoning,
    ]
    if url:
        lines.extend(["", url])
    return "\n".join(lines)
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
pytest tests/unit/alerter/test_format.py -v
```

Expected: 6 PASSED.

- [ ] **Step 7: Commit**

```bash
git add services/alerter/__init__.py services/alerter/format.py \
        tests/unit/alerter/__init__.py tests/unit/alerter/test_format.py \
        tests/fixtures/scored_event.json
git commit -m "feat(alerter): alert message formatter (pure functions)"
```

---

## Task 3: Twilio client wrapper (services/alerter/twilio_client.py)

Wraps `twilio.rest.Client.messages.create()` with retry/backoff, typed `AlerterError`, and a `_FakeTwilioClient` for integration tests. The single public entry point is `send_message(client, *, channel, to, from_number, body) -> twilio_sid`.

**Files:**
- Create: `services/alerter/twilio_client.py`
- Create: `tests/unit/alerter/test_twilio_client.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/alerter/test_twilio_client.py`:

```python
"""Unit tests for the Twilio client wrapper."""
from unittest.mock import MagicMock

import pytest
from twilio.base.exceptions import TwilioRestException

from services.alerter.twilio_client import (
    AlerterError, send_message, _FakeTwilioClient, build_client,
)


def _ok_client(sid="SMfakesid"):
    """Mock client whose messages.create() returns a message with .sid."""
    msg = MagicMock()
    msg.sid = sid
    client = MagicMock()
    client.messages.create.return_value = msg
    return client


def _failing_client(exc):
    client = MagicMock()
    client.messages.create.side_effect = exc
    return client


def _twilio_err(status, code=None):
    return TwilioRestException(status=status, uri="/Messages", msg=str(status),
                               code=code, method="POST")


def test_success_returns_twilio_sid():
    client = _ok_client(sid="SM123abc")
    sid = send_message(client, channel="whatsapp",
                       to="whatsapp:+44...", from_number="whatsapp:+14155238886",
                       body="hello")
    assert sid == "SM123abc"


def test_429_retries_three_times_then_dlq(monkeypatch):
    sleeps = []
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep",
                        lambda s: sleeps.append(s))
    client = _failing_client([_twilio_err(429)] * 4)
    client.messages.create.side_effect = [_twilio_err(429)] * 4

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_throttle"
    assert exc_info.value.retry_count == 3
    assert sleeps == [1, 4, 16]


def test_5xx_retries_three_times_then_dlq(monkeypatch):
    sleeps = []
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep",
                        lambda s: sleeps.append(s))
    client = MagicMock()
    client.messages.create.side_effect = [_twilio_err(503)] * 4

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_5xx"
    assert exc_info.value.retry_count == 3
    assert sleeps == [1, 4, 16]


def test_auth_error_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(401)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_auth"
    assert client.messages.create.call_count == 1


def test_invalid_recipient_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=21211)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient"


def test_recipient_not_opted_in_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=63007)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient_not_opted_in"


def test_whatsapp_template_required_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=63016)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_whatsapp_template"


def test_unsubscribed_recipient_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=21610)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient_not_opted_in"


def test_timeout_retries_once_then_dlq(monkeypatch):
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    client = MagicMock()
    client.messages.create.side_effect = [TimeoutError("slow"), TimeoutError("slow")]

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_timeout"
    assert client.messages.create.call_count == 2


def test_unknown_exception_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = ValueError("???")
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_unknown"
    assert client.messages.create.call_count == 1


def test_recovery_after_one_throttle(monkeypatch):
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    msg = MagicMock(); msg.sid = "SMok"
    client = MagicMock()
    client.messages.create.side_effect = [_twilio_err(429), msg]

    sid = send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert sid == "SMok"
    assert client.messages.create.call_count == 2


# ---- _FakeTwilioClient ----------------------------------------------------

def test_fake_client_default_returns_sid():
    fake = _FakeTwilioClient(fail_mode=None)
    msg = fake.messages.create(to="x", from_="y", body="z")
    assert msg.sid.startswith("SM")


def test_fake_client_throttle_mode_raises_429():
    fake = _FakeTwilioClient(fail_mode="throttle")
    with pytest.raises(TwilioRestException) as exc_info:
        fake.messages.create(to="x", from_="y", body="z")
    assert exc_info.value.status == 429


def test_fake_client_recipient_mode_raises_21211():
    fake = _FakeTwilioClient(fail_mode="recipient")
    with pytest.raises(TwilioRestException) as exc_info:
        fake.messages.create(to="x", from_="y", body="z")
    assert exc_info.value.code == 21211


def test_build_client_uses_fake_when_env_set(monkeypatch):
    monkeypatch.setenv("TWILIO_FAKE", "1")
    monkeypatch.setenv("TWILIO_FAIL_MODE", "throttle")
    client = build_client()
    assert isinstance(client, _FakeTwilioClient)
    assert client._fail_mode == "throttle"


def test_build_client_raises_when_creds_missing(monkeypatch):
    monkeypatch.delenv("TWILIO_FAKE", raising=False)
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="TWILIO_ACCOUNT_SID"):
        build_client()
```

- [ ] **Step 2: Run failing test**

```bash
pytest tests/unit/alerter/test_twilio_client.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `services/alerter/twilio_client.py`**

```python
"""Twilio client wrapper.

Public surface:
    send_message(client, *, channel, to, from_number, body) -> twilio_sid
    build_client() -> twilio.rest.Client | _FakeTwilioClient
    AlerterError(stage, original, retry_count)

Handles retry/backoff for transient failures, maps Twilio error codes to
typed `stage` strings used for DLQ routing.
"""
from __future__ import annotations
import os
import time
from typing import Any

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException


_BACKOFF_DELAYS_SECONDS = [1, 4, 16]

# Twilio error codes (verified against twilio.com/docs/api/errors).
_CODE_INVALID_RECIPIENT = {21211, 21408}            # bad To number
_CODE_NOT_OPTED_IN = {63007, 63018, 21610}          # WhatsApp / SMS opt-in required
_CODE_TEMPLATE_REQUIRED = {63016}                   # WhatsApp 24h window expired


class AlerterError(Exception):
    """Raised when the Twilio call fails terminally. `stage` drives DLQ routing."""

    def __init__(self, stage: str, original: BaseException | None = None,
                 retry_count: int = 0) -> None:
        self.stage = stage
        self.original = original
        self.retry_count = retry_count
        super().__init__(
            f"{stage}: {type(original).__name__ if original else ''}: {original}"
        )


def send_message(client, *, channel: str, to: str, from_number: str, body: str) -> str:
    """Send a WhatsApp/SMS message via Twilio. Returns the message SID on success.

    Raises AlerterError(stage=...) on terminal failure (after retries).
    """
    transient_attempt = 0
    timeout_attempt = 0

    while True:
        try:
            msg = client.messages.create(to=to, from_=from_number, body=body)
            return msg.sid
        except TwilioRestException as e:
            status = getattr(e, "status", 0) or 0
            code = getattr(e, "code", 0) or 0

            if status == 429:
                if transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                    time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                    transient_attempt += 1
                    continue
                raise AlerterError("alerter_throttle", e, retry_count=transient_attempt)

            if status in (401, 403):
                raise AlerterError("alerter_auth", e, retry_count=0)

            if code in _CODE_NOT_OPTED_IN:
                raise AlerterError("alerter_recipient_not_opted_in", e, retry_count=0)

            if code in _CODE_TEMPLATE_REQUIRED:
                raise AlerterError("alerter_whatsapp_template", e, retry_count=0)

            if code in _CODE_INVALID_RECIPIENT:
                raise AlerterError("alerter_recipient", e, retry_count=0)

            if 500 <= status < 600 and transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                transient_attempt += 1
                continue
            raise AlerterError("alerter_5xx", e, retry_count=transient_attempt)

        except (TimeoutError, OSError) as e:
            if timeout_attempt < 1:
                timeout_attempt += 1
                continue
            raise AlerterError("alerter_timeout", e, retry_count=timeout_attempt)

        except Exception as e:
            raise AlerterError("alerter_unknown", e, retry_count=0)


# ---- Integration-test seam ------------------------------------------------

class _FakeTwilioClient:
    """Stand-in for twilio.rest.Client used by integration tests.

    Activated by TWILIO_FAKE=1. TWILIO_FAIL_MODE controls behavior:
    - unset / 'none': returns a fake Message with sid='SM<...>fake'
    - 'throttle':     raises TwilioRestException(status=429)
    - 'recipient':    raises TwilioRestException(status=400, code=21211)
    - 'auth':         raises TwilioRestException(status=401)
    """

    def __init__(self, fail_mode: str | None = None):
        self._fail_mode = fail_mode or "none"
        self.messages = self  # so .messages.create works

    def create(self, **kwargs):
        if self._fail_mode == "throttle":
            raise TwilioRestException(status=429, uri="/Messages",
                                      msg="429", method="POST")
        if self._fail_mode == "recipient":
            raise TwilioRestException(status=400, uri="/Messages",
                                      msg="invalid", code=21211, method="POST")
        if self._fail_mode == "auth":
            raise TwilioRestException(status=401, uri="/Messages",
                                      msg="auth", method="POST")
        # Success: return a Message-like object.
        class _Msg:
            sid = "SM" + "0" * 30 + "fake"
        return _Msg()


def build_client():
    if os.environ.get("TWILIO_FAKE"):
        return _FakeTwilioClient(fail_mode=os.environ.get("TWILIO_FAIL_MODE"))

    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    if not sid or not token:
        raise RuntimeError("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN env vars are required")
    return Client(sid, token)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/alerter/test_twilio_client.py -v
```

Expected: 16 PASSED (10 send_message scenarios + 4 _FakeTwilioClient + 2 build_client).

- [ ] **Step 5: Commit**

```bash
git add services/alerter/twilio_client.py tests/unit/alerter/test_twilio_client.py
git commit -m "feat(alerter): twilio client wrapper with retries, typed errors, fake-mode seam"
```

---

## Task 4: Alerter main service + Dockerfile

The consumer loop, decision logic, idempotency check, archive lookup, Twilio call, and PG/Kafka writes all live here. Plus the Dockerfile so this can run in Compose.

**Files:**
- Create: `services/alerter/main.py`
- Create: `services/alerter/Dockerfile`
- Create: `tests/unit/alerter/test_main.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/alerter/test_main.py`:

```python
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
```

- [ ] **Step 2: Run failing test**

```bash
pytest tests/unit/alerter/test_main.py -v
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `services/alerter/main.py`**

```python
"""Alerter service: consume events.scored, send WhatsApp via Twilio for high-scoring events.

Decision: should_fire(score, confidence) → has_been_alerted(event_id) → send → record.
Failures (Twilio errors, missing archive row, post-send PG failure) route to events.dlq
with typed stages. The alerter never crashes on a single bad message.
"""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from services.shared.db import connect
from services.shared.dlq import send_to_dlq
from services.shared.kafka_client import flush, make_consumer, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.models import ScoredEvent
from services.alerter.format import format_alert
from services.alerter.twilio_client import AlerterError, build_client, send_message


# ---- predicates -----------------------------------------------------------

def should_fire(score: int, confidence: float, *,
                threshold: int, min_confidence: float) -> bool:
    return score >= threshold and confidence >= min_confidence


def has_been_alerted(event_id: str) -> bool:
    """Returns True iff alert_history already has a row for this event_id."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM alert_history WHERE event_id = %s LIMIT 1",
                (event_id,),
            )
            return cur.fetchone() is not None


def fetch_archive_context(event_id: str) -> tuple[str, str, datetime, str | None]:
    """Returns (headline, source, ts_source, url). Raises if row missing."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT headline, source, ts_source, url FROM events_archive WHERE id = %s",
                (event_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"events_archive row not found for event_id={event_id}")
    return row


def insert_alert_history(event_id: str, *, channel: str,
                         recipient: str, twilio_sid: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_history
                  (event_id, channel, recipient, twilio_sid, delivery_status)
                VALUES (%s, %s, %s, %s, 'queued')
                """,
                (event_id, channel, recipient, twilio_sid),
            )
        conn.commit()


def mark_alerted(event_id: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status='alerted', ts_alerted=NOW()
                WHERE id = %s
                """,
                (event_id,),
            )
            if cur.rowcount == 0:
                raise RuntimeError(f"events_archive row not found for event_id={event_id}")
        conn.commit()


# ---- per-message processing ----------------------------------------------

def process_one_alert(scored_dict: dict[str, Any], *, twilio_client, producer, log,
                      channel: str, recipient: str, from_number: str,
                      threshold: int, min_confidence: float) -> None:
    """Decide → idempotency → send → record. Routes failures to DLQ."""
    scored = ScoredEvent.from_dict(scored_dict)

    if not should_fire(scored.score, scored.confidence,
                       threshold=threshold, min_confidence=min_confidence):
        log.debug("below threshold; skip", event_id=scored.event_id,
                  score=scored.score, confidence=scored.confidence)
        return

    if has_been_alerted(scored.event_id):
        log.info("already alerted; skip", event_id=scored.event_id)
        return

    # Send path: archive read → format → Twilio.
    try:
        headline, source, ts_source, url = fetch_archive_context(scored.event_id)
        body = format_alert(scored, headline=headline, source=source,
                            ts_source=ts_source, url=url)
        twilio_sid = send_message(twilio_client, channel=channel, to=recipient,
                                  from_number=from_number, body=body)
    except AlerterError as e:
        log.warning("alert send failed", event_id=scored.event_id,
                    stage=e.stage, error=str(e.original),
                    retry_count=e.retry_count)
        send_to_dlq(producer, stage=e.stage, service="alerter",
                    error=e.original or e, original_event=scored_dict,
                    retry_count=e.retry_count)
        flush(producer)
        return
    except Exception as e:
        log.error("alerter pre-send error", event_id=scored.event_id, error=str(e))
        send_to_dlq(producer, stage="alerter_unknown", service="alerter",
                    error=e, original_event=scored_dict, retry_count=0)
        flush(producer)
        return

    # Post-send writes: alert_history + events_archive + audit topic.
    try:
        insert_alert_history(scored.event_id, channel=channel,
                             recipient=recipient, twilio_sid=twilio_sid)
        mark_alerted(scored.event_id)
        produce(producer, "alerts.outgoing", key=scored.event_id, payload={
            "event_id": scored.event_id,
            "channel": channel,
            "recipient": recipient,
            "twilio_sid": twilio_sid,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        })
        flush(producer)
    except Exception as e:
        # Twilio already delivered — PG/Kafka writes failed.
        # DLQ with the twilio_sid embedded so an operator can manually reconcile.
        log.error("post-send write failed",
                  event_id=scored.event_id, error=str(e), twilio_sid=twilio_sid)
        send_to_dlq(producer, stage="alerter_unknown", service="alerter",
                    error=e,
                    original_event={**scored_dict, "_twilio_sid": twilio_sid},
                    retry_count=0)
        flush(producer)
        return

    log.info("alerted", event_id=scored.event_id, score=scored.score,
             direction=scored.direction, confidence=scored.confidence,
             twilio_sid=twilio_sid)


# ---- main loop -----------------------------------------------------------

def _consumer_group_id() -> str:
    return os.environ.get("ALERTER_GROUP_ID", "alerter-cg")


def main() -> int:
    configure_logging("alerter")
    log = get_logger()
    log.info("starting alerter")

    channel = os.environ.get("ALERT_CHANNEL", "whatsapp")
    recipient = os.environ.get("ALERT_RECIPIENT")
    if not recipient:
        raise RuntimeError("ALERT_RECIPIENT env var is required")
    from_number = os.environ.get("TWILIO_FROM")
    if not from_number:
        raise RuntimeError("TWILIO_FROM env var is required")
    threshold = int(os.environ.get("ALERT_THRESHOLD", "4"))
    min_confidence = float(os.environ.get("MIN_CONFIDENCE", "0.6"))

    log.info("alerter config",
             channel=channel, threshold=threshold,
             min_confidence=min_confidence, recipient=recipient[:14] + "...")

    twilio_client = build_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.scored"])

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
                consumer.commit(message=msg, asynchronous=False)
                continue

            process_one_alert(
                payload,
                twilio_client=twilio_client,
                producer=producer,
                log=log,
                channel=channel,
                recipient=recipient,
                from_number=from_number,
                threshold=threshold,
                min_confidence=min_confidence,
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        flush(producer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Create `services/alerter/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "twilio>=9.0.0" \
        "httpx>=0.27.0"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.alerter.main"]
```

- [ ] **Step 5: Run unit tests to verify they pass**

```bash
pytest tests/unit/alerter/test_main.py -v
```

Expected: 6 PASSED.

- [ ] **Step 6: Build the alerter image**

```bash
docker build -f services/alerter/Dockerfile -t headline-alerter-alerter:phase-1b .
```

Expected: builds cleanly.

- [ ] **Step 7: Commit**

```bash
git add services/alerter/main.py services/alerter/Dockerfile tests/unit/alerter/test_main.py
git commit -m "feat(alerter): main service with decision, idempotency, dlq routing + dockerfile"
```

---

## Task 5: Compose wiring + verification

Add `alerter` to `docker-compose.yml`, bring it up alongside the existing services, and verify the alerter starts cleanly. Real Twilio messages won't fire yet because Phase 1a's threshold-passing events are rare — Task 6's smoke tool is the next test.

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add `alerter` to `docker-compose.yml`**

After the existing `scorer:` block (at the same indentation level under `services:`), append:

```yaml
  alerter:
    build:
      context: .
      dockerfile: services/alerter/Dockerfile
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
      TWILIO_ACCOUNT_SID: "${TWILIO_ACCOUNT_SID}"
      TWILIO_AUTH_TOKEN: "${TWILIO_AUTH_TOKEN}"
      TWILIO_FROM: "${TWILIO_FROM}"
      ALERT_RECIPIENT: "${ALERT_RECIPIENT}"
      ALERT_CHANNEL: "${ALERT_CHANNEL:-whatsapp}"
      ALERT_THRESHOLD: "${ALERT_THRESHOLD:-4}"
      MIN_CONFIDENCE: "${MIN_CONFIDENCE:-0.6}"
    restart: unless-stopped
```

- [ ] **Step 2: Build and bring up the alerter**

```bash
docker compose build alerter
docker compose up -d
```

Then `docker compose ps` — expect kafka + postgres healthy; kafka-init + migrate exited 0; ingestor-cnbc + scorer + **alerter** all `Up`.

- [ ] **Step 3: Verify the alerter is alive and waiting**

```bash
docker compose logs alerter --tail 20
```

Expected: `{"event": "starting alerter", ...}` followed by `{"event": "alerter config", "channel": "whatsapp", "threshold": 4, ...}`. No exceptions, no restart loop.

If the alerter is restarting, check the logs for missing env vars (most likely `ALERT_RECIPIENT` empty in `.env`) and report back.

- [ ] **Step 4: Verify Phase 1a tests still pass with the new service running**

```bash
pytest -v
```

Expected: 52 (Phase 1a) + 28 (Phase 1b unit tests so far: 6 format + 16 twilio_client + 6 main) = ~80 PASSED.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(compose): add alerter service"
```

---

## Task 6: Smoke tool (tools/alerter_smoke.py)

One-shot real-Twilio test. Sends a single hardcoded WhatsApp message to `ALERT_RECIPIENT` to verify credentials + sandbox opt-in + network end-to-end. Cost: ~$0.005.

**Files:**
- Create: `tools/alerter_smoke.py`

- [ ] **Step 1: Implement `tools/alerter_smoke.py`**

```python
"""alerter_smoke.py — one-shot end-to-end test against the real Twilio API.

Sends a single hardcoded WhatsApp message to ALERT_RECIPIENT.
Cost: ~$0.005. Not run in CI.

Usage:
    # Bash: source env vars from .env and run
    set -a; source .env; set +a
    python tools/alerter_smoke.py

Requires: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM, ALERT_RECIPIENT in env.
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.alerter.twilio_client import build_client, send_message
from services.shared.logging import configure_logging, get_logger


SMOKE_BODY = (
    "[smoke test 4/10 → neutral · conf 100%]\n"
    "headline-alerter · {ts}Z\n"
    "\n"
    "Phase 1b smoke test\n"
    "\n"
    "If you see this, your Twilio credentials work and the WhatsApp Sandbox "
    "opt-in succeeded. Reply anything to this message to keep the 24h session "
    "window open.\n"
)


def main() -> int:
    configure_logging("alerter-smoke")
    log = get_logger()

    recipient = os.environ.get("ALERT_RECIPIENT")
    from_number = os.environ.get("TWILIO_FROM")
    if not recipient or not from_number:
        log.error("ALERT_RECIPIENT and TWILIO_FROM env vars are required")
        return 1

    log.info("sending smoke message", recipient=recipient[:14] + "...",
             from_number=from_number)
    client = build_client()
    body = SMOKE_BODY.format(ts=datetime.now(timezone.utc).strftime("%H:%M"))
    sid = send_message(client, channel="whatsapp",
                       to=recipient, from_number=from_number, body=body)

    print(f"OK — Phase 1b smoke test passed (twilio_sid: {sid})")
    print("Check your phone — the message should arrive within 5 seconds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the smoke test**

Make sure `.env` has the user's Twilio credentials and that they've already done the sandbox `join <code>` ritual (see README runbook from Task 1).

```bash
set -a; source .env; set +a
python tools/alerter_smoke.py
```

Expected stdout:
```
OK — Phase 1b smoke test passed (twilio_sid: SM...)
Check your phone — the message should arrive within 5 seconds.
```

The user's phone should buzz with the smoke message. If it doesn't:
- Check `docker compose ps` — alerter doesn't matter for the smoke test, but confirms env vars
- Check `Twilio Console → Monitor → Logs → Errors` for the message status
- Most common failure: `code=63007 alerter_recipient_not_opted_in` → user hasn't sent the join code from their phone

- [ ] **Step 3: Commit**

```bash
git add tools/alerter_smoke.py
git commit -m "feat(tools): alerter_smoke.py — one-shot real-twilio verification"
```

---

## Task 7: Integration test (tests/integration/test_phase1b_e2e.py)

Real Kafka + real Postgres, mocked Twilio (via `TWILIO_FAKE=1`). Three tests:
- success path writes alert_history + flips events_archive + produces audit msg
- idempotency — second call for same event_id is a no-op
- forced throttle failure produces a DLQ row and the event_archive row is NOT flipped

**Files:**
- Create: `tests/integration/test_phase1b_e2e.py`

- [ ] **Step 1: Write the integration test**

```python
"""Phase 1b end-to-end integration test.

Real Kafka + real Postgres, in-process alerter. Twilio is faked via
TWILIO_FAKE=1 (set per-test).

Requires: docker compose up -d (kafka + postgres + migrate done).
"""
import json
import time
import uuid
from datetime import datetime, timezone

import pytest
from confluent_kafka import Consumer

from services.alerter.main import process_one_alert
from services.alerter.twilio_client import build_client
from services.shared.db import connect
from services.shared.kafka_client import make_producer
from services.shared.logging import configure_logging, get_logger
from services.shared.models import NormalizedEvent


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9094")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://rates:changeme@localhost:5432/rates")
    monkeypatch.setenv("TWILIO_FAKE", "1")
    yield


def _seed_received_event(event_id: str, headline: str = "test alert"):
    """Seed an events_archive row in 'scored' status that the alerter can find."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, ts_ingested, ts_scored, status,
                   headline, body, url, metadata,
                   score, direction, confidence, reasoning, model)
                VALUES (%s, %s, %s, %s, %s, 'scored',
                        %s, %s, %s, %s::jsonb,
                        %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event_id, "cnbc_rss",
                    datetime.now(timezone.utc), datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    headline, "body", "https://example.com/x", "{}",
                    7, "rates_lower", 0.72, "test reasoning", "claude-haiku-4-5",
                ),
            )
        conn.commit()


def _scored_dict(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "score": 7,
        "direction": "rates_lower",
        "confidence": 0.72,
        "reasoning": "test reasoning",
        "model": "claude-haiku-4-5",
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }


def _archive_status(event_id: str) -> str | None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM events_archive WHERE id = %s", (event_id,))
            row = cur.fetchone()
            return row[0] if row else None


def _alert_history_count(event_id: str) -> int:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM alert_history WHERE event_id = %s", (event_id,))
            return cur.fetchone()[0]


# ---- success path --------------------------------------------------------

def test_success_writes_alert_history_and_flips_archive(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-success-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id, headline="Powell dovish pivot")

    process_one_alert(
        _scored_dict(event_id),
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    assert _archive_status(event_id) == "alerted"
    assert _alert_history_count(event_id) == 1


# ---- idempotency --------------------------------------------------------

def test_idempotency_second_call_is_no_op(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-idem-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)
    kwargs = dict(
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    # First call: alerts.
    process_one_alert(_scored_dict(event_id), **kwargs)
    first_count = _alert_history_count(event_id)
    assert first_count == 1

    # Second call: no-op (idempotency check kicks in).
    process_one_alert(_scored_dict(event_id), **kwargs)
    second_count = _alert_history_count(event_id)
    assert second_count == 1, "alert_history should not have grown on the second call"


# ---- below threshold -----------------------------------------------------

def test_below_threshold_writes_nothing(env):
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-low-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)

    low = _scored_dict(event_id) | {"score": 2}
    process_one_alert(
        low,
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    assert _archive_status(event_id) == "scored"  # not flipped
    assert _alert_history_count(event_id) == 0


# ---- DLQ on throttle -----------------------------------------------------

def test_throttle_failure_routes_to_dlq(env, monkeypatch):
    monkeypatch.setenv("TWILIO_FAIL_MODE", "throttle")
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    configure_logging("itest")
    log = get_logger()
    event_id = f"itest-dlq-{uuid.uuid4().hex[:8]}"
    _seed_received_event(event_id)

    process_one_alert(
        _scored_dict(event_id),
        twilio_client=build_client(),
        producer=make_producer(),
        log=log,
        channel="whatsapp",
        recipient="whatsapp:+44test",
        from_number="whatsapp:+14155238886",
        threshold=4,
        min_confidence=0.6,
    )

    # Archive NOT flipped, alert_history empty.
    assert _archive_status(event_id) == "scored"
    assert _alert_history_count(event_id) == 0

    # DLQ row present with stage='alerter_throttle'.
    consumer = Consumer({
        "bootstrap.servers": "localhost:9094",
        "group.id": f"itest-alerter-dlq-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["events.dlq"])
    found = None
    deadline = time.time() + 15
    while time.time() < deadline and not found:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            p = json.loads(msg.value().decode("utf-8"))
            if p.get("original_event", {}).get("event_id") == event_id:
                found = p
                break
    consumer.close()

    assert found is not None
    assert found["stage"] == "alerter_throttle"
    assert found["service"] == "alerter"
```

- [ ] **Step 2: Run the integration test**

Ensure the stack is up:
```bash
docker compose up -d
```

Then:
```bash
pytest tests/integration/test_phase1b_e2e.py -v
```

Expected: 4 PASSED. Total runtime ~10–20s.

- [ ] **Step 3: Run the FULL test suite**

```bash
pytest -v
```

Expected: all green. Approximate breakdown:
- Phase 0 baseline: 10
- Phase 1a unit + integration: 39 + 2 + 1 (added in fix) = 42
- Phase 1b unit: 28
- Phase 1b integration: 4
- Total: ~84 tests

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_phase1b_e2e.py
git commit -m "test(integration): phase 1b end-to-end (real kafka+pg, mocked twilio)"
```

---

## Task 8: Acceptance run-through

Verify all 8 acceptance criteria from spec § 9. No new files; document the verification ritual.

- [ ] **Step 1: Bring stack up cleanly (criterion 1)**

```bash
docker compose down
docker compose up -d
sleep 30
docker compose ps
```

Expected: kafka + postgres healthy; kafka-init + migrate exited 0; ingestor-cnbc + scorer + alerter Up.

- [ ] **Step 2: Smoke test puts a real WhatsApp on the phone (criterion 2)**

```bash
set -a; source .env; set +a
python tools/alerter_smoke.py
```

Expected: prints `OK — Phase 1b smoke test passed (twilio_sid: SM...)` AND the user's phone receives the smoke message within 5s.

If the message does not land, this criterion fails. Most common cause: user hasn't done the WhatsApp Sandbox `join <code>` ritual. Refer them back to the README runbook from Task 1.

- [ ] **Step 3: Real high-scoring CNBC event triggers a real WhatsApp (criterion 3)**

Inject one synthetic high-score event end-to-end (PG row + Kafka produce in one script). The running alerter consumes the Kafka message and fires WhatsApp:

```bash
python -c "
import os, uuid
os.environ['KAFKA_BROKERS'] = 'localhost:9094'
os.environ.setdefault('POSTGRES_URL', 'postgresql://rates:changeme@localhost:5432/rates')

from datetime import datetime, timezone
from services.shared.db import connect
from services.shared.kafka_client import make_producer, produce, flush

event_id = f'itest-real-{uuid.uuid4().hex[:8]}'

# 1. Seed events_archive row in 'scored' status (what the alerter expects).
with connect() as conn:
    with conn.cursor() as cur:
        cur.execute(
            \"\"\"
            INSERT INTO events_archive (id, source, ts_source, ts_ingested, ts_scored,
                                        status, headline, body, url, metadata,
                                        score, direction, confidence, reasoning, model)
            VALUES (%s, 'cnbc_rss', NOW(), NOW(), NOW(),
                    'scored', 'Test Phase 1b acceptance', 'body', 'https://example.com',
                    '{}'::jsonb, 7, 'rates_lower', 0.72, 'test reasoning', 'claude-haiku-4-5')
            \"\"\",
            (event_id,),
        )
    conn.commit()

# 2. Produce the matching ScoredEvent to events.scored.
p = make_producer()
produce(p, 'events.scored', key=event_id, payload={
    'event_id': event_id, 'score': 7, 'direction': 'rates_lower',
    'confidence': 0.72, 'reasoning': 'test reasoning',
    'model': 'claude-haiku-4-5',
    'scored_at': datetime.now(timezone.utc).isoformat(),
})
flush(p)
print(f'produced event_id={event_id}; check phone for whatsapp message within ~10s')
"
```

Expected: console prints `produced event_id=itest-real-<hex>`. The user's phone receives a WhatsApp message within ~10s. Verify:

```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT id, status FROM events_archive WHERE id LIKE 'itest-real-%' ORDER BY ts_ingested DESC LIMIT 1;"
```

Expected: `status='alerted'`.

- [ ] **Step 4: Below-threshold events skipped silently (criterion 4)**

```bash
docker compose logs alerter --tail 50 | grep -E '"event": "below threshold; skip"'
```

Expected: at least one log line where the alerter saw a low-score event and skipped. (CNBC's natural traffic at threshold=4 will produce these regularly.)

If no skipped events appear in logs, the alerter may not have seen any below-threshold events yet; wait a few minutes and re-check.

- [ ] **Step 5: Replay produces zero new messages (criterion 5)**

```bash
# Reset the consumer group offset to earliest
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group alerter-cg --topic events.scored \
  --reset-offsets --to-earliest --execute

# Note alert_history count BEFORE
BEFORE=$(docker compose exec -T postgres psql -U rates -d rates -t -c "SELECT count(*) FROM alert_history;" | tr -d ' \n')
echo "before: $BEFORE"

# Wait 60s for the alerter to chew through the replay
sleep 60

# Note alert_history count AFTER
AFTER=$(docker compose exec -T postgres psql -U rates -d rates -t -c "SELECT count(*) FROM alert_history;" | tr -d ' \n')
echo "after: $AFTER"
```

Expected: `BEFORE == AFTER` (no new alert_history rows from the replay). Phone should NOT have received any duplicate messages.

- [ ] **Step 6: Forced throttle failure routes to DLQ (criterion 6)**

The integration test from Task 7 (`test_throttle_failure_routes_to_dlq`) verified this against the in-process alerter. Re-run it to confirm:

```bash
pytest tests/integration/test_phase1b_e2e.py::test_throttle_failure_routes_to_dlq -v
```

Expected: PASSED.

Also confirm the alerter is still up after the test:
```bash
docker compose ps alerter
```

Expected: `Up`, not in restart loop.

- [ ] **Step 7: alert_history schema integrity (criterion 7)**

```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT event_id IS NOT NULL AS has_event_id,
          channel IS NOT NULL AS has_channel,
          recipient IS NOT NULL AS has_recipient,
          twilio_sid IS NOT NULL AS has_sid,
          sent_at IS NOT NULL AS has_sent_at,
          delivery_status
   FROM alert_history ORDER BY sent_at DESC LIMIT 5;"
```

Expected: every row has all `has_*` columns = `t`, `delivery_status='queued'`.

- [ ] **Step 8: Full test suite passes (criterion 8)**

```bash
pytest -v 2>&1 | tail -10
```

Expected: ~84 PASSED, no failures.

- [ ] **Step 9: Phase 1b complete — final commit**

If any incidental fixes were made during acceptance (e.g., README typo), commit them. Otherwise no commit needed; Phase 1b was committed task by task.

```bash
git status
```

Expected: clean working tree.

---

## What this phase does NOT cover (intentional)

These are deferred to later phases:

- **Cross-source dedup, cooldowns, per-source thresholds** — parent spec § 9. We accept duplicate-headline alerts as a known v1 limitation, expecting real traffic to inform the right dedup model.
- **Twilio delivery webhooks** — needs a public URL (Cloudflare Tunnel / VPS). `delivery_status='queued'` is captured at send-time only.
- **WhatsApp template registration** for >24h-silent windows — we DLQ the failure as `alerter_whatsapp_template`. If real-traffic frequency makes this annoying, register a utility template (separate spec).
- **Phase 1c dashboard** — FastAPI + SSE web UI for live event viewing.
- **Phase 2+ ingestors** — `bls_rss`, `treasury_rss`, `truth_social`, `x_curated`.

Phase 1b's deliverable is end-to-end SMS-grade alerting on a personal phone. Anything beyond that is gold-plating that the parent spec deliberately defers.
