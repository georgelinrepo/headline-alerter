# Macro Context Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Inject a nightly AI-generated macro summary (Fed stance, yields, dominant themes, key events) into the scorer's system prompt so Claude scores events with full regime awareness.

**Architecture:** A background thread inside the scorer wakes at midnight ET, calls Claude Sonnet with web search tools to synthesise a macro summary, stores it in Postgres, and updates the in-memory system prompt. On startup the scorer loads the latest stored context. Every scoring call uses the enriched prompt via a new `system_prompt` parameter threaded through `score_event()` and `process_one_event()`.

**Tech Stack:** anthropic SDK (`web_search_20250305` tool), psycopg3, zoneinfo + tzdata, threading.Lock

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `migrations/002_macro_context.sql` | Create | `macro_context` table |
| `services/shared/macro_context.py` | Create | `get_latest_context(conn)`, `save_context(conn, summary, model)` |
| `services/scorer/context_builder.py` | Create | `build_macro_context(client, model, today)`, `_seconds_until_midnight_et(now=None)` |
| `services/shared/scorer_prompts.py` | Modify | Add `build_system_prompt(macro_context=None)` |
| `services/shared/anthropic_client.py` | Modify | Add `system_prompt` param to `score_event()` |
| `services/scorer/main.py` | Modify | Lock, `get/set_system_prompt`, refresh thread, startup load, pass `system_prompt` |
| `services/scorer/Dockerfile` | Modify | Add `tzdata` to pip install |
| `docker-compose.yml` | Modify | Add `CONTEXT_MODEL` env var |
| `pyproject.toml` | Modify | Add `tzdata` to dependencies |
| `tests/unit/shared/test_macro_context.py` | Create | Unit tests for DB helpers |
| `tests/unit/scorer/test_context_builder.py` | Create | Unit tests for builder and midnight calc |

---

## Task 1: Migration — `macro_context` table

**Files:**
- Create: `migrations/002_macro_context.sql`

- [ ] **Step 1.1: Create the migration file**

Create `migrations/002_macro_context.sql`:

```sql
-- 002_macro_context.sql
-- depends: 001_initial

CREATE TABLE macro_context (
  id           SERIAL PRIMARY KEY,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  summary      TEXT NOT NULL,
  model        TEXT NOT NULL
);

-- !rollback DROP TABLE IF EXISTS macro_context;
```

- [ ] **Step 1.2: Apply the migration**

```bash
docker compose run --rm migrate
```

Expected: exits 0 with no errors.

- [ ] **Step 1.3: Verify the table exists**

```bash
docker compose exec postgres psql -U rates -d rates -c "\d macro_context"
```

Expected: table with columns `id`, `generated_at`, `summary`, `model`.

- [ ] **Step 1.4: Commit**

```bash
cd /home/dev/projects/headline-alerter
git add migrations/002_macro_context.sql
git commit -m "feat(db): add macro_context table for nightly scorer context"
```

---

## Task 2: DB helpers — `services/shared/macro_context.py`

**Files:**
- Create: `services/shared/macro_context.py`
- Create: `tests/unit/shared/test_macro_context.py`

- [ ] **Step 2.1: Write failing tests**

Create `tests/unit/shared/test_macro_context.py`:

```python
"""Unit tests for macro_context DB helpers.

Integration tests (require POSTGRES_URL) are marked with pytest.mark.skipif.
"""
from __future__ import annotations
import os
import pytest
from unittest.mock import MagicMock, call


# ---------------------------------------------------------------------------
# Unit: mock-based tests (no Postgres required)
# ---------------------------------------------------------------------------

def test_get_latest_context_returns_none_when_no_rows():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = None

    result = get_latest_context(conn)
    assert result is None


def test_get_latest_context_returns_summary_when_row_exists():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = ("**US Macro Context — 2026-05-16**\n\nFed holds at 3.5%",)

    result = get_latest_context(conn)
    assert result == "**US Macro Context — 2026-05-16**\n\nFed holds at 3.5%"


def test_get_latest_context_queries_correct_sql():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = None

    get_latest_context(conn)
    sql = cur.execute.call_args[0][0]
    assert "ORDER BY generated_at DESC" in sql
    assert "LIMIT 1" in sql


def test_save_context_inserts_row():
    from services.shared.macro_context import save_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value

    save_context(conn, "some summary", "claude-sonnet-4-6")

    sql, params = cur.execute.call_args[0]
    assert "INSERT INTO macro_context" in sql
    assert "some summary" in params
    assert "claude-sonnet-4-6" in params
    conn.commit.assert_called_once()
```

- [ ] **Step 2.2: Run tests — confirm ImportError**

```bash
cd /home/dev/projects/headline-alerter
pytest tests/unit/shared/test_macro_context.py -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'get_latest_context'`

- [ ] **Step 2.3: Implement `services/shared/macro_context.py`**

```python
"""Postgres helpers for the macro_context table."""
from __future__ import annotations
import psycopg


def get_latest_context(conn: psycopg.Connection) -> str | None:
    """Return the most recent macro summary, or None if the table is empty."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT summary FROM macro_context ORDER BY generated_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        return row[0] if row else None


def save_context(conn: psycopg.Connection, summary: str, model: str) -> None:
    """Insert a new macro context row."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO macro_context (summary, model) VALUES (%s, %s)",
            (summary, model),
        )
    conn.commit()
```

- [ ] **Step 2.4: Run tests — confirm all pass**

```bash
pytest tests/unit/shared/test_macro_context.py -v
```

Expected: 4 PASS.

- [ ] **Step 2.5: Commit**

```bash
git add services/shared/macro_context.py tests/unit/shared/test_macro_context.py
git commit -m "feat(shared): add macro_context DB helpers (get_latest_context, save_context)"
```

---

## Task 3: Context builder — `services/scorer/context_builder.py`

**Files:**
- Create: `services/scorer/context_builder.py`
- Create: `tests/unit/scorer/test_context_builder.py`

- [ ] **Step 3.1: Write failing tests**

Create `tests/unit/scorer/test_context_builder.py`:

```python
"""Unit tests for context_builder.py."""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# _seconds_until_midnight_et
# ---------------------------------------------------------------------------

def test_seconds_until_midnight_et_from_noon():
    from services.scorer.context_builder import _seconds_until_midnight_et
    import zoneinfo
    et = zoneinfo.ZoneInfo("America/New_York")
    # 12:00 ET → midnight is 12h away = 43200s
    noon_et = datetime(2026, 5, 16, 12, 0, 0, tzinfo=et)
    delay = _seconds_until_midnight_et(now=noon_et)
    assert abs(delay - 43200) < 2


def test_seconds_until_midnight_et_from_11pm():
    from services.scorer.context_builder import _seconds_until_midnight_et
    import zoneinfo
    et = zoneinfo.ZoneInfo("America/New_York")
    # 23:00 ET → midnight is 1h away = 3600s
    late_et = datetime(2026, 5, 16, 23, 0, 0, tzinfo=et)
    delay = _seconds_until_midnight_et(now=late_et)
    assert abs(delay - 3600) < 2


def test_seconds_until_midnight_et_never_negative():
    from services.scorer.context_builder import _seconds_until_midnight_et
    delay = _seconds_until_midnight_et()
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
```

- [ ] **Step 3.2: Run tests — confirm ImportError**

```bash
pytest tests/unit/scorer/test_context_builder.py -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'build_macro_context'`

- [ ] **Step 3.3: Implement `services/scorer/context_builder.py`**

```python
"""Nightly macro context builder.

Calls Claude Sonnet with Anthropic web search to synthesise a macro summary
that is injected into the scorer system prompt.
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Any
import zoneinfo

_ET = zoneinfo.ZoneInfo("America/New_York")

CONTEXT_SEARCH_PROMPT = """\
Today is {date}. You are building a daily macro context summary for a US \
interest rates trader who watches SOFR futures, Treasury futures, and yields.

Search for information on these topics IN ORDER — start broad to catch regime \
events, then fill in specifics:

1. What are the dominant macro themes driving US interest rates right now? \
(Search this first — it catches wars, crises, central-bank transitions that \
narrow searches would miss.)
2. What is the current Federal Reserve policy stance, rate level, and who is \
the Chair?
3. What are current US Treasury yield levels for the 2y, 10y, and 30y?
4. What were the most recent major US economic data prints (CPI, NFP, PCE) \
and how did they compare to consensus?
5. What major economic events or Fed speakers are scheduled this week?

Synthesise your findings into this exact structure:

**US Macro Context — {date}**

**Dominant Themes:** [regime events, wars, crises, structural shifts driving rates]
**Fed Stance:** [rate level, direction, chair, recent dissents, next meeting date]
**Rates:** [2y, 10y, 30y levels, recent bp moves, curve shape]
**Recent Data:** [CPI, NFP, PCE prints with surprises vs consensus]
**This Week:** [scheduled events, Fed speakers, major auctions]

Be specific with numbers. This context will be injected into a scoring prompt \
for a live rates trading system.
"""


def _seconds_until_midnight_et(now: datetime | None = None) -> float:
    """Return seconds from now until next midnight ET."""
    if now is None:
        now = datetime.now(_ET)
    else:
        now = now.astimezone(_ET)
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return (next_midnight - now).total_seconds()


def build_macro_context(client: Any, model: str, today: str) -> str:
    """Call Anthropic Sonnet with web search to synthesise a macro summary.

    Raises ValueError if the response contains no text blocks.
    """
    response = client.messages.create(
        model=model,
        max_tokens=1500,
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}],
        messages=[{
            "role": "user",
            "content": CONTEXT_SEARCH_PROMPT.format(date=today),
        }],
    )
    texts = [
        b.text for b in response.content
        if hasattr(b, "text") and b.text and b.text.strip()
    ]
    if not texts:
        raise ValueError("context builder got no text from Anthropic response")
    return "\n\n".join(texts).strip()
```

- [ ] **Step 3.4: Run tests — confirm all pass**

```bash
pytest tests/unit/scorer/test_context_builder.py -v
```

Expected: 7 PASS.

- [ ] **Step 3.5: Commit**

```bash
git add services/scorer/context_builder.py tests/unit/scorer/test_context_builder.py
git commit -m "feat(scorer): add context_builder with web search synthesis and midnight ET timer"
```

---

## Task 4: Dynamic system prompt

**Files:**
- Modify: `services/shared/scorer_prompts.py`
- Modify: `services/shared/anthropic_client.py`
- Modify: `tests/unit/scorer/test_prompts.py` (add tests)
- Modify: `tests/unit/shared/test_anthropic_client.py` (add test)

- [ ] **Step 4.1: Write failing tests for `build_system_prompt`**

Add to `tests/unit/scorer/test_prompts.py`:

```python
# Add these tests to the existing test_prompts.py file

def test_build_system_prompt_no_context_returns_bare_prompt():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt()
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["text"] == SYSTEM_PROMPT
    assert blocks[0]["cache_control"] == {"type": "ephemeral"}


def test_build_system_prompt_with_context_prepends_xml_block():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt(macro_context="Fed holds at 3.5%.")
    assert len(blocks) == 1
    text = blocks[0]["text"]
    assert text.startswith("<macro_context>")
    assert "Fed holds at 3.5%." in text
    assert "</macro_context>" in text
    assert SYSTEM_PROMPT in text


def test_build_system_prompt_none_context_returns_bare_prompt():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt(macro_context=None)
    assert blocks[0]["text"] == SYSTEM_PROMPT
```

- [ ] **Step 4.2: Run new prompt tests — confirm failure**

```bash
pytest tests/unit/scorer/test_prompts.py::test_build_system_prompt_no_context_returns_bare_prompt -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'build_system_prompt'`

- [ ] **Step 4.3: Add `build_system_prompt` to `services/shared/scorer_prompts.py`**

Append to the bottom of `services/shared/scorer_prompts.py`:

```python

def build_system_prompt(macro_context: str | None = None) -> list[dict]:
    """Build the scorer system prompt cache block, optionally with macro context.

    When macro_context is provided it is prepended as a <macro_context> XML block
    before the scoring rubric so Claude has regime awareness.
    """
    if macro_context:
        text = f"<macro_context>\n{macro_context}\n</macro_context>\n\n{SYSTEM_PROMPT}"
    else:
        text = SYSTEM_PROMPT
    return [{"type": "text", "text": text, "cache_control": {"type": "ephemeral"}}]
```

- [ ] **Step 4.4: Write failing test for `score_event` system_prompt param**

Add to `tests/unit/shared/test_anthropic_client.py`:

```python
def test_score_event_uses_custom_system_prompt(monkeypatch):
    """score_event passes system_prompt kwarg to client.messages.create when provided."""
    from services.shared.anthropic_client import score_event
    from services.shared.models import NormalizedEvent
    from datetime import datetime, timezone

    monkeypatch.setattr("services.shared.anthropic_client.time.sleep", lambda s: None)

    block = MagicMock()
    block.type = "tool_use"
    block.input = {
        "score": 5, "direction": "neutral",
        "confidence": 0.5, "reasoning": "test"
    }
    response = MagicMock()
    response.content = [block]
    client = MagicMock()
    client.messages.create.return_value = response

    event = NormalizedEvent(
        event_id="e1", source="test",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="test", body="body", url="", metadata={},
    )
    custom_prompt = [{"type": "text", "text": "custom", "cache_control": {"type": "ephemeral"}}]

    score_event(client, normalized_event=event, model="m", system_prompt=custom_prompt)

    call_kwargs = client.messages.create.call_args[1]
    assert call_kwargs["system"] == custom_prompt
```

- [ ] **Step 4.5: Run test — confirm failure**

```bash
pytest tests/unit/shared/test_anthropic_client.py::test_score_event_uses_custom_system_prompt -v 2>&1 | head -20
```

Expected: `TypeError: score_event() got an unexpected keyword argument 'system_prompt'`

- [ ] **Step 4.6: Add `system_prompt` param to `score_event` in `anthropic_client.py`**

Change the `score_event` signature and the `client.messages.create` call:

Old signature:
```python
def score_event(
    client,
    *,
    normalized_event: NormalizedEvent,
    model: str,
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
) -> ScoredEvent:
```

New signature (add `system_prompt` param):
```python
def score_event(
    client,
    *,
    normalized_event: NormalizedEvent,
    model: str,
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
    system_prompt: list | None = None,
) -> ScoredEvent:
```

Change the `client.messages.create` call inside `score_event` (find the line `system=SYSTEM_PROMPT_CACHE_BLOCK,` and replace it):

Old:
```python
                system=SYSTEM_PROMPT_CACHE_BLOCK,
```

New:
```python
                system=system_prompt if system_prompt is not None else SYSTEM_PROMPT_CACHE_BLOCK,
```

- [ ] **Step 4.7: Run all prompt and anthropic_client tests**

```bash
pytest tests/unit/scorer/test_prompts.py tests/unit/shared/test_anthropic_client.py -v
```

Expected: all PASS.

- [ ] **Step 4.8: Commit**

```bash
git add services/shared/scorer_prompts.py services/shared/anthropic_client.py \
        tests/unit/scorer/test_prompts.py tests/unit/shared/test_anthropic_client.py
git commit -m "feat(shared): add build_system_prompt and system_prompt param to score_event"
```

---

## Task 5: Scorer integration — startup load + midnight refresh thread

**Files:**
- Modify: `services/scorer/main.py`
- Modify: `tests/unit/scorer/test_main.py` (add tests)

- [ ] **Step 5.1: Write failing tests**

Add to `tests/unit/scorer/test_main.py`:

```python
# Add these tests to the existing test_main.py file

def test_process_one_event_passes_system_prompt_to_score_event(
    fake_pg, normalized_event_dict, monkeypatch
):
    """process_one_event forwards system_prompt to score_event."""
    captured = {}

    def fake_score_event(client, *, normalized_event, model, timeout_seconds, system_prompt=None):
        from services.shared.models import ScoredEvent
        from datetime import datetime, timezone
        captured["system_prompt"] = system_prompt
        return ScoredEvent(
            event_id=normalized_event.event_id,
            score=7, direction="rates_lower", confidence=0.72,
            reasoning="test", model=model,
            scored_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr("services.scorer.main.score_event", fake_score_event)

    from services.scorer.main import process_one_event
    custom_prompt = [{"type": "text", "text": "custom context"}]
    process_one_event(
        normalized_event_dict,
        anthropic_client=MagicMock(),
        producer=MagicMock(),
        log=MagicMock(),
        model="m",
        system_prompt=custom_prompt,
    )
    assert captured["system_prompt"] == custom_prompt


def test_get_set_system_prompt_thread_safe():
    """get_system_prompt returns whatever set_system_prompt last set."""
    from services.scorer.main import get_system_prompt, set_system_prompt
    prompt = [{"type": "text", "text": "test"}]
    set_system_prompt(prompt)
    assert get_system_prompt() == prompt
```

- [ ] **Step 5.2: Run new tests — confirm failure**

```bash
pytest tests/unit/scorer/test_main.py::test_process_one_event_passes_system_prompt_to_score_event \
       tests/unit/scorer/test_main.py::test_get_set_system_prompt_thread_safe -v 2>&1 | head -20
```

Expected: failures (missing `get_system_prompt`, `set_system_prompt`, and `system_prompt` param).

- [ ] **Step 5.3: Update `services/scorer/main.py`**

Add the following imports at the top of `main.py` (after the existing imports):

```python
import threading
from datetime import datetime, timezone

from services.scorer.context_builder import _seconds_until_midnight_et, build_macro_context
from services.shared.macro_context import get_latest_context, save_context
from services.shared.scorer_prompts import build_system_prompt
```

Add the thread-safe prompt state after the imports (before `update_archive_with_score`):

```python
# ---- Thread-safe macro context prompt ------------------------------------

_prompt_lock = threading.Lock()
_current_system_prompt: list | None = None


def get_system_prompt() -> list | None:
    with _prompt_lock:
        return _current_system_prompt


def set_system_prompt(prompt: list) -> None:
    global _current_system_prompt
    with _prompt_lock:
        _current_system_prompt = prompt
```

Update `process_one_event` signature (add `system_prompt` param) and the `score_event` call inside it:

Old signature:
```python
def process_one_event(event_dict: dict[str, Any], *, anthropic_client,
                      producer, log, model: str,
                      timeout_seconds: int = 30) -> None:
```

New:
```python
def process_one_event(event_dict: dict[str, Any], *, anthropic_client,
                      producer, log, model: str,
                      timeout_seconds: int = 30,
                      system_prompt: list | None = None) -> None:
```

Old `score_event` call inside `process_one_event`:
```python
        scored = score_event(
            anthropic_client,
            normalized_event=event,
            model=model,
            timeout_seconds=timeout_seconds,
        )
```

New:
```python
        scored = score_event(
            anthropic_client,
            normalized_event=event,
            model=model,
            timeout_seconds=timeout_seconds,
            system_prompt=system_prompt,
        )
```

Add startup load and refresh thread functions before `main()`:

```python
def _load_initial_context(log) -> None:
    """Load the most recent macro context from Postgres on startup."""
    try:
        with connect() as conn:
            summary = get_latest_context(conn)
        if summary:
            set_system_prompt(build_system_prompt(summary))
            log.info("macro context loaded", chars=len(summary))
        else:
            log.info("no macro context found, using bare system prompt")
    except Exception as e:
        log.warning("failed to load macro context on startup", error=str(e))


def _context_refresh_loop(anthropic_client, context_model: str, log) -> None:
    """Background daemon: rebuild macro context at midnight ET each night."""
    while True:
        delay = _seconds_until_midnight_et()
        log.info("context refresh sleeping until midnight ET",
                 seconds=int(delay))
        time.sleep(delay)
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            summary = build_macro_context(anthropic_client, context_model, today)
            with connect() as conn:
                save_context(conn, summary, context_model)
            set_system_prompt(build_system_prompt(summary))
            log.info("macro context refreshed", model=context_model,
                     chars=len(summary))
        except Exception as e:
            log.error("context refresh failed", error=str(e))
```

Update `main()` to load initial context, start the refresh thread, and pass `system_prompt` to `process_one_event`. Find these lines in `main()`:

```python
    client = build_anthropic_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.normalized"])
```

Replace with:

```python
    client = build_anthropic_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.normalized"])

    context_model = os.environ.get("CONTEXT_MODEL", "claude-sonnet-4-6")
    _load_initial_context(log)
    t = threading.Thread(
        target=_context_refresh_loop,
        args=(client, context_model, log),
        daemon=True,
    )
    t.start()
```

Update the `process_one_event` call inside the consumer loop:

Old:
```python
            process_one_event(
                payload,
                anthropic_client=client,
                producer=producer,
                log=log,
                model=model,
                timeout_seconds=timeout_s,
            )
```

New:
```python
            process_one_event(
                payload,
                anthropic_client=client,
                producer=producer,
                log=log,
                model=model,
                timeout_seconds=timeout_s,
                system_prompt=get_system_prompt(),
            )
```

- [ ] **Step 5.4: Run all scorer unit tests**

```bash
pytest tests/unit/scorer/ -v
```

Expected: all PASS.

- [ ] **Step 5.5: Run full unit test suite**

```bash
pytest tests/unit/ -v
```

Expected: all PASS.

- [ ] **Step 5.6: Commit**

```bash
git add services/scorer/main.py tests/unit/scorer/test_main.py
git commit -m "feat(scorer): add macro context startup load and midnight ET refresh thread"
```

---

## Task 6: Dockerfile, dependencies, docker-compose

**Files:**
- Modify: `services/scorer/Dockerfile`
- Modify: `pyproject.toml`
- Modify: `docker-compose.yml`

- [ ] **Step 6.1: Add `tzdata` to scorer Dockerfile**

In `services/scorer/Dockerfile`, find the `pip install` line and add `tzdata`:

```dockerfile
RUN pip install --no-cache-dir \
        "anthropic>=0.40.0" \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "tzdata>=2024.1"
```

(Add `"tzdata>=2024.1"` to whatever pip install block already exists in the Dockerfile.)

- [ ] **Step 6.2: Add `tzdata` to `pyproject.toml`**

In `pyproject.toml`, find the `dependencies` list and add `"tzdata>=2024.1"`.

- [ ] **Step 6.3: Add `CONTEXT_MODEL` to `docker-compose.yml`**

In the `scorer` service environment block, add:

```yaml
      CONTEXT_MODEL: "${CONTEXT_MODEL:-claude-sonnet-4-6}"
```

- [ ] **Step 6.4: Build and restart the scorer**

```bash
cd /home/dev/projects/headline-alerter
docker compose up --build scorer -d
```

- [ ] **Step 6.5: Verify startup log shows context load**

```bash
docker compose logs scorer --tail=30
```

Expected: log line containing `"macro context loaded"` (if a context row exists) or `"no macro context found, using bare system prompt"` (first run), followed by `"context refresh sleeping until midnight ET"`.

- [ ] **Step 6.6: Smoke test — manually trigger a context build**

Run from the repo root to build and store a real context (this makes one Sonnet call with web search, ~$0.01):

```bash
ANTHROPIC_API_KEY=$(grep ANTHROPIC_API_KEY /home/dev/projects/headline-alerter/.env | cut -d= -f2) \
POSTGRES_URL="postgresql://rates:changeme@localhost:5432/rates" \
python3 -c "
import anthropic, os
from services.shared.macro_context import save_context
from services.shared.db import connect
from services.scorer.context_builder import build_macro_context

client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
today = '$(date +%Y-%m-%d)'
summary = build_macro_context(client, 'claude-sonnet-4-6', today)
print('=== SUMMARY ===')
print(summary)
print('=== END ===')
with connect() as conn:
    save_context(conn, summary, 'claude-sonnet-4-6')
print('Saved to postgres.')
"
```

Expected: prints the macro summary and `Saved to postgres.`

- [ ] **Step 6.7: Restart scorer to pick up the new context**

```bash
docker compose restart scorer
sleep 5
docker compose logs scorer --tail=20
```

Expected: log line `"macro context loaded"` with `chars=<N>`.

- [ ] **Step 6.8: Commit**

```bash
git add services/scorer/Dockerfile pyproject.toml docker-compose.yml
git commit -m "feat(scorer): add tzdata dep and CONTEXT_MODEL compose env var"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `macro_context` Postgres table | Task 1 |
| `get_latest_context`, `save_context` DB helpers | Task 2 |
| `build_macro_context` with web search tool | Task 3 |
| `_seconds_until_midnight_et` | Task 3 |
| `build_system_prompt(macro_context=None)` | Task 4 |
| `system_prompt` param on `score_event()` | Task 4 |
| `system_prompt` param on `process_one_event()` | Task 5 |
| Thread-safe `get/set_system_prompt` | Task 5 |
| Startup load from Postgres | Task 5 |
| Midnight ET refresh daemon thread | Task 5 |
| `CONTEXT_MODEL` env var | Task 6 |
| `tzdata` dependency | Task 6 |
| Top-down search sequence in prompt | Task 3 (CONTEXT_SEARCH_PROMPT) |
