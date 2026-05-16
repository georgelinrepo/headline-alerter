# Macro Context Builder — Design

**Date:** 2026-05-16
**Status:** Draft v1

## 1. Overview

The scorer currently evaluates each event in isolation with no knowledge of the current macro regime. A "Fed official concerned about energy prices" headline scores completely differently when Claude knows there is an active war disrupting global oil supply, CPI is at 3.8%, and the Fed just appointed a hawkish new chair. Without this regime awareness the scorer is systematically miscalibrated.

This feature adds a nightly macro context summary that is injected into the scorer's system prompt, giving Claude the regime awareness to score events accurately.

## 2. Architecture

A background thread inside the scorer process wakes at midnight ET each night. It calls Claude Sonnet with Anthropic's built-in web search tool to synthesise a macro summary, stores it in a new `macro_context` Postgres table, and updates the in-memory system prompt. Every scoring call thereafter uses the enriched prompt.

On startup the scorer loads the most recent stored context from Postgres so it is immediately enriched even after a restart. If no context exists yet (first run before midnight) it uses the bare system prompt.

No new service, no external cron job — everything lives inside the scorer process.

## 3. Search Strategy

Top-down sequence: broad regime events first, specifics second. This ensures geopolitical shocks, wars, and central-bank transitions surface before the detailed fill-in.

1. `"dominant macro themes driving US interest rates {month} {year}"` — catches regime events (wars, crises, structural policy shifts)
2. `"Federal Reserve policy stance {month} {year}"`
3. `"US Treasury yields 2 year 10 year 30 year {month} {year}"`
4. `"recent US economic data CPI NFP PCE {month} {year}"`
5. `"US economic calendar major events next week {month} {year}"`

Claude Sonnet runs all searches autonomously via the Anthropic `web_search_20250305` tool (max_uses: 8) in a single API call. No separate search API key is required.

## 4. Output Format

```
**US Macro Context — YYYY-MM-DD**

**Dominant Themes:** [regime events, wars, crises, structural shifts driving rates]
**Fed Stance:** [rate level, direction, chair, recent dissents, next meeting date]
**Rates:** [2y, 10y, 30y levels, recent bp moves, curve shape]
**Recent Data:** [CPI, NFP, PCE prints with surprises vs consensus]
**This Week:** [scheduled events, Fed speakers, major auctions]
```

## 5. Data Model

New Postgres table added in `migrations/002_macro_context.sql`:

```sql
CREATE TABLE macro_context (
  id           SERIAL PRIMARY KEY,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  summary      TEXT NOT NULL,
  model        TEXT NOT NULL
);
```

The scorer always reads `ORDER BY generated_at DESC LIMIT 1`. Old rows are retained for audit — no automated cleanup in v1.

## 6. Scorer Integration

### 6.1 Dynamic system prompt

`scorer_prompts.py` gains a `build_system_prompt(macro_context=None)` function that returns the cache block list. When `macro_context` is provided it is prepended as a `<macro_context>` XML block before the scoring rubric:

```
<macro_context>
{summary}
</macro_context>

You are scoring a news or social-media event...
```

`anthropic_client.score_event()` gains an optional `system_prompt: list | None = None` parameter. When `None` it falls back to the bare `SYSTEM_PROMPT_CACHE_BLOCK`.

`process_one_event()` in `scorer/main.py` gains the same `system_prompt` parameter and passes it through.

### 6.2 Thread-safe shared prompt

`scorer/main.py` maintains a module-level `_current_system_prompt: list | None` guarded by `threading.Lock`. Two helpers:

```python
def get_system_prompt() -> list | None: ...   # main loop calls this per event
def set_system_prompt(prompt: list) -> None: ... # startup + refresh thread call this
```

### 6.3 Background refresh thread

Daemon thread started in `main()` before entering the consumer loop:

1. Compute seconds until next midnight ET using `zoneinfo.ZoneInfo("America/New_York")`
2. Sleep
3. Call `build_macro_context(client, context_model, today)` from `context_builder.py`
4. Write result to `macro_context` table via `save_context()`
5. Call `set_system_prompt(build_system_prompt(summary))`
6. Log and repeat

On any exception: log error, do not crash — fall back to existing prompt.

### 6.4 Startup load

Before entering the consumer loop, `main()` calls `_load_initial_context()` which reads the latest row from `macro_context` and calls `set_system_prompt()`. Failure is logged and swallowed — the scorer must always start.

## 7. New Files

| File | Purpose |
|---|---|
| `migrations/002_macro_context.sql` | New table |
| `services/shared/macro_context.py` | DB helpers: `get_latest_context`, `save_context` |
| `services/scorer/context_builder.py` | Sonnet + web search synthesis |
| `tests/unit/shared/test_macro_context.py` | Unit tests for DB helpers |
| `tests/unit/scorer/test_context_builder.py` | Unit tests for builder |

## 8. Modified Files

| File | Change |
|---|---|
| `services/shared/scorer_prompts.py` | Add `build_system_prompt(macro_context=None)` |
| `services/shared/anthropic_client.py` | Add `system_prompt` param to `score_event()` |
| `services/scorer/main.py` | Lock, helpers, refresh thread, startup load, pass system_prompt |
| `services/scorer/Dockerfile` | Add `tzdata` pip dependency |
| `docker-compose.yml` | Add `CONTEXT_MODEL` env var to scorer service |
| `pyproject.toml` | Add `tzdata` to dependencies |

## 9. Configuration

`CONTEXT_MODEL` env var (default: `claude-sonnet-4-6`). Added to scorer service in `docker-compose.yml`.

## 10. Cost

~8 web search calls + ~2k input tokens + ~500 output tokens of Sonnet once per night ≈ $0.01/night. Negligible.

## 11. Testing

- **Unit:** `test_macro_context.py` — `get_latest_context` (returns None when empty, returns latest when rows exist), `save_context` (inserts row)
- **Unit:** `test_context_builder.py` — `build_macro_context` with fake client (text extraction, empty response error), `_seconds_until_midnight_et` (correct delay calculation)
- **Unit:** `test_prompts.py` (existing) — add tests for `build_system_prompt` with and without context
- **Unit:** `test_main.py` (existing) — add test that `process_one_event` passes `system_prompt` through to `score_event`
