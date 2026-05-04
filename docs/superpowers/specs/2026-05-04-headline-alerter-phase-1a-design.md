# Headline Alerter — Phase 1a Design

**Date:** 2026-05-04
**Author:** George Lin
**Status:** Draft
**Parent spec:** [`2026-05-03-headline-alerter-design.md`](2026-05-03-headline-alerter-design.md) (v1 design)

## 1. Overview

Phase 1a is the first vertical slice of the headline-alerter pipeline: a single RSS ingestor (CNBC) plus the LLM scorer, end-to-end. Real news events flow into Kafka, get scored by Claude Haiku 4.5, and land in Postgres with score fields populated. No alerting (Twilio) and no dashboard (FastAPI/SSE) yet — those are Phase 1b and Phase 1c.

This phase deviates from the parent spec in one place: the first ingestor is **CNBC** rather than `fed_rss`. Rationale: Fed RSS is too low-volume to exercise the pipeline meaningfully (a few items per week outside FOMC weeks). CNBC's free RSS feeds (Economy, Markets, Top News) emit dozens of items per hour during US market hours and have high rates-relevance density — perfect for stress-testing the scorer and getting real "is this any good" feedback within hours.

## 2. Goals & non-goals

### Goals

- Prove the **ingestor → Kafka → scorer → Postgres** contract end-to-end on real traffic.
- Build the `Ingestor` ABC + DLQ pattern in the shared library so Phase 2 (BLS) is a thin subclass with zero changes to `services/shared/`.
- Use real Anthropic Haiku 4.5 with prompt caching for the system prompt.
- Provide a CLI tail tool (`tools/tail.py`) so the developer can monitor the pipeline before the dashboard exists.
- DLQ pattern in place: a single failed scoring call cannot stall the pipeline.

### Non-goals (deferred)

- Twilio / SMS / WhatsApp alerting — Phase 1b.
- Dashboard / FastAPI / SSE / browser UI — Phase 1c.
- Additional sources (`bls_rss`, `treasury_rss`, `truth_social`, `x_curated`) — Phases 2–5.
- Dedup, cooldowns, per-source thresholds, evaluation — see parent spec § 9.
- Scaling tests under `--scale scorer=N` — verified manually if relevant; not a dev-test gate.

### Success criterion (one sentence)

A real CNBC headline arriving in the RSS feed appears in `events_archive` with a populated score, direction, confidence, and reasoning within 5 seconds (p95) of being published, with no developer intervention.

## 3. Architecture

```
                  CNBC RSS feed(s)
                         │
                         ▼
                ┌────────────────────┐
                │  ingestor-cnbc     │   poll every 60s; for each new item:
                │                    │   1. dedup vs last_ts_source (from PG)
                │                    │   2. normalize → NormalizedEvent
                └─────┬──────────────┘   3. produce → events.normalized
                      │                  4. INSERT events_archive (status='received')
                      ▼
       ┌────────────────────────────┐
       │  Kafka: events.normalized  │
       └─────┬──────────────────────┘
             │ subscribe (group: scorer-cg)
             ▼
       ┌────────────────────┐  HTTP POST       ┌─────────────────┐
       │  scorer            │ ───────────────▶ │  Anthropic API  │
       │                    │ ◀─────────────── │  Haiku 4.5      │
       └─────┬──────────────┘  ScoredEvent     │  (cached prompt)│
             │                                  └─────────────────┘
             ├─── on success:
             │      • produce → events.scored
             │      • UPDATE events_archive (status='scored', score, ...)
             │      • commit Kafka offset
             │
             └─── on failure (after retries):
                    • produce → events.dlq (typed envelope)
                    • UPDATE events_archive (status='failed')
                    • commit Kafka offset (skip past)
```

All other elements (Kafka, Postgres, schema, topics, shared models, logging, db helper, kafka client) already exist from Phase 0.

## 4. Components

### 4.1 New files

```
services/
├── shared/
│   ├── ingestor_base.py         # Ingestor ABC: polling loop, last_ts hydration,
│   │                            # DLQ-on-parse-error, structured logging.
│   ├── dlq.py                   # send_to_dlq(producer, *, stage, service, error,
│   │                            # original_event, retry_count) — typed envelope.
│   └── anthropic_client.py      # Wraps anthropic SDK: prompt caching, retry/backoff,
│                                # 30s hard timeout, typed exceptions per failure mode.
├── ingestors/
│   ├── Dockerfile               # Shared image used by all ingestor services
│   │                            # (cnbc_rss now; bls_rss/treasury_rss in Phase 2/5).
│   └── cnbc_rss/
│       └── main.py              # Concrete subclass: feed URLs + _parse_item.
└── scorer/
    ├── Dockerfile
    ├── main.py                  # Kafka consumer loop: consume → score → produce/upsert.
    └── prompts.py               # System prompt + score_event tool schema (separate
                                 # for testability; lifted from parent spec § 5).
tools/
└── tail.py                      # CLI: poll Postgres every 2s, print live event table.
tests/
├── unit/
│   ├── shared/
│   │   ├── test_ingestor_base.py
│   │   ├── test_dlq.py
│   │   └── test_anthropic_client.py
│   ├── ingestors/
│   │   └── test_cnbc_rss.py     # uses tests/fixtures/cnbc_sample.xml
│   └── scorer/
│       ├── test_prompts.py
│       └── test_main.py         # mocked Kafka + mocked Anthropic
├── integration/
│   └── test_phase1a_e2e.py      # real Kafka + real PG, mocked Anthropic
└── fixtures/
    ├── cnbc_sample.xml          # captured CNBC RSS response (~10 items)
    └── anthropic_score_response.json  # captured tool-use response
```

### 4.2 New Compose services

| Service | Image | Key env vars |
|---|---|---|
| `ingestor-cnbc` | `services/ingestors/Dockerfile` | `INGESTOR_SOURCE=cnbc_rss`, `CNBC_RSS_URLS` (comma-separated), `POLL_INTERVAL_SECONDS=60`, plus shared `KAFKA_BROKERS`, `POSTGRES_URL` |
| `scorer` | `services/scorer/Dockerfile` | `ANTHROPIC_API_KEY`, `SCORER_MODEL=claude-haiku-4-5`, `SCORER_TIMEOUT_SECONDS=30`, plus shared Kafka/PG vars. Replicable via `docker compose up --scale scorer=N`. |

Both depend on `kafka` (healthy) + `postgres` (healthy) + `migrate` (completed successfully).

### 4.3 Default CNBC RSS URLs

```
CNBC_RSS_URLS=
  https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664,
  https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258,
  https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135
```

(Economy, Markets, Top News — confirmed working at spec-write time. The single ingestor process polls all three and produces under `source='cnbc_rss'` — multiple URLs is just an implementation detail; the source name stays singular.)

## 5. Data flow & contracts

### 5.1 NormalizedEvent (already exists from Phase 0 — § 4.2 of parent spec)

For CNBC: the ingestor populates `body` from the RSS `<description>` (a short summary; CNBC RSS does not include full article text — that's an accepted limitation, the scorer reads what it gets).

```python
NormalizedEvent(
    event_id=sha256_hex(f"cnbc_rss|{url}|{ts_source.isoformat()}"),  # deterministic; full hex per parent spec §6.1
    source="cnbc_rss",
    ts_source=...,        # from RSS <pubDate>
    ts_ingested=now_utc(),
    headline=...,         # from RSS <title>
    body=...,             # from RSS <description>
    url=...,              # from RSS <link>
    metadata={
        "raw_id": rss_guid,
        "feed_url": one_of(CNBC_RSS_URLS),
        "categories": [...]   # from RSS <category> if present
    },
)
```

### 5.2 ScoredEvent (already exists from Phase 0)

Produced by scorer to `events.scored` and used to upsert `events_archive`:

```python
ScoredEvent(
    event_id=...,                   # same as input event_id
    score=int_0_to_10,
    direction="rates_higher" | "rates_lower" | "neutral" | "unclear",
    confidence=float_0_to_1,
    reasoning=...,                  # ≤1000 chars
    model="claude-haiku-4-5",
    scored_at=now_utc(),
)
```

### 5.3 DLQ envelope

Produced to `events.dlq` (key=`event_id`):

```jsonc
{
  "stage": "scorer_throttle" | "scorer_5xx" | "scorer_timeout"
         | "scorer_schema_violation" | "scorer_auth" | "scorer_unknown"
         | "ingest_parse",
  "service": "scorer" | "ingestor-cnbc",
  "ts_dlq": "2026-05-04T14:32:11Z",
  "error": "anthropic.RateLimitError: 429 after 3 retries",  // class + message
  "retry_count": 3,
  "original_event": { ... }    // NormalizedEvent dict, or raw RSS dict for ingest errors
}
```

## 6. Error handling

### 6.1 Scorer

| Failure | Detection | Behavior |
|---|---|---|
| Anthropic 429 | `anthropic.RateLimitError` | Backoff 1s/4s/16s, then DLQ `stage='scorer_throttle'` |
| Anthropic 5xx | `anthropic.APIStatusError` (5xx) | Same backoff, then DLQ `stage='scorer_5xx'` |
| Per-call timeout (>30s) | `httpx.TimeoutException` | 1 retry, then DLQ `stage='scorer_timeout'` |
| Malformed tool response | parse error in our code | 1 retry, then DLQ `stage='scorer_schema_violation'` |
| Auth (401, 403) | `anthropic.AuthenticationError` | No retry — log loudly, DLQ `stage='scorer_auth'`, scorer continues |
| Unknown exception | bare `Exception` | DLQ `stage='scorer_unknown'`, log full traceback, scorer continues |

The retry/backoff and timeout enforcement live in `services/shared/anthropic_client.py` so they're tested once. The scorer's main loop just calls `score_event(normalized_event)` and either gets a `ScoredEvent` back or catches the final exception and routes to DLQ.

### 6.2 Ingestor

| Failure | Behavior |
|---|---|
| RSS feed 4xx/5xx | Log warning, skip this poll, exponential backoff next poll interval (60s → 120s → 240s, capped at 600s; reset on first success) |
| RSS XML parse error on the feed itself | Same as above (treated as transient) |
| Per-item normalization error | DLQ `stage='ingest_parse'` (raw RSS dict as `original_event`), continue with next item |
| Postgres write fails | Crash → Compose restarts. PG outages are not transient at the per-event level. |
| Kafka produce fails after `flush()` retries | Same — crash and restart. |

### 6.3 The 30s timeout vs the 5s SLO

Two distinct numbers:

- **Hard timeout: 30s per Anthropic call.** Above this, the call is genuinely stuck — kill it, route to DLQ, move on.
- **SLO: 5s p95** for `ts_scored - ts_ingested` on the happy path. Measured by emitting `latency_ms` on every successful scored-event log. Measurable via `docker compose logs scorer | grep latency_ms`.

These are orthogonal: the timeout protects the consumer from hangs; the SLO is what we expect under normal Anthropic conditions. Regularly seeing >5s p95 (without timeouts) is a "performance is degrading" signal worth investigating, not a failure mode.

## 7. Idempotency & restart safety

Reuses Phase 0's invariants:

- `event_id = sha256(source + url + ts_source)` → deterministic (per parent spec § 6.1).
- Kafka producer: `enable.idempotence=true` (set in `services/shared/kafka_client.py`).
- Postgres writes: `INSERT INTO events_archive ... ON CONFLICT (id) DO UPDATE` (ingestor and scorer both upsert).
- Consumer: manual offset commit only after both PG write and Kafka produce succeed (at-least-once semantics; the upsert makes downstream state effectively exactly-once).

**Restart safety:**

- Ingestor on startup: `SELECT MAX(ts_source) FROM events_archive WHERE source='cnbc_rss'`. Only emit RSS items newer than that. If the table is empty or the max is >24h old, fall back to a 24h lookback so we don't flood with very old items but also don't miss anything from the recent window.
- Scorer on startup: resume from last committed Kafka offset; the `INSERT ON CONFLICT` upsert makes any in-flight retry harmless.

## 8. Testing strategy

### 8.1 Unit tests (CI on every push, no Docker required)

- `tests/unit/shared/test_ingestor_base.py` — polling loop, dedup-by-ts, restart hydration, DLQ-on-parse-error. Uses a fake `Ingestor` subclass + monkeypatched clock.
- `tests/unit/shared/test_dlq.py` — envelope construction, key selection, error serialization.
- `tests/unit/shared/test_anthropic_client.py` — retry/backoff math, exception → stage routing, prompt-cache header injection. Mocks the SDK with `respx` or a stub.
- `tests/unit/ingestors/test_cnbc_rss.py` — CNBC item → `NormalizedEvent` mapping. Uses captured RSS XML fixture.
- `tests/unit/scorer/test_prompts.py` — prompt template + tool schema sanity.
- `tests/unit/scorer/test_main.py` — consume-loop wiring with mocked Kafka + mocked Anthropic. Verifies success path, DLQ path, offset commit semantics.

### 8.2 Integration tests (require `docker compose up -d`, run in a separate CI job)

- `tests/integration/test_phase1a_e2e.py` — real Kafka + real PG, mocked Anthropic. Produce a synthetic `NormalizedEvent` → assert scorer writes the row and produces to `events.scored` within 10s. Inject a "force 429" Anthropic mock → assert one row appears on `events.dlq` and the scorer continues processing subsequent events.

### 8.3 Smoke test (manual, one-shot, not in CI)

- `tools/scorer_smoke.py` — real CNBC URL fetch + real Anthropic call (1 event). Cost ≈ $0.0014. Prints the score and exits. Run once after deploy to prove keys + network work end-to-end.

### 8.4 Test data

- `tests/fixtures/cnbc_sample.xml` — captured CNBC RSS response, ~10 items. Committed.
- `tests/fixtures/anthropic_score_response.json` — captured tool-use response. Committed.

### 8.5 Deliberately not tested in Phase 1a

- Real Anthropic in CI (cost + flakiness).
- Long-running soak test (premature; revisit in Phase 1b/1c).
- `--scale scorer=N` concurrency (verified manually if relevant).

## 9. Acceptance criteria

Phase 1a is done when **all** of these hold:

1. `docker compose up -d` results in `kafka` + `postgres` healthy, `kafka-init` + `migrate` exited 0, `ingestor-cnbc` + `scorer` running.
2. Within ~1 minute, `SELECT count(*) FROM events_archive WHERE source='cnbc_rss'` is non-zero.
3. Within ~5 seconds (p95) of an event landing on `events.normalized`, that row in `events_archive` has `status='scored'` and non-null `score`, `direction`, `confidence`, `reasoning`, `model`. (Measured via `latency_ms` field in scorer logs.)
4. `python tools/tail.py` renders a continuously-refreshing table of recent events with score fields.
5. DLQ catches a synthetic broken event without crashing the scorer:
   - one row appears on `events.dlq` with the right `stage`
   - the failed event's `events_archive` row shows `status='failed'`
   - the scorer continues consuming subsequent events
6. `python tools/scorer_smoke.py` prints `OK — Phase 1a smoke test passed (event scored: ...)`.
7. `pytest -v` passes (unit + integration).
8. `docker compose restart ingestor-cnbc scorer` does not produce duplicate `events_archive` rows for the same `event_id` and the scorer resumes from where it left off.

## 10. Open items (resolved at plan-writing time, not architecture)

- `tools/tail.py` rendering style: simple reprint loop (cleared with ANSI escapes), not curses.
- Exact `event_id` length for `events_archive.id` (the column is `TEXT`; any sha256 hex length works).
- Whether the integration test injects the "force 429" via env var, monkeypatch, or a small respx stub.

These don't change the architecture; they're plan-level details.

## 11. References

- Parent spec: `docs/superpowers/specs/2026-05-03-headline-alerter-design.md`
- Phase 0 plan: `docs/superpowers/plans/2026-05-03-headline-alerter-phase-0.md`
