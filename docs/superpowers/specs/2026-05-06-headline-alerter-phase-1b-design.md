# Headline Alerter — Phase 1b Design

**Date:** 2026-05-06
**Author:** George Lin
**Status:** Draft
**Parent spec:** [`2026-05-03-headline-alerter-design.md`](2026-05-03-headline-alerter-design.md) (v1 design)
**Predecessor:** [`2026-05-04-headline-alerter-phase-1a-design.md`](2026-05-04-headline-alerter-phase-1a-design.md) (CNBC ingestor + scorer, shipped)

## 1. Overview

Phase 1b adds the **alerter** service: a Kafka consumer that turns the high-scoring events Phase 1a is already producing into WhatsApp messages on the user's phone. No new ingestors, no scoring changes, no dashboard — just the missing link from "events are scored in Postgres" to "user's phone buzzes when something rates-relevant lands."

For v1 we use **Twilio's WhatsApp Sandbox** as the alert channel. It's free (no monthly number fee), cheap per-message (~$0.005), and works on a Twilio trial account without exiting trial mode. SMS support is scaffolded (the existing `ALERT_CHANNEL=sms|whatsapp|both` env var from the parent spec is honored) but WhatsApp is the default and the only path covered by acceptance.

## 2. Goals & non-goals

### Goals

- Take the existing `events.scored` Kafka stream from Phase 1a and produce WhatsApp messages for events meeting `score >= ALERT_THRESHOLD AND confidence >= MIN_CONFIDENCE`.
- **Idempotency**: redelivery of the same `event_id` (Kafka offset reset, container restart, replay) does NOT produce a duplicate WhatsApp message. This is correctness, not a feature.
- DLQ pattern matching the scorer: Twilio failures land on `events.dlq` with typed stages, the alerter never crashes on a single bad message.
- Audit message produced to `alerts.outgoing` (no consumer in v1; reserved).
- Idiomatic file layout matching the scorer's pattern: `main.py` + `twilio_client.py` + `format.py`, each with one clear responsibility.
- One-time WhatsApp Sandbox setup ritual is documented in the implementation plan (text the join code from the user's phone, paste credentials, restart alerter).

### Non-goals (deferred)

- **Cross-source dedup, cooldowns, per-source thresholds** — parent spec § 9 explicitly defers these. At threshold=4 we expect to see ~2 alerts for a single Powell-style event because CNBC re-lists across feeds. We accept the spam in v1; real traffic informs what the right dedup model is in Phase 1b.5 or 1c.
- **Twilio delivery status webhooks** — needs a public URL (Cloudflare Tunnel / VPS). `delivery_status='queued'` is captured at send time; upgrade to `delivered`/`failed` is a future spec.
- **WhatsApp template fallback** for >24h silent windows — we detect and DLQ it as `alerter_whatsapp_template`. If it becomes a real problem during quiet weekends, register a utility template via Twilio (separate spec).
- **Dashboard / FastAPI / SSE** — Phase 1c.
- **Additional ingestors** (`bls_rss`, `treasury_rss`, `truth_social`, `x_curated`) — Phases 2–5.

### Success criterion (one sentence)

A scored event with `score >= ALERT_THRESHOLD AND confidence >= MIN_CONFIDENCE` produces a WhatsApp message on the user's phone within ~10 seconds of being scored, and replays do not produce duplicates.

## 3. Architecture

```
       ┌────────────────────────────┐
       │  Kafka: events.scored      │   produced by scorer (Phase 1a)
       └─────┬──────────────────────┘
             │ subscribe (group "alerter-cg", offsets manual)
             ▼
       ┌─────────────────────┐
       │  alerter            │   For each ScoredEvent message:
       │                     │   1. JSON-decode → ScoredEvent
       │                     │   2. should_fire(score, confidence)? — if no, commit & continue
       │                     │   3. has_been_alerted(event_id)? — if yes, commit & continue (idempotency)
       │                     │   4. fetch headline/source/ts_source/url from events_archive
       │                     │   5. build alert text (format.py)
       │                     │   6. twilio_client.send(channel, to, from, body)
       │                     │   7. on success:
       │                     │        • INSERT alert_history (event_id, channel, recipient,
       │                     │            twilio_sid, sent_at, delivery_status='queued')
       │                     │        • UPDATE events_archive SET ts_alerted, status='alerted'
       │                     │        • produce audit msg to alerts.outgoing
       │                     │        • commit Kafka offset
       │                     │   8. on failure (after retries):
       │                     │        • produce envelope to events.dlq with typed stage
       │                     │        • commit Kafka offset (skip past)
       └─────┬───────────────┘
             │
             ▼
       ┌─────────────────────────────────┐
       │  Twilio (WhatsApp Sandbox)      │
       └─────────────┬───────────────────┘
                     │
                     ▼
                Your phone 📱
```

All other elements (Kafka, Postgres, schema including `alert_history`, the four topics, shared library, ingestor, scorer) already exist from Phase 0 / Phase 1a.

## 4. Components

### 4.1 New files

```
services/
└── alerter/
    ├── __init__.py
    ├── Dockerfile
    ├── main.py             # consumer loop + decision + audit + status update
    ├── twilio_client.py    # wraps twilio SDK: build msg body, retry/backoff, typed errors
    └── format.py           # build the alert text from a ScoredEvent (separate for testability)
tools/
└── alerter_smoke.py        # one-shot real-Twilio smoke test (single hardcoded message)
tests/
├── unit/alerter/
│   ├── __init__.py
│   ├── test_main.py
│   ├── test_twilio_client.py
│   └── test_format.py
├── integration/
│   └── test_phase1b_e2e.py  # real Kafka + real PG, mocked Twilio
└── fixtures/
    └── scored_event.json    # representative ScoredEvent payload for tests
```

### 4.2 New Compose service

| Service | Image | Key env vars |
|---|---|---|
| `alerter` | `services/alerter/Dockerfile` | `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM`, `ALERT_RECIPIENT`, `ALERT_CHANNEL=whatsapp` (default), `ALERT_THRESHOLD=4`, `MIN_CONFIDENCE=0.6`, plus shared `KAFKA_BROKERS`, `POSTGRES_URL` |

Depends on `kafka` (healthy) + `postgres` (healthy) + `migrate` (completed).

### 4.3 `.env.example` updates

```
# Twilio (Phase 1b)
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
# WhatsApp Sandbox FROM number (shared by all Twilio sandbox users)
TWILIO_FROM=whatsapp:+14155238886
# Your phone number, with WhatsApp prefix. Must have texted "join <code>" to the
# sandbox number from this phone first (one-time opt-in, see runbook in plan).
ALERT_RECIPIENT=whatsapp:+44...

ALERT_CHANNEL=whatsapp
ALERT_THRESHOLD=4
MIN_CONFIDENCE=0.6
```

### 4.4 Why three files instead of one

Same factoring as the Phase 1a scorer (`anthropic_client.py` / `prompts.py` / `main.py`):

- **`format.py`** is pure functions over `ScoredEvent` + the looked-up archive row → string. No I/O. Trivially testable.
- **`twilio_client.py`** owns retry/backoff and exception → stage routing. Tested once with a mocked SDK; consumed elsewhere as a stable interface.
- **`main.py`** is the consumer loop + decision + DB writes. SDK details and formatting concerns don't bleed in.

## 5. Data flow & contracts

### 5.1 Decision predicates

```python
def should_fire(score: int, confidence: float) -> bool:
    return score >= ALERT_THRESHOLD and confidence >= MIN_CONFIDENCE

def has_been_alerted(conn, event_id: str) -> bool:
    cur.execute("SELECT 1 FROM alert_history WHERE event_id = %s LIMIT 1", (event_id,))
    return cur.fetchone() is not None
```

Both are pure (predicate functions) — testable without mocking I/O at the call site.

### 5.2 Archive read

The scorer's `ScoredEvent` Kafka payload contains `event_id`, `score`, `direction`, `confidence`, `reasoning`, `model`, `scored_at` — but not headline/source/ts_source/url. Those live in `events_archive`. One indexed SELECT per alert (rare events, not a hot path):

```sql
SELECT headline, source, ts_source, url
FROM events_archive
WHERE id = %s;
```

### 5.3 Alert message body

Plain text, ≤4096 chars (WhatsApp soft cap). Format built by `format.py`:

```
[7/10 ↓ rates_lower · conf 72%]
cnbc_rss · 14:32Z

Fed Chair Powell remarks on inflation outlook

Powell tone notably more dovish than recent statements; market is likely to price in a near-term cut. Largest impact expected on the 2y tenor.

https://www.cnbc.com/2026/05/04/powell-dovish-pivot.html
```

Direction → glyph mapping:

| `direction` | glyph |
|---|---|
| `rates_higher` | ↑ |
| `rates_lower` | ↓ |
| `neutral` | → |
| `unclear` | ? |

WhatsApp auto-linkifies the URL — it becomes a tap-target on the phone.

### 5.4 alerts.outgoing audit envelope

```jsonc
{
  "event_id": "<sha256 hex>",
  "channel": "whatsapp" | "sms",
  "recipient": "whatsapp:+44...",
  "twilio_sid": "SM...",
  "sent_at": "2026-05-06T14:32:11Z"
}
```

Keyed by `event_id`. No consumer in v1.

### 5.5 alert_history row

After successful Twilio send:

```sql
INSERT INTO alert_history (event_id, channel, recipient, twilio_sid, sent_at, delivery_status)
VALUES (%s, %s, %s, %s, NOW(), 'queued');
```

`delivery_status='queued'` is Twilio's initial state. Webhook-driven upgrade to `delivered`/`failed` is deferred per § 2.

### 5.6 events_archive update on success

```sql
UPDATE events_archive
SET status = 'alerted', ts_alerted = NOW()
WHERE id = %s;
```

If `rowcount == 0`, raise — same defensive pattern as the scorer's `update_archive_with_score` (the row must exist; missing-row indicates an ordering bug).

## 6. Error handling

### 6.1 Twilio failure modes

The retry/backoff and exception → stage routing live in `services/alerter/twilio_client.py`. The alerter's main loop catches a typed `AlerterError` and routes to DLQ.

| Failure | Detection | Behavior |
|---|---|---|
| Twilio 429 | `TwilioRestException` `status == 429` | Backoff 1s/4s/16s, then DLQ `stage='alerter_throttle'` |
| Twilio 5xx | `TwilioRestException` `500 <= status < 600` | Same backoff, then DLQ `stage='alerter_5xx'` |
| Twilio 401 / 403 (auth) | status in `{401, 403}` | No retry — DLQ `stage='alerter_auth'`, log loud, alerter continues |
| Invalid recipient (code 21211) | `TwilioRestException.code == 21211` | No retry — DLQ `stage='alerter_recipient'` |
| Recipient not opted into sandbox (codes 63007 / 21610) | code-checked | No retry — DLQ `stage='alerter_recipient_not_opted_in'`, log with remediation hint |
| WhatsApp template required (24h window expired, code 63016) | code-checked | No retry — DLQ `stage='alerter_whatsapp_template'`, alerter keeps consuming |
| Network timeout / `httpx.TimeoutException` | caught | 1 retry, then DLQ `stage='alerter_timeout'` |
| Unknown exception | bare `Exception` | DLQ `stage='alerter_unknown'`, log full traceback |

### 6.2 What does NOT crash the alerter

By design: nothing the user can cause should crash this service. Twilio outage, bad recipient, expired template, malformed Kafka payload — all become DLQ rows + log lines, and the alerter keeps consuming.

### 6.3 What DOES crash the alerter

Only environment-broken failures, where Compose-restart is the right answer:
- Postgres connection refused
- Kafka broker unreachable

Per Phase 1a's pattern: an uncaught exception in the main loop crashes the process; Compose's `restart: unless-stopped` brings it back.

### 6.4 The post-Twilio failure window

Like the scorer, there's an edge case between "Twilio call succeeded" and "PG writes complete." If the alerter crashes between them:
- Twilio has already delivered the message (real WhatsApp on the user's phone).
- `alert_history` row was NOT written.
- On restart, the offset hasn't been committed → re-deliver from Kafka.
- The idempotency check `has_been_alerted(event_id)` returns False (no row).
- We'd re-call Twilio → duplicate message.

This is unlikely in practice (the PG writes are <100ms after the Twilio call) but the parent spec accepts at-least-once semantics. The mitigation pattern is the same as the scorer: the writes are wrapped in a try/except that routes unexpected errors to DLQ as `alerter_unknown` rather than crashing. For a true "exactly once" guarantee we'd need a transactional outbox pattern; that's overkill for v1.

## 7. Idempotency & restart safety

### 7.1 Invariants

- `event_id` is deterministic from Phase 1a (`sha256(source|url|ts_source)`).
- `has_been_alerted(event_id)` query gates every Twilio call.
- `alert_history.id` is `UUID DEFAULT gen_random_uuid()` — even if we somehow wrote twice (gate bypass), no PK collision.
- Kafka offset commits AFTER both Twilio + PG writes — at-least-once semantics, made effectively-once by the gate.

### 7.2 Replay scenario

Operator runs `kafka-consumer-groups --reset-offsets --to-earliest --topic events.scored --group alerter-cg --execute`. Expected behavior:
- Alerter consumes every historical scored event.
- For each event_id already in `alert_history`: gate returns `True`, skip. No Twilio call, no PG writes, just an offset commit.
- For each event_id NOT in `alert_history` (e.g., events scored when the alerter was offline): full delivery path runs.
- User's phone buzzes only for genuinely new alerts.

### 7.3 Restart scenario

`docker compose restart alerter`. Expected:
- Alerter resumes from last committed offset.
- Any in-flight message that was processed but not yet committed is re-delivered.
- Gate handles deduplication.

## 8. Testing strategy

### 8.1 Unit tests (CI on every push, no Docker)

| File | What it covers |
|---|---|
| `tests/unit/alerter/test_format.py` | direction-glyph mapping, message body assembly, URL appending, all four direction values |
| `tests/unit/alerter/test_twilio_client.py` | retry/backoff math (1s/4s/16s for 429/5xx, 1 retry for timeout, no-retry for auth/recipient/template), exception → stage routing for each Twilio error code in § 6.1. Mocks the `twilio.rest.Client` |
| `tests/unit/alerter/test_main.py` | `process_one_alert` wiring with mocked Twilio + mocked PG + mocked producer. Verifies: skip-below-threshold, idempotency hit, success path writes everything, failure path writes nothing but DLQ |

### 8.2 Integration tests (require `docker compose up -d`, separate CI job)

| File | What it covers |
|---|---|
| `tests/integration/test_phase1b_e2e.py` | Real Kafka + real PG, mocked Twilio (env-toggle `TWILIO_FAKE=1`). Three tests: success path (alert_history + events_archive + audit msg), idempotency (second call is no-op), failure path (DLQ row when `TWILIO_FAIL_MODE=throttle`) |

### 8.3 Smoke test (manual, real Twilio, not in CI)

| File | What it covers |
|---|---|
| `tools/alerter_smoke.py` | Sends one hardcoded WhatsApp message to `ALERT_RECIPIENT`. Confirms credentials, sandbox opt-in, network all work. Cost ~$0.005 |

### 8.4 Test data

- `tests/fixtures/scored_event.json` — representative `ScoredEvent` payload for unit tests. Reuses Phase 1a's `anthropic_score_response.json` shape where convenient.

### 8.5 Deliberately not tested

- Real Twilio in CI (cost + would deliver real messages to a real phone).
- Long-running soak (premature; Phase 1c).
- WhatsApp template fallback (we DLQ it; if it becomes real we revisit).
- Cross-source dedup behavior (deferred per spec § 9).

## 9. Acceptance criteria

Phase 1b is done when **all** of these hold:

1. `docker compose up -d` results in `kafka` + `postgres` healthy, `kafka-init` + `migrate` exited 0, and `ingestor-cnbc` + `scorer` + `alerter` all running.
2. `python tools/alerter_smoke.py` puts a real WhatsApp message on the user's phone within ~5s.
3. A scored event meeting `score >= ALERT_THRESHOLD AND confidence >= MIN_CONFIDENCE` (default `4 / 0.6` for dev) produces a WhatsApp message to `ALERT_RECIPIENT` within ~10s of `events.scored` arrival.
4. A scored event with `score < ALERT_THRESHOLD` produces no Twilio call, no `alert_history` row, no `alerts.outgoing` message — only a Kafka offset commit.
5. Resetting the alerter consumer group's offset to `earliest` and replaying historical traffic produces zero new WhatsApp messages (every event_id already in `alert_history`).
6. Forcing a fake throttle failure (`TWILIO_FAKE=1` + `TWILIO_FAIL_MODE=throttle`) for one event produces a row on `events.dlq` with `stage='alerter_throttle'`, the failed event's `events_archive` row stays at `status='scored'`, and the alerter continues consuming subsequent events.
7. Each successful send writes a row to `alert_history` with non-null `event_id`, `channel`, `recipient`, `twilio_sid`, `sent_at`, `delivery_status='queued'`.
8. `pytest -v` — unit + integration both green.

## 10. Open items (resolved at plan-writing time, not architecture)

- Exact Twilio error codes for "recipient not opted in to sandbox" (the SDK uses 63007 / 63016 / 21610 depending on context; verify against current Twilio API at plan time).
- Whether to log the full `twilio_sid` or just truncated (lean: full — they aren't secret).
- Whether `tools/alerter_smoke.py` accepts a `--message` flag or is purely hardcoded (lean: hardcoded, 5-line tool).
- Exact `TWILIO_FAKE` env-var name and structure for the integration test seam (lean: `TWILIO_FAKE=1`, `TWILIO_FAIL_MODE=throttle|recipient|none`).

These don't change the architecture; they're plan-level details.

## 11. References

- Parent spec: `docs/superpowers/specs/2026-05-03-headline-alerter-design.md`
- Phase 1a design: `docs/superpowers/specs/2026-05-04-headline-alerter-phase-1a-design.md`
- Phase 1a plan: `docs/superpowers/plans/2026-05-04-headline-alerter-phase-1a.md`
- Twilio WhatsApp Sandbox: https://www.twilio.com/docs/whatsapp/sandbox
- Twilio Python SDK: https://www.twilio.com/docs/libraries/python
