# Headline Alerter — Design

**Date:** 2026-05-03
**Author:** George Lin
**Status:** Draft (v1 design)

## 1. Overview

Headline Alerter is a streaming pipeline that ingests news and social-media events, scores each event for likely impact on US interest-rates markets using an LLM, and alerts the user via SMS/WhatsApp when a high-scoring event is detected. A live web dashboard streams events and scores to the browser as they flow through.

The target user persona is a US interest rates trader (SOFR / Treasury futures / Treasury yields). v1 user is the project owner; the design is deliberately shaped so a real trader could plausibly use it on a personal device — Goldman Sachs compliance keeps it off the firm network.

### Goals

- React quickly to news headlines and social-media posts (notably Trump on Truth Social) that move rates markets, without watching all those channels manually.
- Provide curation alpha (filter the firehose down to "AI says this matters") and coverage alpha (catch sources outside the trader's main attention).
- Run on cheap personal infrastructure (a Pi 5 or single VPS) with no firm-network dependencies.
- Build it incrementally — v1 ships a working end-to-end loop with a small set of sources, and adding more sources is a copy-paste of the ingestor contract.

### Non-goals (v1)

- Sub-second latency / true speed alpha against Bloomberg.
- Predicting precise basis-point moves per tenor.
- Multi-user / web-product features.
- Closed-loop evaluation against actual market moves (its own future spec).

### Design principle

**Prefer real data over speculative configuration.** Defer policy decisions (dedup, cooldowns, thresholds, evaluation metrics) and the schemas that support them until real traffic exists to inform them. Ship the minimum needed to capture real behavior, then design follow-ups against actual data.

## 2. Architecture Overview

Four-stage pipeline. Five ingestor types (one container per source) produce normalized events to Kafka; a scorer consumer calls the Anthropic API to assign a score; an alerter consumer sends SMS/WhatsApp via Twilio for high-scoring events; a dashboard service consumes both topics and pushes live updates to the browser via Server-Sent Events.

```
                   ┌─────────────────────┐                ┌──────────────┐
INGESTORS ────────▶│  events.normalized  │──────────────▶ │  SCORER      │
(producers)        │  (Kafka topic, 3p)  │                │ (consumer    │
                   └──────────┬──────────┘                │  group)      │
                              │                           └──────┬───────┘
                              │                                  │
                              │                                  ▼
                              │                  ┌─────────────────────┐
                              │                  │   events.scored     │
                              │                  │   (Kafka topic, 3p) │
                              │                  └────┬─────────┬──────┘
                              │                       │         │
                              │                       ▼         │
                              │                   ALERTER       │
                              │                       │         │
                              │                       ▼         │
                              │                    Twilio       │
                              │                                 │
                              ▼                                 ▼
                          ┌─────────────────────────────────────────┐
                          │  DASHBOARD API (FastAPI + SSE)          │
                          │  consumes both topics, joins by         │
                          │  event_id, pushes to browsers           │
                          └────────────────────┬────────────────────┘
                                               │
                                               ▼
                                            Browser
```

### Deployment shape

Single host running Docker Compose. Default v1 host: a Pi 5 (8GB) with NVMe SSD and active cooler. Same compose works on a laptop or VPS. External services (Anthropic API, Twilio, RSS/X/Truth feeds) are reached via outbound HTTPS — no inbound public IP required for v1.

Remote access from phone via Tailscale (free, zero-config). Optional public exposure via Cloudflare Tunnel later.

### Volume

Realistic v1 traffic is well under 100 events/hour normal, with bursts of a few hundred events in a 5-minute FOMC window. The scorer consumer is stateless and horizontally scalable: 3 partitions on `events.normalized` allow scaling from 1 → 3 instances for burst handling (`docker compose up --scale scorer=3`).

## 3. Components

Each component is a separate Python service in its own container. All read config from environment variables and YAML; all emit structured JSON logs to stdout.

### 3.1 Ingestor (one process per source)

Pulls raw items from one source, normalizes to a common schema, produces to `events.normalized`, and writes a `status='received'` row to `events_archive`.

**Sources for v1 (built incrementally — see Section 8):**

| Source | Mechanism | Cadence | Library |
|---|---|---|---|
| `fed_rss` | RSS poll of federalreserve.gov press, speeches, FOMC | every 60s | `feedparser` |
| `bls_rss` | RSS poll of BLS releases (CPI, NFP, PCE) | every 30s near scheduled drops | `feedparser` |
| `treasury_rss` | RSS poll of Treasury auctions, statements | every 5min | `feedparser` |
| `x_curated` | X API basic tier, filtered stream of ~30 macro accounts | streaming | `tweepy` |
| `truth_social` | Polling Trump's profile via `truthbrush` | every 30s | `truthbrush` |

**Common interface:**

```python
class Ingestor(ABC):
    source_name: str

    def fetch(self) -> list[RawItem]: ...
    def normalize(self, raw: RawItem) -> NormalizedEvent: ...
    def run(self) -> None:
        # main loop: fetch → normalize → produce → archive
```

**Normalized event schema:**

```jsonc
{
  "event_id": "fed-rss-2026-05-03T14:32:00Z-a3f2",  // deterministic hash
  "source": "fed_rss",
  "ts_source": "2026-05-03T14:32:00Z",
  "ts_ingested": "2026-05-03T14:32:08Z",
  "headline": "Fed Chair Powell remarks on inflation outlook",
  "body": "...full text or excerpt...",
  "url": "https://...",
  "metadata": {"author": "...", "raw_id": "..."}
}
```

`event_id` is deterministic: `sha256(source + url + ts_source)`. Re-fetching the same RSS item produces the same `event_id`, making the pipeline idempotent end-to-end.

**Failure handling:** Network errors retry with backoff. Parse errors → `events.dlq` with `stage='ingest'`. Source rate-limits → exponential backoff on next poll, ingestor does not crash.

### 3.2 Scorer

Consumes `events.normalized`, calls the Anthropic API with a fixed prompt and tool-use schema, produces a scored result to `events.scored`, and updates the `events_archive` row to `status='scored'`.

**Inputs:** the normalized event only. v1 does not pass market context to the scorer (real-time market data is deferred — see Section 9).

**Model:** `claude-haiku-4-5` via the Anthropic API direct (not Bedrock; we are not on AWS for v1).

**Output schema:** see Section 5 (Scoring approach) for the prompt and tool definition.

**Failure handling:**

| Failure | Behavior |
|---|---|
| Anthropic 429 (throttle) | Exponential backoff (1s, 4s, 16s), then DLQ with `stage='scorer_throttle'` |
| Anthropic 5xx | Same backoff, then DLQ |
| Tool call missing or malformed | 1 retry with same prompt, then DLQ |
| Required field missing | DLQ with `stage='scorer_schema_violation'` |
| Timeout (>30s) | DLQ with `stage='scorer_timeout'` |
| Auth / region error | No retry — log loudly, DLQ, ops alert |

**Idempotency:** writes to `events_archive` are upserts keyed by `event_id`; replays simply overwrite the row.

**Scaling:** stateless, horizontally scalable via Docker Compose (`--scale scorer=3` during high-volume windows).

### 3.3 Alerter

Consumes `events.scored`, decides whether to fire an alert, and delivers via Twilio.

**Decision logic (v1, intentionally minimal):**

```python
def should_fire(scored_event):
    return (scored_event.score >= ALERT_THRESHOLD
            and scored_event.confidence >= MIN_CONFIDENCE)
```

`ALERT_THRESHOLD` and `MIN_CONFIDENCE` are global env vars (no per-source overrides in v1; see Section 9).

No dedup, no cooldown — accepted as known limitations. v1 will see real alert-amplification behavior, which informs follow-up specs.

**Twilio integration:** `twilio-python`. Channel selectable via `ALERT_CHANNEL=sms|whatsapp|both` env var.

**Alert content:**

```
[7/10 ↓ rates_lower, conf 72%]
Source: fed_rss · 14:32Z
"Fed Chair Powell remarks on inflation outlook"

Reason: Powell tone notably more dovish than recent...
```

**State written after a successful Twilio send:**
- `alert_history` row (event_id, channel, twilio_sid, sent_at)
- Update `events_archive.ts_alerted`, `status='alerted'`
- Produce audit message to `alerts.outgoing` (event_id, channel, twilio_sid, sent_at) — no consumer in v1; reserved for future delivery-status / metrics consumers

**Failure handling:** Twilio 5xx → backoff, then DLQ. Invalid recipient → DLQ + log alarm.

### 3.4 Dashboard API

FastAPI service that consumes both `events.normalized` and `events.scored`, maintains an in-memory ring buffer of the last ~500 events (keyed by `event_id`, score fields populated as scored messages arrive), and pushes live updates to connected browsers via Server-Sent Events (SSE). Buffer is warmed at startup from the most-recent 100 rows of `events_archive`.

**Why SSE not WebSocket:** SSE is one-way (server → client), HTTP-based, has automatic reconnect, and is simpler. WebSocket only earns its keep when the client also pushes data.

**Why a unique consumer group per dashboard instance:** SSE viewers are read-only fan-out. Using a unique CG ID per process (e.g., `dashboard-{uuid}`) ensures each dashboard instance receives every message — without this, multiple dashboard instances would split messages between them.

**Endpoints:**
- `GET /` — static HTML dashboard
- `GET /api/events?limit=N&since=ISO` — recent events from Postgres `events_archive`
- `GET /api/stream` — SSE: live stream of `{type: 'event'|'score', payload}`
- `GET /api/status` — per-source health (last event ts, errors, consumer lag, etc.)

**Two SSE message types:**

```jsonc
// from events.normalized
{ "type": "event", "data": { "event_id", "source", "ts", "headline", "body", ... } }

// from events.scored (matched by event_id)
{ "type": "score", "data": { "event_id", "score", "direction", "confidence", "reasoning" } }
```

**Browser logic (vanilla JS, no build step):**

```js
const evt = new EventSource('/api/stream');
evt.addEventListener('event', m => upsertRow(JSON.parse(m.data)));
evt.addEventListener('score', m => updateRowScore(JSON.parse(m.data)));
```

The join happens in the browser by `event_id` — events arrive over the same SSE connection in two flavors and are reconciled client-side.

### 3.5 Postgres

Source of truth for queryable state. Single Docker container with a named volume for persistence. Schema in Section 4.

### 3.6 Kafka

The event bus. Single broker in KRaft mode (no ZooKeeper) for v1; a separate `docker-compose.replicated.yml` overlay supports a 3-broker setup for replication exercises.

**Topics:** see Section 4.

## 4. Data Model

### 4.1 Kafka topics

| Topic | Partitions | RF | Key | Retention |
|---|---|---|---|---|
| `events.normalized` | 3 | 1 | `source` | 30 days |
| `events.scored` | 3 | 1 | `event_id` | 30 days |
| `alerts.outgoing` | 1 | 1 | `event_id` | 90 days |
| `events.dlq` | 1 | 1 | `event_id` | 14 days |

(RF=3, min.ISR=2 in the replicated overlay.)

**Producer config (all services):**

```
enable.idempotence=true
acks=all
compression.type=snappy
max.in.flight.requests=5
linger.ms=10
```

**Consumer config (all consumers):**

```
enable.auto.commit=false
isolation.level=read_committed
auto.offset.reset=earliest
session.timeout.ms=30000
```

**Message format:** JSON for v1. Schema Registry + Avro is a future upgrade once schema evolution becomes a real concern.

### 4.2 Postgres schemas

v1 uses two tables. (Earlier draft contained `dedup_state`, `evaluations`, `market_data_cache`, `sources_config` — all deferred per Section 9.)

```sql
CREATE TABLE events_archive (
  id              TEXT PRIMARY KEY,                 -- == event_id
  source          TEXT NOT NULL,
  ts_source       TIMESTAMPTZ NOT NULL,
  ts_ingested     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ts_scored       TIMESTAMPTZ,
  ts_alerted      TIMESTAMPTZ,
  status          TEXT NOT NULL,                    -- 'received'|'scored'|'alerted'|'failed'

  -- raw event
  headline        TEXT NOT NULL,
  body            TEXT,
  url             TEXT,
  metadata        JSONB DEFAULT '{}'::jsonb,

  -- score (filled by scorer)
  score           INT,                              -- 0-10
  direction       TEXT,                             -- 'rates_higher'|'rates_lower'|'neutral'|'unclear'
  confidence      NUMERIC(3,2),                     -- 0.00-1.00
  reasoning       TEXT,
  model           TEXT
);

CREATE INDEX idx_events_ts_ingested ON events_archive (ts_ingested DESC);
CREATE INDEX idx_events_source_status ON events_archive (source, status);
CREATE INDEX idx_events_score ON events_archive (score DESC) WHERE status IN ('scored', 'alerted');
```

```sql
CREATE TABLE alert_history (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        TEXT NOT NULL REFERENCES events_archive(id),
  channel         TEXT NOT NULL,                   -- 'sms' | 'whatsapp'
  recipient       TEXT NOT NULL,
  twilio_sid      TEXT,
  sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivery_status TEXT,                            -- updated by webhook in future
  error           TEXT
);

CREATE INDEX idx_alerts_event ON alert_history (event_id);
CREATE INDEX idx_alerts_sent_at ON alert_history (sent_at DESC);
```

### 4.3 Topic ↔ table relationship

| Kafka topic | Producer | Consumer | Postgres effect |
|---|---|---|---|
| `events.normalized` | Ingestors | Scorer, Dashboard | Ingestor inserts `events_archive` row, `status='received'` |
| `events.scored` | Scorer | Alerter, Dashboard | Scorer updates `events_archive` with score fields, `status='scored'` |
| `alerts.outgoing` | Alerter (after successful Twilio send) | None in v1 (audit only) | Alerter inserts `alert_history`; updates `events_archive`, `status='alerted'` |
| `events.dlq` | All services on error | None (manual replay) | No PG table; DLQ items live in Kafka only |

### 4.4 Migrations

`migrations/*.sql` files applied via `yoyo-migrations` from a one-shot `migrate` Compose service that runs before app services on every `docker compose up`.

## 5. Scoring Approach

The scorer uses a single fixed prompt for every event. Source name is passed as a field in the user message but does not change the prompt template. v1 does not include market context, few-shot examples, or per-source variants — all deferred until real data informs them.

### 5.1 System prompt

```
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
```

### 5.2 User message (per event)

```
Source: {source}
Published: {ts_source}
Headline: {headline}

Body:
{body}
```

(Body truncated to ~5k chars for safety; almost all events fit comfortably.)

### 5.3 Tool definition (forced)

```python
score_event_tool = {
    "name": "score_event",
    "description": "Score the rates-market relevance of an event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "score":      {"type": "integer", "minimum": 0, "maximum": 10},
            "direction":  {"type": "string",
                           "enum": ["rates_higher", "rates_lower",
                                    "neutral", "unclear"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reasoning":  {"type": "string", "maxLength": 1000}
        },
        "required": ["score", "direction", "confidence", "reasoning"]
    }
}
```

`tool_choice = {"type": "tool", "name": "score_event"}` forces structured output.

### 5.4 Invocation

```python
import anthropic
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

response = client.messages.create(
    model="claude-haiku-4-5",
    max_tokens=500,
    system=SYSTEM_PROMPT,
    messages=[{"role": "user", "content": user_message}],
    tools=[score_event_tool],
    tool_choice={"type": "tool", "name": "score_event"},
    temperature=0.0,
)

tool_use = next(b for b in response.content if b.type == "tool_use")
score_data = tool_use.input
```

**Timeout:** 30 seconds.

### 5.5 Cost ballpark

- ~1k input tokens (system + event) + ~150 output tokens per call
- Haiku 4.5 pricing: ~$0.80/M input, ~$4/M output → **~$0.0014 per event**
- 1,000 events/day → ~$42/month
- 100 events/hour sustained → ~$100/month

Stays well within "I don't care" cost ranges.

## 6. Operational Concerns

### 6.1 Idempotency

| Layer | Mechanism |
|---|---|
| Event identity | `event_id = sha256(source + url + ts_source)` |
| Producer | `enable.idempotence=true` |
| Consumer offset | Manual commit after successful processing (at-least-once) |
| Postgres writes | `INSERT ... ON CONFLICT (id) DO UPDATE` on `events_archive` |
| `events.scored` | Keyed by `event_id` |

Re-processing the same event is always safe.

### 6.2 Replay

```bash
# Replay last 24h through scorer
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --group scorer-cg \
  --topic events.normalized --reset-offsets \
  --to-datetime $(date -u -d '24 hours ago' +%FT%T.000) --execute

# Inspect DLQ
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic events.dlq --from-beginning | jq

# Replay DLQ back into events.normalized after fix
python tools/replay_dlq.py --since 2026-05-01
```

### 6.3 Restart safety

| Service | State | Recovery |
|---|---|---|
| `kafka` | Named volume | Resumes from disk |
| `postgres` | Named volume | Resumes |
| Ingestors | None — derives last-seen from `MAX(ts_source) FROM events_archive WHERE source = ?` on startup | Resume |
| Scorer / Alerter | None | Resume from last committed offset |
| Dashboard | In-memory ring buffer (lost) | Warm from `events_archive` last 100 rows on startup |

All services configured `restart: unless-stopped`.

### 6.4 Rate-limit handling

- Anthropic 429: exponential backoff 1s/4s/16s, then DLQ
- Twilio 429/5xx: same pattern
- RSS source 4xx/5xx: skip poll, exponential backoff next time
- X API: honor `Retry-After`

### 6.5 Logging

Structured JSON to stdout via `structlog`. Standard fields: `ts`, `service`, `level`, `event_id`, `msg`. Collected by Docker's default `json-file` driver. Tail with `docker compose logs -f <service>`.

### 6.6 Health surface

- Each service exits non-zero on unrecoverable error → Compose restarts
- Dashboard `GET /api/status` exposes per-service health (last event ts, consumer lag, error count)

### 6.7 Secrets

`.env` file on host, gitignored, loaded by Compose. Required keys:

```
POSTGRES_PASSWORD=
ANTHROPIC_API_KEY=
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_FROM=
ALERT_RECIPIENT=
X_API_KEY=          # only when x_curated enabled
```

`.env.example` checked in with placeholders.

## 7. Docker Setup

### 7.1 Project layout

```
headline-alerter/
├── docker-compose.yml              # 1 broker (v1 default)
├── docker-compose.replicated.yml   # overlay: 3 brokers, RF=3
├── .env / .env.example
├── README.md
├── pyproject.toml
├── services/
│   ├── shared/                     # kafka client, db, models, logging
│   ├── ingestors/
│   │   ├── Dockerfile              # one image, used by all 5 ingestors
│   │   ├── base.py                 # Ingestor ABC
│   │   ├── fed_rss/main.py
│   │   ├── bls_rss/main.py
│   │   ├── treasury_rss/main.py
│   │   ├── x_curated/main.py
│   │   └── truth_social/main.py
│   ├── scorer/{Dockerfile, main.py}
│   ├── alerter/{Dockerfile, main.py}
│   ├── dashboard/{Dockerfile, api.py, static/index.html}
│   └── migrate/{Dockerfile}        # yoyo-migrations runner
├── migrations/001_initial.sql
└── tools/replay_dlq.py
```

### 7.2 Compose services

| Service | Image | Role |
|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.1` (KRaft) | Broker |
| `kafka-init` | `confluentinc/cp-kafka:7.6.1` | One-shot: creates 4 topics, exits |
| `postgres` | `postgres:16-alpine` | Database |
| `migrate` | Custom (yoyo + psycopg) | One-shot: applies pending migrations, exits |
| `ingestor-{fed_rss,bls_rss,treasury_rss,x_curated,truth_social}` | Custom (shared image) | One per source |
| `scorer` | Custom | Anthropic-calling consumer |
| `alerter` | Custom | Twilio sender |
| `dashboard` | Custom | FastAPI + SSE; only service exposing host port (`8080:8000`) |

### 7.3 Networking

All services on default Compose bridge network. Resolve by container name (`kafka:9092`, `postgres:5432`, `dashboard:8000`). Only `dashboard` publishes a host port.

### 7.4 Volumes

- `kafka_data`: Kafka log segments (named volume)
- `postgres_data`: Postgres data files (named volume)
- `./migrations` bind-mounted into `migrate` container

`docker compose down` keeps volumes; `docker compose down -v` wipes them.

### 7.5 Secrets management

`.env` interpolated by Compose. Never checked into git. Future: secret manager (Vault, Doppler) when going multi-host.

### 7.6 Pi-specific

A Pi 4 8GB with a SATA SSD (e.g. Argon One M.2 case) is sufficient for production at headline-alerter's volume. A Pi 5 with NVMe is not required.

**Memory:** the full stack uses ~1.2–1.5GB steady-state. Kafka's JVM heap is capped at 512MB via `KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"` in `docker-compose.yml`, so the 8GB RAM on either Pi is not a constraint.

**Storage:** a proper SSD is required — SD cards will wear out quickly under Kafka log writes and Postgres WAL. A SATA M.2 via USB 3.0 (Pi 4 Argon One case) works fine at this volume.

**Theoretical bottleneck on Pi 4:** the gigabit ethernet port shares the USB 3.0 bus with the SSD, so disk I/O and network traffic contend. In practice this only matters at millions of events/hour — irrelevant for headline-alerter's sub-100 events/hour normal rate and FOMC burst of a few hundred.

**Thermals:** Pi 4 in the Argon One case runs cool (~38°C idle). No active cooler required at this workload.

- Move Docker data root to the SSD (not the SD card if booting from SD — see Pi bootstrap runbook)

### 7.7 Remote access

Tailscale on the Pi and on the user's phone gives `http://hostname:8080` access from anywhere. WireGuard-encrypted, free for personal use, no port forwarding or domain required.

## 8. Build Phases

Spec covers all 5 ingestors. Build incrementally — each phase ships value by itself.

### Phase 0 — Skeleton (1-2 days)

- Repo scaffold, `docker-compose.yml`, shared library, `001_initial.sql`
- Kafka + Postgres + topic creation working
- Test producer + consumer prove plumbing

**Done when:** `docker compose up -d` brings the cluster up; topics exist; schema applied.

### Phase 1 — First end-to-end loop with `fed_rss` (3-4 days)

- `fed_rss` ingestor + scorer + alerter + dashboard
- DLQ pattern in place
- First real Fed event flows: ingest → score → SMS → dashboard

**Done when:** real Fed RSS event delivers an SMS; DLQ catches synthetic broken event without crashing scorer.

### Phase 2 — Add `bls_rss` (~1 day)

- Copy-paste-tweak from `fed_rss`
- Should require zero changes to `services/shared/`

**Done when:** both ingestors produce concurrently; scorer interleaves; dashboard shows source labels.

### Phase 3 — `truth_social` (2-3 days)

- The "iconic" source: Trump posts moving rates
- Robustness against scraping flakes (truthbrush)

**Done when:** real Trump post arrives, gets scored, dashboard shows it within ~2 minutes.

### Phase 4 — `x_curated` (1-2 days)

- Streaming source via `tweepy.StreamingClient`
- Account list in YAML
- Same `Ingestor.run()` contract; push-driven internally

**Done when:** stream stays connected, reconnects on drop; real Timiraos tweet flows within seconds.

### Phase 5 — `treasury_rss` (~half day)

- Trivial — same shape as `fed_rss` and `bls_rss`

**Done when:** all 5 ingestors enabled and healthy.

### Total v1: ~8-12 days part-time

Stop at any phase boundary and have a working system. Phase 1 is the single highest-value milestone.

## 9. Out of Scope / Future

The "knowingly deferred" list. Each becomes its own spec when real data or a real need calls for it.

| Item | Why deferred | Trigger to revisit |
|---|---|---|
| Cross-source dedup | Unknown duplication rate across our source mix | Duplicate alert spam observed for 2+ weeks |
| Burst suppression / cooldown | Right key (source vs. author vs. content-cluster) unknown | Burst patterns observed in real traffic |
| Per-source threshold tuning | No data on per-source noise levels | ~200 real scored events per source |
| Evaluator + market data + ground truth | Market data sourcing/format/conversion unscoped | "How do I know if this is any good" becomes pressing |
| Sonnet 4.6 escalation tier | Haiku may be sufficient | Eval data shows Haiku weak on borderlines |
| Real-time market context to scorer | No clean intraday data source verified | Eval data shows score quality regime-dependent |
| Twilio delivery webhooks | Needs public URL (Cloudflare Tunnel or VPS) | Wanting delivery-failure alerts |
| Multi-broker Kafka / replication / Schema Registry | v1 single broker is sufficient; replicated overlay exists for exercises | Going to production / multi-host |
| Multi-user | v1 is single-user | Sharing the system |
| Dashboard auth | Tailscale gates access for solo use | Public exposure of dashboard |
| Web UI polish (filter, search, charts) | v1 dashboard is bare minimum | Basic table feels insufficient |
| Backup / DR | Personal project, loss tolerance high | Wanting historical data preserved |
| Observability (Loki/Grafana, metrics) | `docker compose logs` + `/api/status` are enough | Multi-host or system-health alerting |
| CI/CD, image registry | One developer, build on Pi | Multiple environments / contributors |
| Pi 5 bootstrap runbook | Distinct from app architecture | Deploying on a fresh Pi (separate spec) |
| Cloud / AWS graduation | v1 runs on Pi | Going from "just me" to a real product |

## 10. Tech Stack Summary

- **Language:** Python 3.12
- **Kafka client:** `confluent-kafka-python`
- **HTTP framework:** FastAPI + `sse-starlette` + `uvicorn`
- **Postgres client:** `psycopg[binary]`
- **Migrations:** `yoyo-migrations`
- **AI client:** `anthropic` (direct API, not Bedrock)
- **Twilio:** `twilio-python`
- **RSS:** `feedparser`
- **X:** `tweepy`
- **Truth Social:** `truthbrush`
- **Logging:** `structlog`
- **Container:** Docker + Compose v2
- **Broker:** Confluent Kafka 7.6 (KRaft)
- **Database:** Postgres 16
- **Host (v1):** Raspberry Pi 4 8GB + SATA M.2 SSD (Argon One M.2 case)
- **Remote access:** Tailscale
- **Alerting:** Twilio (SMS / WhatsApp)
- **AI model:** Claude Haiku 4.5 (`claude-haiku-4-5`)
