# Headline Alerter — Dashboard Design

**Date:** 2026-05-13
**Author:** George Lin
**Status:** Approved
**Parent spec:** [`2026-05-03-headline-alerter-design.md`](2026-05-03-headline-alerter-design.md)

## 1. Overview

The dashboard is a live web UI for monitoring the headline-alerter pipeline. It shows a chronological table of ingested events with their AI scores, supports per-column filtering, and opens a detail panel on row click showing the scorer's reasoning and full news content. It operates in two modes: **live** (today's date, SSE stream active) and **historical** (past date selected, Postgres query only).

## 2. New Files

```
services/dashboard/
├── Dockerfile
├── api.py          # FastAPI app: Kafka consumers, SSE fan-out, REST endpoints
└── static/
    └── index.html  # Single-file frontend: Tabulator.js + SSE + detail panel
```

## 3. Architecture

### 3.1 Backend (`api.py`)

FastAPI service with two async background tasks consuming Kafka, an in-memory ring buffer, SSE fan-out to connected browsers, and four endpoints.

**Kafka consumers:**
- Consumer group `dashboard-normalized-{uuid}` subscribes to `events.normalized`
- Consumer group `dashboard-scored-{uuid}` subscribes to `events.scored`

Each instance uses a unique UUID suffix so multiple dashboard instances (e.g. two browser tabs on the same host) each receive every message independently.

**Ring buffer:** in-memory dict keyed by `event_id`, capped at 500 entries (evict oldest by `ts_ingested`). Holds today's live events for SSE fan-out. Warmed at startup from Postgres (today's events from midnight UTC) before accepting connections.

**SSE fan-out:** each connected `GET /api/stream` response gets its own `asyncio.Queue`. The Kafka consumer tasks broadcast to all live queues.

**Endpoints:**

| Route | Params | Purpose |
|---|---|---|
| `GET /` | — | Serves `static/index.html` |
| `GET /api/events` | `since` (ISO), `until` (ISO, optional) | Events from Postgres for the given window |
| `GET /api/stream` | — | SSE stream: `event` and `score` message types |
| `GET /api/status` | — | Per-source health: last-seen ts, error count, consumer lag |

**`/api/events` default:** `since=<today-midnight-UTC>`. No hard row cap — returns all events for the selected window. Historical queries (past dates) bypass the ring buffer and read directly from Postgres.

### 3.2 Frontend (`static/index.html`)

Single self-contained HTML file. No build step. Dependencies loaded from CDN:
- **Tabulator.js** (~150KB) — table with per-column header filters, live row upserts
- No other JS framework dependencies

**Layout:** two-panel split.
- Left (~55%): Tabulator event table + source status bar at the bottom
- Right (~45%): detail panel, populated on row click, empty state when nothing is selected

**Header bar (top of left panel):**
- Title: `HEADLINE ALERTER`
- Date picker (defaults to today). Selecting today → live mode. Selecting a past date → historical mode.
- Live mode indicator: `● LIVE` in green. Historical mode: `📅 YYYY-MM-DD · Historical` in gray.

### 3.3 Deployment

Added to `docker-compose.yml` as the `dashboard` service, exposing host port `8080:8000`. Same pattern as existing services: reads config from env vars, emits structured JSON logs to stdout.

## 4. Table Columns

| Column | Field | Filter type | Notes |
|---|---|---|---|
| Time | `ts_ingested` | Text (HH:MMZ match) | Displayed as `HH:MMZ` |
| Source | `source` | Text | `fed_rss`, `cnbc_rss`, etc. |
| Score | `score` | Numeric (`>=`) | Colored pill: red 8–10, amber 5–7, gray 0–4. `—` when unscored. |
| Direction | `direction` | Select dropdown | Values: `rates_higher`, `rates_lower`, `neutral`, `unclear`. `—` when unscored. |
| Conf | `confidence` | Numeric (`>=`) | Displayed as `0.82`. `—` when unscored. |
| Headline | `headline` | Text | Truncated; full text in detail panel. |
| Status | `status` | Select dropdown | Values: `received`, `scored`, `alerted`, `failed`. |

**Row styling:**
- High-score rows (8–10): full brightness
- Mid-score rows (5–7): normal brightness
- Low-score/unscored rows (0–4, `received`): visually dimmed
- Selected row: highlighted in muted blue

## 5. Detail Panel

Populated when a row is clicked. Updates live if a `score` SSE message arrives for the currently-selected `event_id`.

**Score header strip:**
- Score badge (colored pill)
- Direction label with arrow (↑ / ↓ / — / ?)
- Confidence value
- Source and timestamp
- `↗ source` link to original URL (hidden if `url` is null)

**Full headline:** displayed in full (not truncated).

**AI Reasoning section:**
- Label: `AI REASONING`
- The scorer's 2–4 sentence reasoning field
- Shown as `—` if not yet scored

**Full Content section:**
- Label: `FULL CONTENT`
- The `body` field, scrollable
- `No content available` if `body` is null

## 6. Live vs. Historical Mode

**Live mode (today selected):**
- On page load: `GET /api/events?since=<today-midnight-UTC>` populates Tabulator
- `EventSource('/api/stream')` opened immediately after
- New `event` SSE message → `table.updateOrAddData([row])` — row appears at top; active filters remain applied
- New `score` SSE message → `table.updateData([{id, score, direction, confidence, reasoning}])` — row updates in place; detail panel updates live if that row is selected

**Historical mode (past date selected):**
- SSE stream disconnected
- `GET /api/events?since=<date>T00:00Z&until=<date>T23:59Z` populates Tabulator
- No live updates; header shows historical mode indicator
- Switching back to today reconnects SSE

**SSE reconnection:** `EventSource` reconnects automatically on drop. On reconnect the browser re-fetches today's events to backfill any events missed during the disconnect, then reattaches the stream.

## 7. Status Bar

Rendered at the bottom of the left panel. Populated from `GET /api/status` (polled every 30s).

- Per-source pill: green `●` if a new event was seen in the last 5 minutes, gray `○` if stale
- Consumer lag indicator (shown if lag > 0)
- DLQ count

## 8. Error Handling

| Scenario | Behavior |
|---|---|
| SSE disconnect | `EventSource` auto-reconnects; browser backfills missed events on reconnect |
| Unscored row in table | Score/direction/confidence show `—`, row dimmed; updates in place when score arrives |
| `body` is null | Detail panel shows "No content available" in Full Content section |
| `url` is null | Source link hidden in detail panel |
| Backend startup | Ring buffer warmed from Postgres before accepting SSE connections |
| Kafka consumer error | Logged via structlog; dashboard keeps running; status bar reflects stale sources |
| Future date in picker | Disabled in the date picker UI |

## 9. Tech Stack

- **Backend:** FastAPI + `sse-starlette` + `uvicorn` (consistent with parent spec)
- **Kafka client:** `confluent-kafka-python` (asyncio wrapper)
- **Frontend table:** Tabulator.js (CDN, no build step)
- **Frontend JS:** Vanilla JS (`EventSource`, `fetch`) — no framework
- **Styling:** Inline CSS in `index.html`, dark clean theme (slate background, colored score badges)

## 10. Out of Scope

- Authentication (Tailscale gates access for solo use — parent spec § 9)
- Search across historical events beyond the selected day
- Chart/sparkline views of score distribution over time
- Mobile-optimized layout
