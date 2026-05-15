# Truth Social Ingestor — Design

**Date:** 2026-05-15
**Author:** George Lin
**Status:** Approved

## 1. Overview

A new ingestor service (`ingestor-truth`) that polls one or more Truth Social accounts via `truthbrush`, normalizes each post to a `NormalizedEvent`, and produces it to `events.normalized`. Follows the same `Ingestor` base class contract as `ingestor-cnbc`.

**Primary motivation:** Trump posts on Truth Social routinely move US rates markets within minutes. This source fills a gap that no RSS feed covers.

**Build phase:** Phase 3 of the headline-alerter spec. Done when a real post from a watched account arrives, gets scored, and appears on the dashboard within ~2 minutes.

## 2. Architecture

No new components. One new service in `docker-compose.yml` using the existing shared ingestor `Dockerfile`. One new Python module `services/ingestors/truth_social/main.py`.

```
truthbrush.Api.pull_statuses()
        │
        ▼
TruthSocialIngestor._fetch_raw_items()   ← loops over accounts
        │
        ▼
TruthSocialIngestor._normalize_item()   ← strips HTML, builds NormalizedEvent
        │
        ▼
Ingestor base (archive + produce to events.normalized)
```

## 3. Configuration

All config via environment variables, consistent with other ingestors.

| Env var | Required | Default | Notes |
|---|---|---|---|
| `TRUTH_SOCIAL_USERNAMES` | yes | — | Comma-separated list, e.g. `realDonaldTrump,PeteNavarro45` |
| `TRUTHSOCIAL_USERNAME` | yes | — | Truth Social login email (truthbrush auth) |
| `TRUTHSOCIAL_PASSWORD` | yes | — | Truth Social password (truthbrush auth) |
| `POLL_INTERVAL_SECONDS` | no | `30` | Seconds between polls |
| `KAFKA_BROKERS` | yes | — | Inherited from compose |
| `POSTGRES_URL` | yes | — | Inherited from compose |

`TRUTHSOCIAL_USERNAME` and `TRUTHSOCIAL_PASSWORD` are added to `.env` / `.env.example`.

## 4. Implementation

### 4.1 File layout

```
services/ingestors/truth_social/
    __init__.py
    main.py
```

Uses the existing `services/ingestors/Dockerfile` — no new image needed. `truthbrush` added to `pyproject.toml` dependencies.

### 4.2 Class design

```python
class TruthSocialIngestor(Ingestor):
    source_name = "truth_social"

    def __init__(self, *, usernames: list[str], producer=None,
                 poll_interval_seconds: int | None = None) -> None:
        super().__init__(producer=producer, poll_interval_seconds=poll_interval_seconds)
        self.usernames = usernames
        self._api = Api()                          # reads TRUTHSOCIAL_* from env
        self._since_ids: dict[str, str | None] = {u: None for u in usernames}

    def _fetch_raw_items(self) -> list[dict]:
        out = []
        for username in self.usernames:
            try:
                posts = list(self._api.pull_statuses(
                    username,
                    since_id=self._since_ids[username],
                    replies=False,
                ))
                if posts:
                    # Mastodon API returns newest-first; posts[0] is the most recent.
                    self._since_ids[username] = posts[0]["id"]
                out.extend({"post": p, "_username": username} for p in posts)
            except Exception as e:
                self.log.warning("truth_social fetch failed for account",
                                 username=username, error=str(e))
        return out

    def _normalize_item(self, raw: dict) -> NormalizedEvent:
        post = raw["post"]
        username = raw["_username"]
        # strip HTML, build event ...
```

### 4.3 Normalization

Truth Social posts use a Mastodon-like JSON structure. Mapping:

| Post field | `NormalizedEvent` field | Notes |
|---|---|---|
| `content` (HTML) | `headline` + `body` | Strip HTML tags; headline = first 280 chars of stripped text; body = full stripped text |
| `created_at` | `ts_source` | ISO 8601 string → UTC datetime |
| `url` | `url` | Canonical post URL |
| `sha256("truth_social\|{id}\|{created_at}")` | `event_id` | Deterministic; replay-safe |
| — | `source` | `"truth_social"` |
| `id`, `account.username`, `reblog != None` | `metadata` | `{"post_id", "account", "is_reblog"}` |

Reposts (`reblog != None`) are included — scorer decides relevance. `metadata["is_reblog"] = True` allows future filtering.

HTML stripping uses Python's stdlib `html.parser` — no extra dependency.

### 4.4 Dedup strategy

Two-layer:

1. **`since_id` (in-memory, per-account):** passed to `pull_statuses` so the API only returns posts newer than the last seen ID. Efficient — avoids re-fetching known posts within a session.
2. **Timestamp filter (base class):** `_last_ts_source` hydrated from `events_archive` on startup. Catches any overlap after a restart when `since_id` is lost.

On restart, `since_id` resets to `None` so `pull_statuses` returns recent posts; the base class timestamp filter drops anything already seen. At most one poll cycle of redundant API calls.

## 5. Error Handling

| Failure | Behavior |
|---|---|
| Per-account network/auth error | Log warning, skip that account, continue with others |
| All accounts fail | `_fetch_raw_items` returns `[]`; base class backoff applies |
| HTML strip failure | `_normalize_item` raises → base class routes to `events.dlq`, `stage='ingest_parse'` |
| Missing `content` or `created_at` | Raise in `_normalize_item` → DLQ |
| truthbrush scraping flake (partial data) | Same as parse error → DLQ |

## 6. Docker Compose

New service added to `docker-compose.yml`:

```yaml
ingestor-truth:
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
    TRUTH_SOCIAL_USERNAMES: "${TRUTH_SOCIAL_USERNAMES}"
    TRUTHSOCIAL_USERNAME: "${TRUTHSOCIAL_USERNAME}"
    TRUTHSOCIAL_PASSWORD: "${TRUTHSOCIAL_PASSWORD}"
    POLL_INTERVAL_SECONDS: "${TRUTH_POLL_INTERVAL_SECONDS:-30}"
  restart: unless-stopped
```

## 7. Dependencies

`truthbrush` added to `pyproject.toml` under `dependencies`. No pinned version — use latest stable.

## 8. Testing

Unit tests only — no real credentials in CI.

| Test | What it covers |
|---|---|
| `test_normalize_item` | Fixture post dict → verify `event_id`, `headline`, `body`, `ts_source`, `metadata`, HTML stripped |
| `test_normalize_item_reblog` | Repost fixture → `metadata["is_reblog"] == True` |
| `test_normalize_item_missing_fields` | Missing `created_at` or `content` → raises |
| `test_fetch_raw_items_since_id` | Mock `Api.pull_statuses`; verify `since_id` updated correctly per account across two cycles |
| `test_fetch_raw_items_per_account_isolation` | One account raises; verify other account's posts still returned |

Test fixtures live in `tests/fixtures/truth_social_post.json`.

## 9. Out of Scope

- Fetching replies (`replies=False` always in v1)
- Persisting `since_id` to Postgres (timestamp filter is sufficient for restart safety)
- Filtering reposts (deferred until real traffic shows whether repost noise is a problem)
- Multiple Truth Social credentials / account rotation
