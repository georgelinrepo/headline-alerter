# Truth Social Ingestor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `truth_social` ingestor that polls a configurable list of Truth Social accounts every 30s and produces normalized events to Kafka.

**Architecture:** A single `TruthSocialIngestor` class extends the existing `Ingestor` base class, overriding `_fetch_raw_items` (loops over accounts via truthbrush) and `_normalize_item` (strips HTML, maps to `NormalizedEvent`). Per-account `since_id` tracking minimises redundant API calls; the base class timestamp filter handles restart-safe dedup.

**Tech Stack:** Python 3.12, `truthbrush`, existing `services/shared/ingestor_base.Ingestor`, `html`+`re` stdlib for HTML stripping, `pytest` + `unittest.mock` for tests.

---

### Task 1: Add truthbrush dependency and test fixtures

**Files:**
- Modify: `pyproject.toml`
- Create: `tests/fixtures/truth_social_post.json`
- Create: `tests/fixtures/truth_social_reblog.json`

- [ ] **Step 1: Add truthbrush to pyproject.toml**

Open `pyproject.toml`. In the `dependencies` list, add after `httpx`:

```toml
    "truthbrush>=0.4.0",
```

The full dependencies block should look like:
```toml
dependencies = [
    "confluent-kafka>=2.4.0",
    "psycopg[binary]>=3.2.0",
    "structlog>=24.0.0",
    "feedparser>=6.0.11",
    "anthropic>=0.39.0",
    "twilio>=9.0.0",
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "sse-starlette>=2.0.0",
    "pyyaml>=6.0",
    "httpx>=0.27.0",
    "truthbrush>=0.4.0",
]
```

- [ ] **Step 2: Create original-post fixture**

Create `tests/fixtures/truth_social_post.json`:

```json
{
  "id": "114494826741302456",
  "created_at": "2026-05-15T12:34:56.000Z",
  "content": "<p>Big announcement: We are cutting tariffs on China by 50%! Great deal for America! <a href=\"https://truthsocial.com/tags/MAGA\">#MAGA</a></p>",
  "url": "https://truthsocial.com/@realDonaldTrump/114494826741302456",
  "uri": "https://truthsocial.com/users/realDonaldTrump/statuses/114494826741302456",
  "account": {
    "id": "107780257626128497",
    "username": "realDonaldTrump",
    "display_name": "Donald J. Trump"
  },
  "reblog": null,
  "reblogs_count": 5432,
  "replies_count": 1234,
  "favourites_count": 98765
}
```

- [ ] **Step 3: Create reblog fixture**

Create `tests/fixtures/truth_social_reblog.json`:

```json
{
  "id": "114494999999999999",
  "created_at": "2026-05-15T13:00:00.000Z",
  "content": "",
  "url": "https://truthsocial.com/@realDonaldTrump/114494999999999999",
  "uri": "https://truthsocial.com/users/realDonaldTrump/statuses/114494999999999999",
  "account": {
    "id": "107780257626128497",
    "username": "realDonaldTrump",
    "display_name": "Donald J. Trump"
  },
  "reblog": {
    "id": "114494888888888888",
    "created_at": "2026-05-15T12:55:00.000Z",
    "content": "<p>Treasury yields hit 5% on surprise jobs report!</p>"
  },
  "reblogs_count": 100,
  "replies_count": 50,
  "favourites_count": 1000
}
```

- [ ] **Step 4: Install dependency**

```bash
pip install -e ".[dev]"
```

Expected: installs truthbrush alongside existing packages, no errors.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml tests/fixtures/truth_social_post.json tests/fixtures/truth_social_reblog.json
git commit -m "chore: add truthbrush dependency and Truth Social test fixtures"
```

---

### Task 2: Implement and test `_strip_html` and `_normalize_item`

**Files:**
- Create: `services/ingestors/truth_social/__init__.py`
- Create: `services/ingestors/truth_social/main.py`
- Create: `tests/unit/ingestors/test_truth_social.py`

- [ ] **Step 1: Create package marker**

Create `services/ingestors/truth_social/__init__.py` as an empty file.

- [ ] **Step 2: Write failing tests for `_strip_html` and `_normalize_item`**

Create `tests/unit/ingestors/test_truth_social.py`:

```python
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from services.ingestors.truth_social.main import TruthSocialIngestor, _strip_html

FIXTURE_DIR = Path(__file__).parent.parent.parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())


@pytest.fixture
def post():
    return _load("truth_social_post.json")


@pytest.fixture
def reblog_post():
    return _load("truth_social_reblog.json")


@pytest.fixture
def ingestor():
    with patch("services.ingestors.truth_social.main.Api"):
        return TruthSocialIngestor(usernames=["realDonaldTrump"])


# ---- _strip_html ----

def test_strip_html_removes_tags():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"


def test_strip_html_unescapes_entities():
    assert _strip_html("AT&amp;T") == "AT&T"


def test_strip_html_collapses_whitespace():
    assert _strip_html("<p>one</p>\n<p>two</p>") == "one two"


# ---- _normalize_item ----

def test_normalize_item(ingestor, post):
    event = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    assert event.source == "truth_social"
    assert event.headline == (
        "Big announcement: We are cutting tariffs on China by 50%! "
        "Great deal for America! #MAGA"
    )
    assert event.body == event.headline
    assert event.url == "https://truthsocial.com/@realDonaldTrump/114494826741302456"
    assert event.ts_source == datetime(2026, 5, 15, 12, 34, 56, tzinfo=timezone.utc)
    assert event.metadata["post_id"] == "114494826741302456"
    assert event.metadata["account"] == "realDonaldTrump"
    assert event.metadata["is_reblog"] is False


def test_normalize_item_event_id_is_deterministic(ingestor, post):
    e1 = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    e2 = ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
    assert e1.event_id == e2.event_id


def test_normalize_item_reblog_uses_reblog_content(ingestor, reblog_post):
    event = ingestor._normalize_item({"post": reblog_post, "_username": "realDonaldTrump"})
    assert event.metadata["is_reblog"] is True
    assert "Treasury yields" in event.body


def test_normalize_item_missing_content_raises(ingestor, post):
    post["content"] = ""
    post["reblog"] = None
    with pytest.raises(ValueError, match="no content"):
        ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})


def test_normalize_item_missing_created_at_raises(ingestor, post):
    del post["created_at"]
    with pytest.raises(ValueError, match="no created_at"):
        ingestor._normalize_item({"post": post, "_username": "realDonaldTrump"})
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
pytest tests/unit/ingestors/test_truth_social.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `main.py` doesn't exist yet.

- [ ] **Step 4: Implement `_strip_html` and `_normalize_item` in main.py**

Create `services/ingestors/truth_social/main.py`:

```python
"""Truth Social ingestor.

Polls one or more Truth Social accounts via truthbrush, normalizes each
post to a NormalizedEvent, and emits it via the shared Ingestor base class.
"""
from __future__ import annotations
import hashlib
import html
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from truthbrush import Api

from services.shared.ingestor_base import Ingestor
from services.shared.logging import configure_logging
from services.shared.models import NormalizedEvent


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split())


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class TruthSocialIngestor(Ingestor):
    source_name = "truth_social"

    def __init__(self, *, usernames: list[str], producer=None,
                 poll_interval_seconds: int | None = None) -> None:
        super().__init__(producer=producer, poll_interval_seconds=poll_interval_seconds)
        self.usernames = usernames
        self._api = Api()
        self._since_ids: dict[str, str | None] = {u: None for u in usernames}

    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        raise NotImplementedError  # added in Task 3

    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        post = raw["post"]
        username = raw["_username"]

        content_html = post.get("content") or ""
        reblog = post.get("reblog")
        is_reblog = reblog is not None

        # For reposts, the original content lives in reblog.content.
        if not content_html.strip() and reblog:
            content_html = reblog.get("content", "")

        if not content_html:
            raise ValueError(f"post has no content: id={post.get('id')}")

        created_at = post.get("created_at")
        if not created_at:
            raise ValueError(f"post has no created_at: id={post.get('id')}")

        post_id = str(post["id"])
        ts_source = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        body = _strip_html(content_html)
        headline = body[:280]
        url = post.get("url") or post.get("uri", "")
        event_id = _sha256_hex(f"truth_social|{post_id}|{created_at}")

        return NormalizedEvent(
            event_id=event_id,
            source=self.source_name,
            ts_source=ts_source,
            ts_ingested=datetime.now(timezone.utc),
            headline=headline,
            body=body,
            url=url,
            metadata={
                "post_id": post_id,
                "account": username,
                "is_reblog": is_reblog,
            },
        )

    # _fetch_raw_items and main() added in Task 3
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
pytest tests/unit/ingestors/test_truth_social.py -v -k "strip_html or normalize"
```

Expected: all 7 tests pass.

- [ ] **Step 6: Commit**

```bash
git add services/ingestors/truth_social/ tests/unit/ingestors/test_truth_social.py
git commit -m "feat(truth_social): implement _strip_html and _normalize_item with tests"
```

---

### Task 3: Implement and test `_fetch_raw_items` and wire up `main()`

**Files:**
- Modify: `services/ingestors/truth_social/main.py`
- Modify: `tests/unit/ingestors/test_truth_social.py`

- [ ] **Step 1: Write failing tests for `_fetch_raw_items`**

Append to `tests/unit/ingestors/test_truth_social.py`:

```python
# ---- _fetch_raw_items ----

def test_fetch_raw_items_updates_since_id():
    posts = [
        {
            "id": "200",
            "content": "<p>newer</p>",
            "created_at": "2026-05-15T14:00:00.000Z",
            "url": "https://truthsocial.com/p/200",
            "reblog": None,
        },
        {
            "id": "100",
            "content": "<p>older</p>",
            "created_at": "2026-05-15T13:00:00.000Z",
            "url": "https://truthsocial.com/p/100",
            "reblog": None,
        },
    ]
    with patch("services.ingestors.truth_social.main.Api") as MockApi:
        MockApi.return_value.pull_statuses.return_value = iter(posts)
        ing = TruthSocialIngestor(usernames=["realDonaldTrump"])
        assert ing._since_ids["realDonaldTrump"] is None

        result = ing._fetch_raw_items()

        assert len(result) == 2
        assert ing._since_ids["realDonaldTrump"] == "200"
        MockApi.return_value.pull_statuses.assert_called_once_with(
            "realDonaldTrump", since_id=None, replies=False
        )

        # Second call passes since_id
        MockApi.return_value.pull_statuses.return_value = iter([])
        ing._fetch_raw_items()
        MockApi.return_value.pull_statuses.assert_called_with(
            "realDonaldTrump", since_id="200", replies=False
        )


def test_fetch_raw_items_per_account_isolation():
    good_post = {
        "id": "100",
        "content": "<p>ok</p>",
        "created_at": "2026-05-15T12:00:00.000Z",
        "url": "https://truthsocial.com/p/100",
        "reblog": None,
    }
    with patch("services.ingestors.truth_social.main.Api") as MockApi:
        def _side(username, **kwargs):
            if username == "badaccount":
                raise RuntimeError("auth failed")
            return iter([good_post])

        MockApi.return_value.pull_statuses.side_effect = _side
        ing = TruthSocialIngestor(usernames=["badaccount", "realDonaldTrump"])
        items = ing._fetch_raw_items()

    assert len(items) == 1
    assert items[0]["_username"] == "realDonaldTrump"
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/unit/ingestors/test_truth_social.py -v -k "fetch_raw"
```

Expected: FAIL — `_fetch_raw_items` exists but `main()` is missing (or passes — check).

- [ ] **Step 3: Implement `_fetch_raw_items` and add `main()` to main.py**

In `services/ingestors/truth_social/main.py`, replace the `_fetch_raw_items` stub and add `main()` at the bottom of the file:

Replace:
```python
    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        raise NotImplementedError  # added in Task 3
```

With:
```python
    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
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
```

Then add at the bottom of the file (after the class):

```python
def _usernames_from_env() -> list[str]:
    raw = os.environ.get("TRUTH_SOCIAL_USERNAMES", "").strip()
    if not raw:
        raise RuntimeError("TRUTH_SOCIAL_USERNAMES env var is required")
    return [u.strip() for u in raw.split(",") if u.strip()]


def main() -> int:
    configure_logging("ingestor-truth")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
    ing = TruthSocialIngestor(
        usernames=_usernames_from_env(),
        poll_interval_seconds=interval,
    )
    ing.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run all truth_social tests**

```bash
pytest tests/unit/ingestors/test_truth_social.py -v
```

Expected: all 9 tests pass.

- [ ] **Step 5: Run full test suite to catch regressions**

```bash
pytest tests/unit/ -v
```

Expected: all existing tests still pass, no new failures.

- [ ] **Step 6: Commit**

```bash
git add services/ingestors/truth_social/main.py tests/unit/ingestors/test_truth_social.py
git commit -m "feat(truth_social): implement _fetch_raw_items, main(); full unit test suite"
```

---

### Task 4: Wire up docker-compose and env vars

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.env.example`

- [ ] **Step 1: Add ingestor-truth service to docker-compose.yml**

In `docker-compose.yml`, after the `ingestor-cnbc` service block and before `scorer`, add:

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

- [ ] **Step 2: Add env vars to .env.example**

Open `.env.example`. Add after the existing vars:

```
# Truth Social ingestor
TRUTH_SOCIAL_USERNAMES=realDonaldTrump
TRUTHSOCIAL_USERNAME=
TRUTHSOCIAL_PASSWORD=
TRUTH_POLL_INTERVAL_SECONDS=30
```

- [ ] **Step 3: Add credentials to .env**

Open `.env` (gitignored). Add your real Truth Social login:

```
TRUTH_SOCIAL_USERNAMES=realDonaldTrump
TRUTHSOCIAL_USERNAME=<your-truth-social-email>
TRUTHSOCIAL_PASSWORD=<your-truth-social-password>
TRUTH_POLL_INTERVAL_SECONDS=30
```

- [ ] **Step 4: Verify compose config parses cleanly**

```bash
docker compose config --quiet
```

Expected: exits 0 with no errors.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat(truth_social): add ingestor-truth compose service and env vars"
```

---

### Task 5: Build image and smoke test end-to-end

**Files:** none — runtime verification only.

- [ ] **Step 1: Check the ingestors Dockerfile CMD**

```bash
cat services/ingestors/Dockerfile
```

Verify it has a CMD or ENTRYPOINT that accepts a module path, or that the existing `ingestor-cnbc` compose service sets a command. The `ingestor-truth` service needs to run `python -m services.ingestors.truth_social.main` — confirm this matches how `ingestor-cnbc` is invoked. If the Dockerfile has a hardcoded module, update the compose service to override with:

```yaml
    command: ["python", "-m", "services.ingestors.truth_social.main"]
```

- [ ] **Step 2: Build the image**

```bash
docker compose build ingestor-truth
```

Expected: build completes, `truthbrush` installed, no errors.

- [ ] **Step 3: Start the new service only**

```bash
docker compose up -d ingestor-truth
```

Expected: container starts (kafka and postgres must already be running).

- [ ] **Step 4: Watch logs for first poll**

```bash
docker compose logs -f ingestor-truth
```

Expected within 30s:
- `"hydrated last_ts"` — base class startup
- Either `"emitted"` (new posts found) or silence (no posts newer than cutoff)
- No `"truth_social fetch failed"` errors

If you see auth errors, double-check `TRUTHSOCIAL_USERNAME` and `TRUTHSOCIAL_PASSWORD` in `.env`.

- [ ] **Step 5: Verify events appear in Kafka**

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events.normalized \
  --from-beginning \
  --max-messages 5 | python3 -m json.tool
```

Expected: JSON events with `"source": "truth_social"` visible (if any posts were fetched).

- [ ] **Step 6: Final commit and push**

```bash
git add pyproject.toml  # in case pip install updated it
git status              # confirm only expected files
git push origin main
```
