# Headline Alerter — Phase 0: Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the project skeleton — Docker Compose with Kafka and Postgres, schema migrations, shared Python library for kafka/db/logging/models, end-to-end smoke test.

**Architecture:** Single Docker Compose stack. Kafka in KRaft mode (1 broker), Postgres 16, `kafka-init` one-shot to create topics, `migrate` one-shot to apply schema. Shared library in `services/shared/` provides factories (Kafka producer/consumer, Postgres connection, structured logging) so subsequent application services compose them without duplication.

**Tech Stack:** Python 3.12, Docker Compose v2, Confluent Kafka 7.6 (KRaft), Postgres 16, `confluent-kafka-python`, `psycopg[binary]`, `structlog`, `yoyo-migrations`, `pytest`.

**Spec:** [`docs/superpowers/specs/2026-05-03-headline-alerter-design.md`](../specs/2026-05-03-headline-alerter-design.md) — see Sections 4 (data model), 6 (operational concerns), 7 (Docker setup).

**Working directory for all commands:** `C:\Projects\headline-alerter\` (Windows / Git Bash). On Pi/Linux: `~/headline-alerter`. Adjust shell quoting as needed.

**Definition of done for Phase 0:**
1. `docker compose up -d` brings up `kafka`, `postgres`, runs `kafka-init` and `migrate` to completion
2. `docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092` lists all 4 topics
3. `docker compose exec postgres psql -U rates -d rates -c "\dt"` shows `events_archive` and `alert_history`
4. `python tools/smoke_test.py` produces an event to Kafka, consumes it back, writes a row to Postgres — prints "OK — Phase 0 smoke test passed"
5. All unit + integration tests pass (`pytest`)

---

## File Structure (created in this plan)

```
headline-alerter/
├── LICENSE                                        # Task 2
├── pyproject.toml                                 # Task 1
├── .python-version                                # Task 1
├── .env.example                                   # Task 1
├── README.md                                      # Task 1
├── docker-compose.yml                             # Task 3 (created), Task 4 (extended)
├── .github/
│   └── workflows/
│       └── ci.yml                                 # Task 2
├── migrations/
│   └── 001_initial.sql                            # Task 4
├── services/
│   ├── __init__.py                                # Task 5
│   ├── shared/
│   │   ├── __init__.py                            # Task 5
│   │   ├── models.py                              # Task 5
│   │   ├── logging.py                             # Task 6
│   │   ├── db.py                                  # Task 7
│   │   └── kafka_client.py                        # Task 8
│   └── migrate/
│       └── Dockerfile                             # Task 4
├── tests/
│   ├── __init__.py                                # Task 2
│   ├── conftest.py                                # Task 5
│   ├── unit/
│   │   ├── __init__.py                            # Task 2
│   │   ├── test_smoke.py                          # Task 2
│   │   └── shared/
│   │       ├── __init__.py                        # Task 5
│   │       ├── test_models.py                     # Task 5
│   │       └── test_logging.py                    # Task 6
│   └── integration/
│       ├── __init__.py                            # Task 7
│       ├── test_db.py                             # Task 7
│       └── test_kafka_client.py                   # Task 8
└── tools/
    └── smoke_test.py                              # Task 9
```

---

## Task 1: Repo bootstrap

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `.env.example`
- Create: `README.md`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "headline-alerter"
version = "0.1.0"
description = "Streaming pipeline scoring rates-market impact of news/social events."
requires-python = ">=3.12"
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
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.24.0",
    "respx>=0.21.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
filterwarnings = [
    "ignore::DeprecationWarning",
]

[tool.setuptools.packages.find]
where = ["."]
include = ["services*"]
```

- [ ] **Step 2: Create `.python-version`**

```
3.12
```

- [ ] **Step 3: Create `.env.example`**

```
# Postgres
POSTGRES_PASSWORD=changeme

# Anthropic API (Phase 1+)
ANTHROPIC_API_KEY=

# Twilio (Phase 1+)
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_FROM=
ALERT_RECIPIENT=
ALERT_CHANNEL=sms

# Alert thresholds (Phase 1+)
ALERT_THRESHOLD=7
MIN_CONFIDENCE=0.6

# X API (Phase 4+)
X_API_KEY=
```

- [ ] **Step 4: Create `README.md`**

```markdown
# Headline Alerter

Streaming pipeline that ingests news and social-media events, scores rates-market
impact via Claude Haiku 4.5, and alerts via Twilio SMS/WhatsApp.

See [`docs/superpowers/specs/2026-05-03-headline-alerter-design.md`](docs/superpowers/specs/2026-05-03-headline-alerter-design.md) for design.

## Setup

1. Install Docker + Docker Compose v2.
2. Copy `.env.example` to `.env` and fill in API keys.
3. `docker compose up -d`.
4. `python tools/smoke_test.py` to verify Phase 0 plumbing.

## Development

```bash
pip install -e ".[dev]"
pytest                              # unit + integration
pytest tests/unit                   # unit only (no Docker required)
```
```

- [ ] **Step 5: Create local `.env` from example for development**

Run:
```bash
cp .env.example .env
```

Open `.env` and set `POSTGRES_PASSWORD=changeme` (other vars can stay empty for Phase 0).

- [ ] **Step 6: Verify Python version**

Run:
```bash
python --version
```

Expected: `Python 3.12.x`. If not 3.12, install via pyenv/asdf/Windows installer matching `.python-version`.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .python-version .env.example README.md
git commit -m "chore: project bootstrap (pyproject, env example, readme)"
```

---

## Task 2: GitHub setup (private repo, MIT license, CI workflow)

Push the project to a private GitHub repo with a minimal CI workflow that runs unit tests on every push and PR.

**Files:**
- Create: `LICENSE`
- Create: `.github/workflows/ci.yml`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/test_smoke.py`

- [ ] **Step 1: Verify `gh` CLI is authenticated**

Run:
```bash
gh auth status
```

Expected: `Logged in to github.com as <your-username>`. If not authenticated, run `gh auth login` and follow the prompts (choose HTTPS, authenticate via web browser).

- [ ] **Step 2: Create `LICENSE` (MIT)**

```
MIT License

Copyright (c) 2026 George Lin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 3: Create test scaffolding directories**

Run:
```bash
mkdir -p tests/unit .github/workflows
touch tests/__init__.py tests/unit/__init__.py
```

- [ ] **Step 4: Create `tests/unit/test_smoke.py`**

```python
"""Placeholder smoke test — verifies the Python environment is set up correctly.
Real unit tests will land in subsequent tasks; this stays as a sanity check."""
import sys


def test_python_312_or_newer():
    assert sys.version_info >= (3, 12), f"Need Python 3.12+, got {sys.version}"
```

- [ ] **Step 5: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: pip

      - name: Install project + dev dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"

      - name: Run unit tests
        run: pytest tests/unit -v
```

- [ ] **Step 6: Verify locally that the placeholder test passes**

Run:
```bash
pytest tests/unit -v
```

Expected: 1 PASSED (`test_python_312_or_newer`).

- [ ] **Step 7: Commit**

```bash
git add LICENSE .github/workflows/ci.yml tests/__init__.py tests/unit/__init__.py tests/unit/test_smoke.py
git commit -m "chore: github setup (MIT license, CI workflow, test scaffolding)"
```

- [ ] **Step 8: Create the GitHub repo and push**

Run:
```bash
gh repo create headline-alerter --private --source=. --remote=origin --push
```

Expected: creates `https://github.com/<your-username>/headline-alerter` (private), adds `origin` remote, pushes the `main` branch with all four commits (spec, plan, bootstrap, github-setup).

- [ ] **Step 9: Verify on GitHub**

Run:
```bash
gh repo view --web
```

This opens the repo in your browser. Verify:
- All four commits visible in history
- LICENSE renders correctly
- README renders correctly
- `Actions` tab shows the CI workflow ran (it should be green — placeholder test passes)

Alternatively, check from CLI:
```bash
gh run list
```

Expected: one run, status `completed`, conclusion `success`.

- [ ] **Step 10: Set the upstream + verify**

Should already be set by `gh repo create --push`, but double-check:
```bash
git remote -v
git branch -vv
```

Expected: `origin` points to `github.com:<your-username>/headline-alerter`; `main` shows `[origin/main]` upstream.

**Going forward:** after each subsequent task's commit, optionally run `git push` to keep GitHub in sync. CI will run on each push and fail the run if any test breaks — useful early-warning signal as you build out the rest of Phase 0.

---

## Task 3: Docker Compose foundation (Kafka + Postgres + topic creation)

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    environment:
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://kafka:29093,EXTERNAL://0.0.0.0:9094"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      CLUSTER_ID: "headline-alerter-cluster-1"
      KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 30s
    ports:
      - "9094:9094"   # external listener; tests/host clients use localhost:9094
    restart: unless-stopped

  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: ["bash", "-c"]
    command:
      - |
        set -e
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic events.normalized --partitions 3 --replication-factor 1 \
          --config retention.ms=2592000000
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic events.scored --partitions 3 --replication-factor 1 \
          --config retention.ms=2592000000
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic alerts.outgoing --partitions 1 --replication-factor 1 \
          --config retention.ms=7776000000
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic events.dlq --partitions 1 --replication-factor 1 \
          --config retention.ms=1209600000
        echo "Topics created."
    restart: "no"

  postgres:
    image: postgres:16-alpine
    container_name: postgres
    environment:
      POSTGRES_USER: rates
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
      POSTGRES_DB: rates
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U rates -d rates"]
      interval: 5s
      timeout: 5s
      retries: 12
    ports:
      - "5432:5432"   # for host-side tests during dev
    restart: unless-stopped

volumes:
  kafka_data:
  postgres_data:
```

- [ ] **Step 2: Bring up the stack**

Run:
```bash
docker compose up -d
```

Expected output: pulls cp-kafka and postgres images, starts both containers, runs kafka-init to completion.

- [ ] **Step 3: Verify Kafka is healthy**

Run:
```bash
docker compose ps
```

Expected: `kafka` shows `healthy`, `postgres` shows `healthy`, `kafka-init` shows exited (0).

- [ ] **Step 4: Verify topics exist**

Run:
```bash
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

Expected (order may vary):
```
alerts.outgoing
events.dlq
events.normalized
events.scored
```

- [ ] **Step 5: Verify Postgres is reachable**

Run:
```bash
docker compose exec postgres psql -U rates -d rates -c "SELECT 1"
```

Expected: returns `1` row with column `?column? = 1`.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: docker compose with kafka (KRaft), postgres, kafka-init"
```

---

## Task 4: Schema migrations

**Files:**
- Create: `migrations/001_initial.sql`
- Create: `services/migrate/Dockerfile`
- Modify: `docker-compose.yml` (add `migrate` service)

- [ ] **Step 1: Write `migrations/001_initial.sql`**

```sql
-- 001_initial.sql
-- Phase 0 schema: events_archive, alert_history

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events_archive (
  id              TEXT PRIMARY KEY,
  source          TEXT NOT NULL,
  ts_source       TIMESTAMPTZ NOT NULL,
  ts_ingested     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ts_scored       TIMESTAMPTZ,
  ts_alerted      TIMESTAMPTZ,
  status          TEXT NOT NULL,

  headline        TEXT NOT NULL,
  body            TEXT,
  url             TEXT,
  metadata        JSONB DEFAULT '{}'::jsonb,

  score           INT,
  direction       TEXT,
  confidence      NUMERIC(3,2),
  reasoning       TEXT,
  model           TEXT
);

CREATE INDEX idx_events_ts_ingested ON events_archive (ts_ingested DESC);
CREATE INDEX idx_events_source_status ON events_archive (source, status);
CREATE INDEX idx_events_score ON events_archive (score DESC) WHERE status IN ('scored', 'alerted');

CREATE TABLE alert_history (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        TEXT NOT NULL REFERENCES events_archive(id),
  channel         TEXT NOT NULL,
  recipient       TEXT NOT NULL,
  twilio_sid      TEXT,
  sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivery_status TEXT,
  error           TEXT
);

CREATE INDEX idx_alerts_event ON alert_history (event_id);
CREATE INDEX idx_alerts_sent_at ON alert_history (sent_at DESC);

-- !rollback DROP INDEX IF EXISTS idx_alerts_sent_at;
-- !rollback DROP INDEX IF EXISTS idx_alerts_event;
-- !rollback DROP TABLE IF EXISTS alert_history;
-- !rollback DROP INDEX IF EXISTS idx_events_score;
-- !rollback DROP INDEX IF EXISTS idx_events_source_status;
-- !rollback DROP INDEX IF EXISTS idx_events_ts_ingested;
-- !rollback DROP TABLE IF EXISTS events_archive;
```

- [ ] **Step 2: Write `services/migrate/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir yoyo-migrations==9.0.0 "psycopg[binary]>=3.2.0"
COPY migrations /app/migrations
CMD ["sh", "-c", "yoyo apply --batch --database \"$POSTGRES_URL\" /app/migrations"]
```

- [ ] **Step 3: Add `migrate` service to `docker-compose.yml`**

Add this block under `services:` (after `postgres`):

```yaml
  migrate:
    build:
      context: .
      dockerfile: services/migrate/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      POSTGRES_URL: "postgresql://rates:${POSTGRES_PASSWORD:-changeme}@postgres:5432/rates"
    restart: "no"
```

- [ ] **Step 4: Build and run migrations**

Run:
```bash
docker compose build migrate
docker compose up migrate
```

Expected output: `migrate-1 | ...applying 001_initial...` then `migrate-1 exited with code 0`.

- [ ] **Step 5: Verify tables exist**

Run:
```bash
docker compose exec postgres psql -U rates -d rates -c "\dt"
```

Expected: lists `events_archive`, `alert_history`, `_yoyo_log`, `_yoyo_migration`, `_yoyo_version`.

Run:
```bash
docker compose exec postgres psql -U rates -d rates -c "\d events_archive"
```

Expected: shows the columns defined in the migration (id, source, ts_source, ts_ingested, ts_scored, ts_alerted, status, headline, body, url, metadata, score, direction, confidence, reasoning, model).

- [ ] **Step 6: Verify rerun is idempotent**

Run:
```bash
docker compose up migrate
```

Expected: yoyo reports no migrations to apply, exits 0.

- [ ] **Step 7: Commit**

```bash
git add migrations/ services/migrate/ docker-compose.yml
git commit -m "feat: postgres schema migrations via yoyo (events_archive, alert_history)"
```

---

## Task 5: Shared models + project package skeleton

**Files:**
- Create: `services/__init__.py`
- Create: `services/shared/__init__.py`
- Create: `services/shared/models.py`
- Create: `tests/conftest.py`
- Create: `tests/unit/shared/__init__.py`
- Create: `tests/unit/shared/test_models.py`

(Note: `tests/__init__.py` and `tests/unit/__init__.py` already exist from Task 2.)

- [ ] **Step 1: Install dev dependencies**

Run:
```bash
pip install -e ".[dev]"
```

Expected: installs project + pytest. May need a virtualenv first (`python -m venv .venv && source .venv/Scripts/activate` on Git Bash, or `.\.venv\Scripts\activate` on PowerShell).

- [ ] **Step 2: Create empty `__init__.py` files**

```bash
mkdir -p services/shared tests/unit/shared
touch services/__init__.py services/shared/__init__.py
touch tests/unit/shared/__init__.py
```

- [ ] **Step 3: Create `tests/conftest.py`**

```python
"""Shared pytest fixtures."""
import os
import pytest


@pytest.fixture
def env(monkeypatch):
    """Convenient env-var setter for tests."""
    def _set(**kwargs):
        for k, v in kwargs.items():
            monkeypatch.setenv(k, str(v))
    return _set
```

- [ ] **Step 4: Write the failing test for models**

Create `tests/unit/shared/test_models.py`:

```python
from datetime import datetime, timezone
from services.shared.models import NormalizedEvent, ScoredEvent


def test_normalized_event_round_trip():
    ev = NormalizedEvent(
        event_id="x-123",
        source="fed_rss",
        ts_source=datetime(2026, 5, 3, 14, 32, tzinfo=timezone.utc),
        ts_ingested=datetime(2026, 5, 3, 14, 32, 8, tzinfo=timezone.utc),
        headline="Test headline",
        body="Test body",
        url="https://example.com",
        metadata={"raw_id": "abc"},
    )
    d = ev.to_dict()
    assert d["event_id"] == "x-123"
    assert d["source"] == "fed_rss"
    assert d["metadata"] == {"raw_id": "abc"}
    restored = NormalizedEvent.from_dict(d)
    assert restored == ev


def test_normalized_event_optional_fields_default():
    ev = NormalizedEvent(
        event_id="x-1",
        source="x",
        ts_source=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ts_ingested=datetime(2026, 1, 1, tzinfo=timezone.utc),
        headline="h",
    )
    assert ev.body is None
    assert ev.url is None
    assert ev.metadata == {}
    restored = NormalizedEvent.from_dict(ev.to_dict())
    assert restored == ev


def test_scored_event_round_trip():
    sc = ScoredEvent(
        event_id="x-123",
        score=8,
        direction="rates_lower",
        confidence=0.75,
        reasoning="Powell tone notably dovish",
        model="claude-haiku-4-5",
        scored_at=datetime(2026, 5, 3, 14, 32, 11, tzinfo=timezone.utc),
    )
    d = sc.to_dict()
    assert d["score"] == 8
    assert d["confidence"] == 0.75
    restored = ScoredEvent.from_dict(d)
    assert restored == sc
```

- [ ] **Step 5: Run the failing test**

Run:
```bash
pytest tests/unit/shared/test_models.py -v
```

Expected: FAILS with `ModuleNotFoundError: No module named 'services.shared.models'`.

- [ ] **Step 6: Implement `services/shared/models.py`**

```python
"""Event dataclasses shared between ingestor, scorer, alerter, dashboard."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class NormalizedEvent:
    """A news/social event after normalization, before scoring."""
    event_id: str
    source: str
    ts_source: datetime
    ts_ingested: datetime
    headline: str
    body: str | None = None
    url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "ts_source": self.ts_source.isoformat(),
            "ts_ingested": self.ts_ingested.isoformat(),
            "headline": self.headline,
            "body": self.body,
            "url": self.url,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NormalizedEvent:
        return cls(
            event_id=d["event_id"],
            source=d["source"],
            ts_source=datetime.fromisoformat(d["ts_source"]),
            ts_ingested=datetime.fromisoformat(d["ts_ingested"]),
            headline=d["headline"],
            body=d.get("body"),
            url=d.get("url"),
            metadata=d.get("metadata") or {},
        )


@dataclass
class ScoredEvent:
    """The output of the scorer service. Joined to NormalizedEvent by event_id."""
    event_id: str
    score: int
    direction: str
    confidence: float
    reasoning: str
    model: str
    scored_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "score": self.score,
            "direction": self.direction,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "model": self.model,
            "scored_at": self.scored_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ScoredEvent:
        return cls(
            event_id=d["event_id"],
            score=int(d["score"]),
            direction=d["direction"],
            confidence=float(d["confidence"]),
            reasoning=d["reasoning"],
            model=d["model"],
            scored_at=datetime.fromisoformat(d["scored_at"]),
        )
```

- [ ] **Step 7: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/shared/test_models.py -v
```

Expected: 3 PASSED.

- [ ] **Step 8: Commit**

```bash
git add services/__init__.py services/shared/__init__.py services/shared/models.py \
        tests/conftest.py tests/unit/shared/__init__.py \
        tests/unit/shared/test_models.py
git commit -m "feat: NormalizedEvent and ScoredEvent dataclasses with serialization"
```

---

## Task 6: Structured logging

**Files:**
- Create: `services/shared/logging.py`
- Create: `tests/unit/shared/test_logging.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/shared/test_logging.py`:

```python
import json
from services.shared.logging import configure_logging, get_logger


def test_logger_emits_json_with_required_fields(capsys):
    configure_logging(service_name="test_svc")
    log = get_logger()
    log.info("hello", event_id="x-1", value=42)
    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["event"] == "hello"
    assert obj["event_id"] == "x-1"
    assert obj["value"] == 42
    assert obj["service"] == "test_svc"
    assert obj["level"] == "info"
    assert "timestamp" in obj


def test_logger_warning_level(capsys):
    configure_logging(service_name="svc2")
    log = get_logger()
    log.warning("careful", code=99)
    line = capsys.readouterr().out.strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["level"] == "warning"
    assert obj["service"] == "svc2"
    assert obj["code"] == 99
```

- [ ] **Step 2: Run the failing test**

Run:
```bash
pytest tests/unit/shared/test_logging.py -v
```

Expected: FAILS with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `services/shared/logging.py`**

```python
"""Structured JSON logging via structlog. One configuration shared across services."""
import logging
import sys
import structlog


def configure_logging(service_name: str, level: str = "INFO") -> None:
    """Configure structlog to emit JSON to stdout. Bind `service` into context."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    # Reset and bind the service tag.
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str = "") -> structlog.BoundLogger:
    return structlog.get_logger(name)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
pytest tests/unit/shared/test_logging.py -v
```

Expected: 2 PASSED.

- [ ] **Step 5: Commit**

```bash
git add services/shared/logging.py tests/unit/shared/test_logging.py
git commit -m "feat: structured JSON logging via structlog"
```

---

## Task 7: Postgres connection helper

**Files:**
- Create: `services/shared/db.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_db.py`

- [ ] **Step 1: Ensure Postgres is running locally**

Run:
```bash
docker compose up -d postgres
docker compose ps postgres
```

Expected: postgres is `healthy`. If not, run `docker compose up -d` and wait.

- [ ] **Step 2: Create `tests/integration/__init__.py`**

```bash
mkdir -p tests/integration
touch tests/integration/__init__.py
```

- [ ] **Step 3: Write the failing integration test**

Create `tests/integration/test_db.py`:

```python
"""Integration test: requires `docker compose up -d postgres`."""
import pytest
from services.shared.db import connect


@pytest.fixture
def pg_url(env):
    env(POSTGRES_URL="postgresql://rates:changeme@localhost:5432/rates")


def test_connect_and_select(pg_url):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
    assert row == (1,)


def test_connect_missing_env_raises(monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    with pytest.raises(RuntimeError, match="POSTGRES_URL"):
        with connect() as _:
            pass
```

- [ ] **Step 4: Run the failing test**

Run:
```bash
pytest tests/integration/test_db.py -v
```

Expected: FAILS with `ModuleNotFoundError: No module named 'services.shared.db'`.

- [ ] **Step 5: Implement `services/shared/db.py`**

```python
"""Postgres connection helper. Reads POSTGRES_URL from environment."""
import os
from contextlib import contextmanager
from typing import Iterator
import psycopg


def get_connection_url() -> str:
    url = os.environ.get("POSTGRES_URL")
    if not url:
        raise RuntimeError("POSTGRES_URL env var is required")
    return url


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    """Open a Postgres connection; auto-close on exit."""
    url = get_connection_url()
    with psycopg.connect(url) as conn:
        yield conn
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
pytest tests/integration/test_db.py -v
```

Expected: 2 PASSED.

If you see a connection error, ensure: (a) `docker compose up -d postgres` is running, (b) port 5432 is exposed (it is, per `docker-compose.yml`), (c) `POSTGRES_PASSWORD=changeme` matches your `.env`.

- [ ] **Step 7: Commit**

```bash
git add services/shared/db.py tests/integration/__init__.py tests/integration/test_db.py
git commit -m "feat: postgres connection helper with integration test"
```

---

## Task 8: Kafka producer/consumer factories

**Files:**
- Create: `services/shared/kafka_client.py`
- Create: `tests/integration/test_kafka_client.py`

- [ ] **Step 1: Ensure Kafka is running locally**

Run:
```bash
docker compose up -d kafka
docker compose ps kafka
```

Expected: kafka is `healthy`.

- [ ] **Step 2: Write the failing integration test**

Create `tests/integration/test_kafka_client.py`:

```python
"""Integration tests for Kafka producer/consumer factories.
Requires `docker compose up -d kafka`. Connects from host via localhost:9094 (EXTERNAL listener)."""
import json
import time
import uuid
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from services.shared.kafka_client import (
    make_producer,
    make_consumer,
    produce,
    flush,
)


BROKERS_HOST = "localhost:9094"


@pytest.fixture
def kafka_brokers(env):
    env(KAFKA_BROKERS=BROKERS_HOST)


@pytest.fixture
def temp_topic(kafka_brokers):
    topic = f"test.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BROKERS_HOST})
    fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    for _, f in fs.items():
        f.result(timeout=10)
    yield topic
    fs = admin.delete_topics([topic])
    for _, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception:
            pass


def _consume_one(consumer, deadline_seconds=10):
    deadline = time.time() + deadline_seconds
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        return msg
    return None


def test_produce_consume_round_trip(temp_topic):
    producer = make_producer()
    payload = {"event_id": "abc", "headline": "test"}
    produce(producer, temp_topic, key="abc", payload=payload)
    flush(producer)

    consumer = make_consumer(f"test-cg-{uuid.uuid4()}", [temp_topic])
    try:
        msg = _consume_one(consumer)
        assert msg is not None, "did not receive message within deadline"
        received = json.loads(msg.value().decode())
        assert received == payload
    finally:
        consumer.close()


def test_two_consumer_groups_each_see_all_messages(temp_topic):
    """Two distinct consumer groups should each receive all produced messages
    (the 'fanout' pattern that make_unique_consumer relies on for the dashboard)."""
    producer = make_producer()
    produce(producer, temp_topic, key="a", payload={"id": "a"})
    produce(producer, temp_topic, key="b", payload={"id": "b"})
    flush(producer)

    c1 = make_consumer(f"viewer-1-{uuid.uuid4()}", [temp_topic])
    c2 = make_consumer(f"viewer-2-{uuid.uuid4()}", [temp_topic])

    def collect(consumer, n=2, deadline_s=10):
        seen = set()
        deadline = time.time() + deadline_s
        while len(seen) < n and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                seen.add(json.loads(msg.value().decode())["id"])
        return seen

    try:
        assert collect(c1) == {"a", "b"}
        assert collect(c2) == {"a", "b"}
    finally:
        c1.close()
        c2.close()
```

- [ ] **Step 3: Run the failing test**

Run:
```bash
pytest tests/integration/test_kafka_client.py -v
```

Expected: FAILS with `ModuleNotFoundError`.

- [ ] **Step 4: Implement `services/shared/kafka_client.py`**

```python
"""Kafka producer/consumer factories used by every service.

The same module is used both inside Compose containers (KAFKA_BROKERS=kafka:9092)
and from the host during testing (KAFKA_BROKERS=localhost:9094).
"""
import json
import os
import uuid
from typing import Any
from confluent_kafka import Producer, Consumer


def _brokers() -> str:
    return os.environ.get("KAFKA_BROKERS", "localhost:9094")


def make_producer() -> Producer:
    """Idempotent JSON producer with snappy compression."""
    return Producer({
        "bootstrap.servers": _brokers(),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 10,
    })


def make_consumer(
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
) -> Consumer:
    """Standard worker consumer: manual offset commit, replay-friendly."""
    consumer = Consumer({
        "bootstrap.servers": _brokers(),
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": auto_offset_reset,
        "session.timeout.ms": 30000,
        "isolation.level": "read_committed",
    })
    consumer.subscribe(topics)
    return consumer


def make_unique_consumer(topics: list[str]) -> Consumer:
    """Per-instance consumer group → fanout (every message to every instance).

    Used by the dashboard service so multiple dashboard processes each see all
    events. `auto.offset.reset='latest'` means a fresh dashboard only gets new
    messages — historical state comes from Postgres on warm-up instead.
    """
    return make_consumer(
        f"viewer-{uuid.uuid4()}",
        topics,
        auto_offset_reset="latest",
    )


def produce(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    """Produce a JSON-serialized payload with a string key."""
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )


def flush(producer: Producer, timeout: float = 5.0) -> None:
    """Block until in-flight messages are delivered (or timeout)."""
    producer.flush(timeout)
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
```bash
pytest tests/integration/test_kafka_client.py -v
```

Expected: 2 PASSED. The first run may take a few seconds as Kafka assigns partitions.

If you see `KafkaError: Broker transport failure`, verify Kafka container is up and port 9094 is exposed.

- [ ] **Step 6: Commit**

```bash
git add services/shared/kafka_client.py tests/integration/test_kafka_client.py
git commit -m "feat: kafka producer/consumer factories with integration tests"
```

---

## Task 9: Phase 0 smoke test

**Files:**
- Create: `tools/smoke_test.py`

- [ ] **Step 1: Ensure full stack is up**

Run:
```bash
docker compose up -d
docker compose ps
```

Expected: `kafka` healthy, `postgres` healthy, `kafka-init` and `migrate` exited (0).

- [ ] **Step 2: Write `tools/smoke_test.py`**

```python
"""Phase 0 smoke test.

End-to-end: produce a synthetic event to events.normalized, consume it back,
write a row to events_archive, read it back. Verifies all of:
- Kafka producer + consumer factory
- Postgres connection
- Schema (events_archive present)
- Models serialization

Run from host: `python tools/smoke_test.py`
Requires: `docker compose up -d` (kafka + postgres + topics + schema applied).
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.shared.kafka_client import (
    make_producer, make_consumer, produce, flush,
)
from services.shared.db import connect
from services.shared.models import NormalizedEvent
from services.shared.logging import configure_logging, get_logger


def _ensure_env():
    os.environ.setdefault("KAFKA_BROKERS", "localhost:9094")
    os.environ.setdefault(
        "POSTGRES_URL",
        "postgresql://rates:changeme@localhost:5432/rates",
    )


def main() -> int:
    _ensure_env()
    configure_logging("smoke")
    log = get_logger()

    event = NormalizedEvent(
        event_id=f"smoke-{uuid.uuid4().hex[:12]}",
        source="smoke",
        ts_source=datetime.now(timezone.utc),
        ts_ingested=datetime.now(timezone.utc),
        headline="smoke test event",
        body="hello from smoke test",
        url=None,
        metadata={"phase": 0},
    )

    log.info("producing", event_id=event.event_id)
    producer = make_producer()
    produce(producer, "events.normalized", key=event.source, payload=event.to_dict())
    flush(producer)
    log.info("produced")

    log.info("consuming")
    consumer = make_consumer(f"smoke-cg-{uuid.uuid4()}", ["events.normalized"])
    deadline = time.time() + 15
    received = None
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value().decode())
            if payload.get("event_id") == event.event_id:
                received = payload
                break
    finally:
        consumer.close()
    assert received is not None, "did not receive our event back from Kafka"
    log.info("consumed", event_id=received["event_id"])

    log.info("writing to events_archive")
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events_archive
                  (id, source, ts_source, status, headline, body, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    received["event_id"],
                    received["source"],
                    received["ts_source"],
                    "received",
                    received["headline"],
                    received["body"],
                    json.dumps(received.get("metadata") or {}),
                ),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, source, status FROM events_archive WHERE id = %s",
                (event.event_id,),
            )
            row = cur.fetchone()
    assert row is not None, "row not found in events_archive"
    log.info("verified", id=row[0], source=row[1], status=row[2])

    print("OK — Phase 0 smoke test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run the smoke test**

Run:
```bash
python tools/smoke_test.py
```

Expected output (last line):
```
OK — Phase 0 smoke test passed
```

If it fails:
- "did not receive our event back from Kafka" → check `docker compose ps kafka` is healthy and topics exist (`docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092`)
- Postgres connection error → check `docker compose ps postgres` and that `migrate` ran successfully

- [ ] **Step 4: Verify the row persists**

Run:
```bash
docker compose exec postgres psql -U rates -d rates -c \
  "SELECT id, source, status FROM events_archive WHERE source = 'smoke' LIMIT 5;"
```

Expected: at least one row with `source = smoke`, `status = received`.

- [ ] **Step 5: Run the full test suite once**

Run:
```bash
pytest -v
```

Expected: all unit + integration tests PASS (6 unit, 4 integration = 10 tests).

- [ ] **Step 6: Commit**

```bash
git add tools/smoke_test.py
git commit -m "feat: phase 0 smoke test (kafka + postgres + models end-to-end)"
```

---

## Phase 0 acceptance check

Run all of these in sequence:

```bash
# 1. Stack is up
docker compose up -d
docker compose ps                                   # all services healthy or completed

# 2. Topics exist
docker compose exec kafka \
  kafka-topics --list --bootstrap-server kafka:9092 \
  | sort                                            # 4 topics

# 3. Schema applied
docker compose exec postgres \
  psql -U rates -d rates -c "\dt"                   # events_archive, alert_history present

# 4. Smoke test passes
python tools/smoke_test.py                          # OK — Phase 0 smoke test passed

# 5. Test suite passes
pytest -v                                           # all green
```

If all five pass, Phase 0 is done. Phase 1 plan can be written next.

---

## What this phase does NOT cover (intentional)

These come in Phase 1 (its own separate plan):

- Ingestor base class and any source-specific ingestor
- Scorer (Anthropic API integration)
- Alerter (Twilio integration)
- Dashboard API (FastAPI + SSE)
- DLQ replay tool
- End-to-end integration test with a real RSS event

These come in even later phases (each its own plan):

- Additional ingestors (`bls_rss`, `truth_social`, `x_curated`, `treasury_rss`)
- Multi-broker Kafka overlay
- Pi 5 bootstrap runbook
