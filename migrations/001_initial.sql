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
