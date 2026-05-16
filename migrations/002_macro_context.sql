-- 002_macro_context.sql
-- depends: 001_initial

CREATE TABLE macro_context (
  id           SERIAL PRIMARY KEY,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  summary      TEXT NOT NULL,
  model        TEXT NOT NULL
);

-- !rollback DROP TABLE IF EXISTS macro_context;
