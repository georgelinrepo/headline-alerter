"""Postgres helpers for the macro_context table."""
from __future__ import annotations
import psycopg


def get_latest_context(conn: psycopg.Connection) -> str | None:
    """Return the most recent macro summary, or None if the table is empty."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT summary FROM macro_context ORDER BY generated_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        return row[0] if row else None


def save_context(conn: psycopg.Connection, summary: str, model: str) -> None:
    """Insert a new macro context row. Caller is responsible for committing the transaction."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO macro_context (summary, model) VALUES (%s, %s)",
            (summary, model),
        )
