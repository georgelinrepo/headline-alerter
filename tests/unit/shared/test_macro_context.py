"""Unit tests for macro_context DB helpers.

Integration tests (require POSTGRES_URL) are marked with pytest.mark.skipif.
"""
from __future__ import annotations
import os
import pytest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Unit: mock-based tests (no Postgres required)
# ---------------------------------------------------------------------------

def test_get_latest_context_returns_none_when_no_rows():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = None

    result = get_latest_context(conn)
    assert result is None


def test_get_latest_context_returns_summary_when_row_exists():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = ("**US Macro Context — 2026-05-16**\n\nFed holds at 3.5%",)

    result = get_latest_context(conn)
    assert result == "**US Macro Context — 2026-05-16**\n\nFed holds at 3.5%"


def test_get_latest_context_queries_correct_sql():
    from services.shared.macro_context import get_latest_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value
    cur.fetchone.return_value = None

    get_latest_context(conn)
    sql = cur.execute.call_args[0][0]
    assert "ORDER BY generated_at DESC" in sql
    assert "LIMIT 1" in sql


def test_save_context_inserts_row():
    from services.shared.macro_context import save_context
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur = conn.cursor.return_value

    save_context(conn, "some summary", "claude-sonnet-4-6")

    sql, params = cur.execute.call_args[0]
    assert "INSERT INTO macro_context" in sql
    assert "some summary" in params
    assert "claude-sonnet-4-6" in params
