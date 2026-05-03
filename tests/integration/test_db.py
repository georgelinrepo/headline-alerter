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
