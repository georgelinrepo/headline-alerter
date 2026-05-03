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
