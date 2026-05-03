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
