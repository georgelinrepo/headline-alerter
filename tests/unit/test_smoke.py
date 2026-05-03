"""Placeholder smoke test — verifies the Python environment is set up correctly.
Real unit tests will land in subsequent tasks; this stays as a sanity check."""
import sys


def test_python_312_or_newer():
    assert sys.version_info >= (3, 12), f"Need Python 3.12+, got {sys.version}"
