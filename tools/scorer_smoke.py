"""scorer_smoke.py — one-shot end-to-end test against the real Anthropic API.

Fetches one CNBC headline, normalizes it, and asks Claude Haiku 4.5 to score it.
Prints the result. Costs ~$0.0014. Not run in CI.

Usage:
    # Bash: load key from .env and run
    export ANTHROPIC_API_KEY=$(grep '^ANTHROPIC_API_KEY=' .env | cut -d= -f2)
    python tools/scorer_smoke.py

Requires: ANTHROPIC_API_KEY in env, CNBC_RSS_URLS optional (defaults to one feed).
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
import feedparser

from services.ingestors.cnbc_rss.main import CnbcIngestor
from services.shared.anthropic_client import score_event
from services.shared.logging import configure_logging, get_logger


DEFAULT_FEED = (
    "https://search.cnbc.com/rs/search/combinedcms/view.xml"
    "?partnerId=wrss01&id=10000664"
)


def main() -> int:
    configure_logging("scorer-smoke")
    log = get_logger()

    feed_url = os.environ.get("CNBC_RSS_URLS", DEFAULT_FEED).split(",")[0].strip()
    log.info("fetching", url=feed_url)
    parsed = feedparser.parse(feed_url)
    if parsed.bozo or not parsed.entries:
        log.error("feed unusable", error=str(getattr(parsed, "bozo_exception", "no entries")))
        return 1

    # Use the same _normalize_item the production ingestor uses.
    ing = CnbcIngestor(urls=[feed_url], producer=type("P", (), {"produce": lambda *a, **k: None,
                                                                 "flush": lambda *a, **k: None})())
    raw = {"entry": parsed.entries[0], "_feed_url": feed_url}
    event = ing._normalize_item(raw)
    log.info("event normalized", event_id=event.event_id, headline=event.headline[:80])

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY env var is required")
        return 1
    client = anthropic.Anthropic(api_key=api_key)

    log.info("scoring (real Anthropic call)")
    scored = score_event(client, normalized_event=event,
                         model=os.environ.get("SCORER_MODEL", "claude-haiku-4-5"),
                         timeout_seconds=30)

    print(
        f"OK — Phase 1a smoke test passed (event scored: "
        f"{scored.score}/{scored.direction}/{scored.confidence:.2f})"
    )
    print(f"Headline: {event.headline}")
    print(f"Reasoning: {scored.reasoning}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
