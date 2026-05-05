"""CNBC RSS ingestor.

Polls one or more CNBC RSS feed URLs, normalizes each <item> to a
NormalizedEvent, and emits it via the shared Ingestor base class.
"""
from __future__ import annotations
import hashlib
import os
import sys
from datetime import datetime, timezone
from typing import Any

import feedparser

from services.shared.ingestor_base import Ingestor
from services.shared.logging import configure_logging
from services.shared.models import NormalizedEvent


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _parsed_to_utc(struct_time) -> datetime:
    """feedparser gives published_parsed as a time.struct_time in UTC."""
    if struct_time is None:
        raise ValueError("missing pubDate")
    return datetime(*struct_time[:6], tzinfo=timezone.utc)


class CnbcIngestor(Ingestor):
    source_name = "cnbc_rss"

    def __init__(self, *, urls: list[str], producer=None,
                 poll_interval_seconds: int | None = None) -> None:
        if poll_interval_seconds is not None:
            self.poll_interval_seconds = poll_interval_seconds
        super().__init__(producer=producer)
        self.urls = urls

    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for url in self.urls:
            parsed = feedparser.parse(url)
            if parsed.bozo:
                self.log.warning("rss feed parse error", url=url,
                                 error=str(getattr(parsed, "bozo_exception", "")))
                continue
            for entry in parsed.entries:
                out.append({"entry": entry, "_feed_url": url})
        return out

    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        entry = raw["entry"]
        feed_url = raw.get("_feed_url", "")

        url = getattr(entry, "link", None) or (entry.get("link") if hasattr(entry, "get") else None)
        if not url:
            raise ValueError("entry has no link")
        title = getattr(entry, "title", None) or (entry.get("title") if hasattr(entry, "get") else None)
        if not title:
            raise ValueError("entry has no title")

        # feedparser parses pubDate to entry.published_parsed (struct_time, UTC).
        published_parsed = getattr(entry, "published_parsed", None) or (
            entry.get("published_parsed") if hasattr(entry, "get") else None
        )
        ts_source = _parsed_to_utc(published_parsed)

        body = (
            getattr(entry, "description", None)
            or (entry.get("description") if hasattr(entry, "get") else None)
            or getattr(entry, "summary", None)
        )
        raw_id = (
            getattr(entry, "id", None)
            or (entry.get("id") if hasattr(entry, "get") else None)
            or url
        )

        event_id = _sha256_hex(f"cnbc_rss|{url}|{ts_source.isoformat()}")
        return NormalizedEvent(
            event_id=event_id,
            source=self.source_name,
            ts_source=ts_source,
            ts_ingested=datetime.now(timezone.utc),
            headline=str(title),
            body=str(body) if body else None,
            url=url,
            metadata={"raw_id": str(raw_id), "feed_url": feed_url},
        )


def _urls_from_env() -> list[str]:
    raw = os.environ.get("CNBC_RSS_URLS", "").strip()
    if not raw:
        raise RuntimeError("CNBC_RSS_URLS env var is required")
    return [u.strip() for u in raw.split(",") if u.strip()]


def main() -> int:
    configure_logging("ingestor-cnbc")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    ing = CnbcIngestor(urls=_urls_from_env(), poll_interval_seconds=interval)
    ing.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
