"""Truth Social ingestor.

Polls one or more Truth Social accounts via truthbrush, normalizes each
post to a NormalizedEvent, and emits it via the shared Ingestor base class.
"""
from __future__ import annotations
import hashlib
import html
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from truthbrush import Api

from services.shared.ingestor_base import Ingestor
from services.shared.logging import configure_logging
from services.shared.models import NormalizedEvent


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split())


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class TruthSocialIngestor(Ingestor):
    source_name = "truth_social"

    def __init__(self, *, usernames: list[str], producer=None,
                 poll_interval_seconds: int | None = None) -> None:
        super().__init__(producer=producer, poll_interval_seconds=poll_interval_seconds)
        self.usernames = usernames
        self._api = Api()
        self._since_ids: dict[str, str | None] = {u: None for u in usernames}

    def _fetch_raw_items(self) -> list[dict[str, Any]]:
        raise NotImplementedError  # implemented in Task 3

    def _normalize_item(self, raw: dict[str, Any]) -> NormalizedEvent:
        post = raw["post"]
        username = raw["_username"]

        content_html = post.get("content") or ""
        reblog = post.get("reblog")
        is_reblog = reblog is not None

        # For reposts, the original content lives in reblog.content.
        if not content_html.strip() and reblog:
            content_html = reblog.get("content", "")

        if not content_html:
            raise ValueError(f"post has no content: id={post.get('id')}")

        created_at = post.get("created_at")
        if not created_at:
            raise ValueError(f"post has no created_at: id={post.get('id')}")

        post_id = str(post["id"])
        ts_source = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        body = _strip_html(content_html)
        headline = body[:280]
        url = post.get("url") or post.get("uri", "")
        event_id = _sha256_hex(f"truth_social|{post_id}|{created_at}")

        return NormalizedEvent(
            event_id=event_id,
            source=self.source_name,
            ts_source=ts_source,
            ts_ingested=datetime.now(timezone.utc),
            headline=headline,
            body=body,
            url=url,
            metadata={
                "post_id": post_id,
                "account": username,
                "is_reblog": is_reblog,
            },
        )
