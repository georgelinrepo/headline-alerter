"""Build a WhatsApp/SMS alert message from a ScoredEvent + archive context.

Pure functions only — no I/O. The alerter's main loop calls format_alert()
just before invoking the Twilio client.
"""
from __future__ import annotations
from datetime import datetime

from services.shared.models import ScoredEvent


_DIRECTION_GLYPH = {
    "rates_higher": "↑",
    "rates_lower": "↓",
    "neutral": "→",
    "unclear": "?",
}


def format_alert(
    scored: ScoredEvent,
    *,
    headline: str,
    source: str,
    ts_source: datetime,
    url: str | None,
) -> str:
    """Build the alert message body. Plain text; WhatsApp auto-linkifies the URL."""
    glyph = _DIRECTION_GLYPH.get(scored.direction, "?")
    conf_pct = int(round(scored.confidence * 100))
    ts_short = ts_source.strftime("%H:%MZ")

    lines = [
        f"[{scored.score}/10 {glyph} {scored.direction} · conf {conf_pct}%]",
        f"{source} · {ts_short}",
        "",
        headline,
        "",
        scored.reasoning,
    ]
    if url:
        lines.extend(["", url])
    return "\n".join(lines)
