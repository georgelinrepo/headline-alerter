"""Nightly macro context builder.

Calls Claude Sonnet with Anthropic web search to synthesise a macro summary
that is injected into the scorer system prompt.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any
import zoneinfo

_ET = zoneinfo.ZoneInfo("America/New_York")

CONTEXT_SEARCH_PROMPT = """\
Today is {date}. You are building a daily macro context summary for a US \
interest rates trader who watches SOFR futures, Treasury futures, and yields.

Search for information on these topics IN ORDER — start broad to catch regime \
events, then fill in specifics:

1. What are the dominant macro themes driving US interest rates right now? \
(Search this first — it catches wars, crises, central-bank transitions that \
narrow searches would miss.)
2. What is the current Federal Reserve policy stance, rate level, and who is \
the Chair?
3. What are current US Treasury yield levels for the 2y, 10y, and 30y?
4. What were the most recent major US economic data prints (CPI, NFP, PCE) \
and how did they compare to consensus?
5. What major economic events or Fed speakers are scheduled this week?

Synthesise your findings into this exact structure:

**US Macro Context — {date}**

**Dominant Themes:** [regime events, wars, crises, structural shifts driving rates]
**Fed Stance:** [rate level, direction, chair, recent dissents, next meeting date]
**Rates:** [2y, 10y, 30y levels, recent bp moves, curve shape]
**Recent Data:** [CPI, NFP, PCE prints with surprises vs consensus]
**This Week:** [scheduled events, Fed speakers, major auctions]

Be specific with numbers. This context will be injected into a scoring prompt \
for a live rates trading system.
"""


def _seconds_until_midnight_et(now: datetime | None = None) -> float:
    """Return seconds from now until next midnight ET."""
    if now is None:
        now = datetime.now(_ET)
    else:
        now = now.astimezone(_ET)
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return (next_midnight - now).total_seconds()


def build_macro_context(client: Any, model: str, today: str) -> str:
    """Call Anthropic Sonnet with web search to synthesise a macro summary.

    Raises ValueError if the response contains no text blocks.
    """
    response = client.messages.create(
        model=model,
        max_tokens=1500,
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}],
        messages=[{
            "role": "user",
            "content": CONTEXT_SEARCH_PROMPT.format(date=today),
        }],
    )
    texts = [
        b.text for b in response.content
        if hasattr(b, "text") and b.text and b.text.strip()
    ]
    if not texts:
        raise ValueError("context builder got no text from Anthropic response")
    return "\n\n".join(texts).strip()
