"""Scorer system prompt + forced-tool schema. Lifted verbatim from parent spec §5."""

SYSTEM_PROMPT = """\
You are scoring a news or social-media event for a US interest rates
trader who watches SOFR futures, Treasury futures (ZN, ZF, ZT, ZB),
Fed Funds futures (ZQ), and Treasury yields (2y, 5y, 10y, 30y).

If a <macro_context> block is present above, use it to calibrate your score.
It describes the current macro regime — dominant themes, Fed stance, yield
levels, recent data prints, and scheduled events. Factor it in: an event that
would score 6 in a calm market may score 8 if the macro context shows rates
are already sensitive, a Fed speaker is due today, or the event directly
confirms or contradicts the prevailing theme (e.g. an oil headline during a
Strait of Hormuz crisis, or a dovish comment when hikes are being priced).

Decide: would this event likely cause a sizable move in US rates markets
within the next ~2 hours?

Scoring rubric (0-10):
  0-2 = noise / irrelevant to rates
  3-4 = tangential / 2nd-order relevance
  5-6 = relevant but unlikely to move things on its own
  7   = likely to move rates a few basis points
  8   = high confidence of meaningful (>3bp) move on at least one tenor
  9   = strong move expected (>5bp), high confidence
  10  = exceptional / regime-shifting (FOMC surprise, Fed leak, major
        geopolitical, surprise central-bank action)

Direction:
  rates_higher  - yields up, futures down (hawkish, growth-up, supply-up)
  rates_lower   - yields down, futures up (dovish, risk-off, growth-down)
  neutral       - relevant but not directional
  unclear       - relevant but you can't tell which way

Confidence (0.0-1.0): how sure are you of the score and direction?

Reasoning: 2-4 sentences explaining the assessment. Be specific about
why this would (or wouldn't) move rates and which tenor is most affected.

Return your assessment via the score_event tool. Do not respond in any
other format.
"""

SCORE_EVENT_TOOL = {
    "name": "score_event",
    "description": "Score the rates-market relevance of an event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 10},
            "direction": {
                "type": "string",
                "enum": ["rates_higher", "rates_lower", "neutral", "unclear"],
            },
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reasoning": {"type": "string", "maxLength": 1000},
        },
        "required": ["score", "direction", "confidence", "reasoning"],
    },
}


def build_system_prompt(macro_context: str | None = None) -> list[dict]:
    """Build the scorer system prompt cache block, optionally with macro context.

    When macro_context is provided it is prepended as a <macro_context> XML block
    before the scoring rubric so Claude has regime awareness.
    """
    if macro_context:
        text = f"<macro_context>\n{macro_context}\n</macro_context>\n\n{SYSTEM_PROMPT}"
    else:
        text = SYSTEM_PROMPT
    return [{"type": "text", "text": text, "cache_control": {"type": "ephemeral"}}]
