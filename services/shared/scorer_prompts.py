"""STUB — replaced by full implementation in Task 4."""
SYSTEM_PROMPT = "Score events for impact on US interest rates markets."
SCORE_EVENT_TOOL = {
    "name": "score_event",
    "description": "Score the rates-market relevance of an event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer"},
            "direction": {"type": "string"},
            "confidence": {"type": "number"},
            "reasoning": {"type": "string"},
        },
        "required": ["score", "direction", "confidence", "reasoning"],
    },
}
