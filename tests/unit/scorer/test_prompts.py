"""Sanity tests for the system prompt + forced-tool schema."""
from services.shared.scorer_prompts import SYSTEM_PROMPT, SCORE_EVENT_TOOL


def test_system_prompt_mentions_rates_terminology():
    text = SYSTEM_PROMPT.lower()
    for needed in ["sofr", "treasury", "fomc", "rates_higher", "rates_lower"]:
        assert needed in text, f"system prompt missing '{needed}'"


def test_system_prompt_includes_full_rubric():
    """Every score 0-10 must be explained somewhere in the rubric."""
    for n in range(0, 11):
        assert str(n) in SYSTEM_PROMPT, f"rubric missing score {n}"


def test_tool_schema_required_fields():
    schema = SCORE_EVENT_TOOL["input_schema"]
    assert schema["type"] == "object"
    assert set(schema["required"]) == {"score", "direction", "confidence", "reasoning"}


def test_tool_schema_score_bounds():
    score = SCORE_EVENT_TOOL["input_schema"]["properties"]["score"]
    assert score["type"] == "integer"
    assert score["minimum"] == 0
    assert score["maximum"] == 10


def test_tool_schema_direction_enum():
    direction = SCORE_EVENT_TOOL["input_schema"]["properties"]["direction"]
    assert set(direction["enum"]) == {"rates_higher", "rates_lower", "neutral", "unclear"}


def test_tool_schema_confidence_bounds():
    confidence = SCORE_EVENT_TOOL["input_schema"]["properties"]["confidence"]
    assert confidence["type"] == "number"
    assert confidence["minimum"] == 0
    assert confidence["maximum"] == 1


def test_tool_name_is_stable():
    assert SCORE_EVENT_TOOL["name"] == "score_event"
