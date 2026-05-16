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


def test_build_system_prompt_no_context_returns_bare_prompt():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt()
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["text"] == SYSTEM_PROMPT
    assert blocks[0]["cache_control"] == {"type": "ephemeral"}


def test_build_system_prompt_with_context_prepends_xml_block():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt(macro_context="Fed holds at 3.5%.")
    assert len(blocks) == 1
    text = blocks[0]["text"]
    assert text.startswith("<macro_context>")
    assert "Fed holds at 3.5%." in text
    assert "</macro_context>" in text
    assert SYSTEM_PROMPT in text


def test_build_system_prompt_none_context_returns_bare_prompt():
    from services.shared.scorer_prompts import build_system_prompt, SYSTEM_PROMPT
    blocks = build_system_prompt(macro_context=None)
    assert blocks[0]["text"] == SYSTEM_PROMPT
