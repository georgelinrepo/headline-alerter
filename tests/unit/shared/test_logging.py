import json
from services.shared.logging import configure_logging, get_logger


def test_logger_emits_json_with_required_fields(capsys):
    configure_logging(service_name="test_svc")
    log = get_logger()
    log.info("hello", event_id="x-1", value=42)
    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["event"] == "hello"
    assert obj["event_id"] == "x-1"
    assert obj["value"] == 42
    assert obj["service"] == "test_svc"
    assert obj["level"] == "info"
    assert "timestamp" in obj


def test_logger_warning_level(capsys):
    configure_logging(service_name="svc2")
    log = get_logger()
    log.warning("careful", code=99)
    line = capsys.readouterr().out.strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["level"] == "warning"
    assert obj["service"] == "svc2"
    assert obj["code"] == 99
