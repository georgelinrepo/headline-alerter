"""Unit tests for the Twilio client wrapper."""
from unittest.mock import MagicMock

import pytest
from twilio.base.exceptions import TwilioRestException

from services.alerter.twilio_client import (
    AlerterError, send_message, _FakeTwilioClient, build_client,
)


def _ok_client(sid="SMfakesid"):
    """Mock client whose messages.create() returns a message with .sid."""
    msg = MagicMock()
    msg.sid = sid
    client = MagicMock()
    client.messages.create.return_value = msg
    return client


def _failing_client(exc):
    client = MagicMock()
    client.messages.create.side_effect = exc
    return client


def _twilio_err(status, code=None):
    return TwilioRestException(status=status, uri="/Messages", msg=str(status),
                               code=code, method="POST")


def test_success_returns_twilio_sid():
    client = _ok_client(sid="SM123abc")
    sid = send_message(client, channel="whatsapp",
                       to="whatsapp:+44...", from_number="whatsapp:+14155238886",
                       body="hello")
    assert sid == "SM123abc"


def test_429_retries_three_times_then_dlq(monkeypatch):
    sleeps = []
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep",
                        lambda s: sleeps.append(s))
    client = MagicMock()
    client.messages.create.side_effect = [_twilio_err(429)] * 4

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_throttle"
    assert exc_info.value.retry_count == 3
    assert sleeps == [1, 4, 16]


def test_5xx_retries_three_times_then_dlq(monkeypatch):
    sleeps = []
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep",
                        lambda s: sleeps.append(s))
    client = MagicMock()
    client.messages.create.side_effect = [_twilio_err(503)] * 4

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_5xx"
    assert exc_info.value.retry_count == 3
    assert sleeps == [1, 4, 16]


def test_unknown_4xx_routes_to_alerter_unknown():
    """A TwilioRestException with a 4xx status and unrecognized code falls through
    to alerter_unknown, not alerter_5xx (which would be a misleading label)."""
    client = MagicMock()
    # 63018 is "daily limit reached" — not in any of our specific code sets.
    client.messages.create.side_effect = _twilio_err(400, code=63018)

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_unknown"
    assert client.messages.create.call_count == 1  # no retry


def test_auth_error_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(401)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_auth"
    assert client.messages.create.call_count == 1


def test_invalid_recipient_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=21211)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient"


def test_recipient_not_opted_in_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=63007)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient_not_opted_in"


def test_whatsapp_template_required_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=63016)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_whatsapp_template"


def test_unsubscribed_recipient_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = _twilio_err(400, code=21610)
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_recipient_not_opted_in"


def test_timeout_retries_once_then_dlq(monkeypatch):
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    client = MagicMock()
    client.messages.create.side_effect = [TimeoutError("slow"), TimeoutError("slow")]

    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")

    assert exc_info.value.stage == "alerter_timeout"
    assert client.messages.create.call_count == 2


def test_unknown_exception_no_retry():
    client = MagicMock()
    client.messages.create.side_effect = ValueError("???")
    with pytest.raises(AlerterError) as exc_info:
        send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert exc_info.value.stage == "alerter_unknown"
    assert client.messages.create.call_count == 1


def test_recovery_after_one_throttle(monkeypatch):
    monkeypatch.setattr("services.alerter.twilio_client.time.sleep", lambda s: None)
    msg = MagicMock(); msg.sid = "SMok"
    client = MagicMock()
    client.messages.create.side_effect = [_twilio_err(429), msg]

    sid = send_message(client, channel="whatsapp", to="x", from_number="y", body="z")
    assert sid == "SMok"
    assert client.messages.create.call_count == 2


# ---- _FakeTwilioClient ----------------------------------------------------

def test_fake_client_default_returns_sid():
    fake = _FakeTwilioClient(fail_mode=None)
    msg = fake.messages.create(to="x", from_="y", body="z")
    assert msg.sid.startswith("SM")


def test_fake_client_throttle_mode_raises_429():
    fake = _FakeTwilioClient(fail_mode="throttle")
    with pytest.raises(TwilioRestException) as exc_info:
        fake.messages.create(to="x", from_="y", body="z")
    assert exc_info.value.status == 429


def test_fake_client_recipient_mode_raises_21211():
    fake = _FakeTwilioClient(fail_mode="recipient")
    with pytest.raises(TwilioRestException) as exc_info:
        fake.messages.create(to="x", from_="y", body="z")
    assert exc_info.value.code == 21211


def test_build_client_uses_fake_when_env_set(monkeypatch):
    monkeypatch.setenv("TWILIO_FAKE", "1")
    monkeypatch.setenv("TWILIO_FAIL_MODE", "throttle")
    client = build_client()
    assert isinstance(client, _FakeTwilioClient)
    assert client._fail_mode == "throttle"


def test_build_client_raises_when_creds_missing(monkeypatch):
    monkeypatch.delenv("TWILIO_FAKE", raising=False)
    monkeypatch.delenv("TWILIO_ACCOUNT_SID", raising=False)
    monkeypatch.delenv("TWILIO_AUTH_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="TWILIO_ACCOUNT_SID"):
        build_client()
