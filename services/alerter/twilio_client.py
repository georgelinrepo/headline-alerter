"""Twilio client wrapper.

Public surface:
    send_message(client, *, channel, to, from_number, body) -> twilio_sid
    build_client() -> twilio.rest.Client | _FakeTwilioClient
    AlerterError(stage, original, retry_count)

Handles retry/backoff for transient failures, maps Twilio error codes to
typed `stage` strings used for DLQ routing.
"""
from __future__ import annotations
import os
import time
from typing import Any

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException


_BACKOFF_DELAYS_SECONDS = [1, 4, 16]

# Twilio error codes (verified against twilio.com/docs/api/errors).
_CODE_INVALID_RECIPIENT = {21211, 21408}            # bad To number
_CODE_NOT_OPTED_IN = {63007, 21610}                 # WhatsApp / SMS opt-in required
_CODE_TEMPLATE_REQUIRED = {63016}                   # WhatsApp 24h window expired


class AlerterError(Exception):
    """Raised when the Twilio call fails terminally. `stage` drives DLQ routing."""

    def __init__(self, stage: str, original: BaseException | None = None,
                 retry_count: int = 0) -> None:
        self.stage = stage
        self.original = original
        self.retry_count = retry_count
        super().__init__(
            f"{stage}: {type(original).__name__ if original else ''}: {original}"
        )


def send_message(client, *, channel: str, to: str, from_number: str, body: str) -> str:
    """Send a WhatsApp/SMS message via Twilio. Returns the message SID on success.

    Raises AlerterError(stage=...) on terminal failure (after retries).
    """
    transient_attempt = 0
    timeout_attempt = 0

    while True:
        try:
            msg = client.messages.create(to=to, from_=from_number, body=body)
            return msg.sid
        except TwilioRestException as e:
            status = getattr(e, "status", 0) or 0
            code = getattr(e, "code", 0) or 0

            if status == 429:
                if transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                    time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                    transient_attempt += 1
                    continue
                raise AlerterError("alerter_throttle", e, retry_count=transient_attempt)

            if status in (401, 403):
                raise AlerterError("alerter_auth", e, retry_count=0)

            if code in _CODE_NOT_OPTED_IN:
                raise AlerterError("alerter_recipient_not_opted_in", e, retry_count=0)

            if code in _CODE_TEMPLATE_REQUIRED:
                raise AlerterError("alerter_whatsapp_template", e, retry_count=0)

            if code in _CODE_INVALID_RECIPIENT:
                raise AlerterError("alerter_recipient", e, retry_count=0)

            if 500 <= status < 600:
                if transient_attempt < len(_BACKOFF_DELAYS_SECONDS):
                    time.sleep(_BACKOFF_DELAYS_SECONDS[transient_attempt])
                    transient_attempt += 1
                    continue
                raise AlerterError("alerter_5xx", e, retry_count=transient_attempt)
            # Non-5xx Twilio error that didn't match any specific code → unknown
            raise AlerterError("alerter_unknown", e, retry_count=0)

        except (TimeoutError, OSError) as e:
            if timeout_attempt < 1:
                timeout_attempt += 1
                continue
            raise AlerterError("alerter_timeout", e, retry_count=timeout_attempt)

        except Exception as e:
            raise AlerterError("alerter_unknown", e, retry_count=0)


# ---- Integration-test seam ------------------------------------------------

class _FakeTwilioClient:
    """Stand-in for twilio.rest.Client used by integration tests.

    Activated by TWILIO_FAKE=1. TWILIO_FAIL_MODE controls behavior:
    - unset / 'none': returns a fake Message with sid='SM<...>fake'
    - 'throttle':     raises TwilioRestException(status=429)
    - 'recipient':    raises TwilioRestException(status=400, code=21211)
    - 'auth':         raises TwilioRestException(status=401)
    """

    def __init__(self, fail_mode: str | None = None):
        self._fail_mode = fail_mode or "none"
        self.messages = self  # so .messages.create works

    def create(self, **kwargs):
        if self._fail_mode == "throttle":
            raise TwilioRestException(status=429, uri="/Messages",
                                      msg="429", method="POST")
        if self._fail_mode == "recipient":
            raise TwilioRestException(status=400, uri="/Messages",
                                      msg="invalid", code=21211, method="POST")
        if self._fail_mode == "auth":
            raise TwilioRestException(status=401, uri="/Messages",
                                      msg="auth", method="POST")
        # Success: return a Message-like object.
        class _Msg:
            sid = "SM" + "0" * 32  # 34-char SID matching real Twilio format
        return _Msg()


def build_client():
    if os.environ.get("TWILIO_FAKE"):
        return _FakeTwilioClient(fail_mode=os.environ.get("TWILIO_FAIL_MODE"))

    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    if not sid or not token:
        raise RuntimeError("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN env vars are required")
    return Client(sid, token)
