"""alerter_smoke.py — one-shot end-to-end test against the real Twilio API.

Sends a single hardcoded WhatsApp message to ALERT_RECIPIENT.
Cost: ~$0.005. Not run in CI.

Usage:
    # Bash: source env vars from .env and run
    set -a; source .env; set +a
    python tools/alerter_smoke.py

Requires: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM, ALERT_RECIPIENT in env.
"""
from __future__ import annotations
import os
import sys
from datetime import datetime, timezone

# Make `services.*` importable when running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.alerter.twilio_client import build_client, send_message
from services.shared.logging import configure_logging, get_logger


SMOKE_BODY = (
    "[smoke test 4/10 → neutral · conf 100%]\n"
    "headline-alerter · {ts}Z\n"
    "\n"
    "Phase 1b smoke test\n"
    "\n"
    "If you see this, your Twilio credentials work and the WhatsApp Sandbox "
    "opt-in succeeded. Reply anything to this message to keep the 24h session "
    "window open.\n"
)


def main() -> int:
    configure_logging("alerter-smoke")
    log = get_logger()

    recipient = os.environ.get("ALERT_RECIPIENT")
    from_number = os.environ.get("TWILIO_FROM")
    if not recipient or not from_number:
        log.error("ALERT_RECIPIENT and TWILIO_FROM env vars are required")
        return 1

    log.info("sending smoke message", recipient=recipient[:14] + "...",
             from_number=from_number)
    client = build_client()
    body = SMOKE_BODY.format(ts=datetime.now(timezone.utc).strftime("%H:%M"))
    sid = send_message(client, channel="whatsapp",
                       to=recipient, from_number=from_number, body=body)

    print(f"OK — Phase 1b smoke test passed (twilio_sid: {sid})")
    print("Check your phone — the message should arrive within 5 seconds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
