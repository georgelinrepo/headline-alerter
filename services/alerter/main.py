"""Alerter service: consume events.scored, send WhatsApp via Twilio for high-scoring events.

Decision: should_fire(score, confidence) → has_been_alerted(event_id) → send → record.
Failures (Twilio errors, missing archive row, post-send PG failure) route to events.dlq
with typed stages. The alerter never crashes on a single bad message.
"""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from services.shared.db import connect
from services.shared.dlq import send_to_dlq
from services.shared.kafka_client import flush, make_consumer, make_producer, produce
from services.shared.logging import configure_logging, get_logger
from services.shared.models import ScoredEvent
from services.alerter.format import format_alert
from services.alerter.twilio_client import AlerterError, build_client, send_message


# ---- predicates -----------------------------------------------------------

def should_fire(score: int, confidence: float, *,
                threshold: int, min_confidence: float) -> bool:
    return score >= threshold and confidence >= min_confidence


def has_been_alerted(event_id: str) -> bool:
    """Returns True iff alert_history already has a row for this event_id."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM alert_history WHERE event_id = %s LIMIT 1",
                (event_id,),
            )
            return cur.fetchone() is not None


def has_alerted_on_headline_recently(headline: str, source: str,
                                      hours: int = 24) -> bool:
    """Returns True iff we've alerted on this headline from this source in the last N hours."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM alert_history ah
                JOIN events_archive ea ON ah.event_id = ea.id
                WHERE ea.headline = %s AND ea.source = %s
                  AND ah.ts_created > NOW() - INTERVAL '%s hours'
                LIMIT 1
                """,
                (headline, source, hours),
            )
            return cur.fetchone() is not None


def fetch_archive_context(event_id: str) -> tuple[str, str, datetime, str | None]:
    """Returns (headline, source, ts_source, url). Raises if row missing."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT headline, source, ts_source, url FROM events_archive WHERE id = %s",
                (event_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"events_archive row not found for event_id={event_id}")
    return row


def insert_alert_history(event_id: str, *, channel: str,
                         recipient: str, twilio_sid: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_history
                  (event_id, channel, recipient, twilio_sid, delivery_status)
                VALUES (%s, %s, %s, %s, 'queued')
                """,
                (event_id, channel, recipient, twilio_sid),
            )
        conn.commit()


def mark_alerted(event_id: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events_archive
                SET status='alerted', ts_alerted=NOW()
                WHERE id = %s
                """,
                (event_id,),
            )
            if cur.rowcount == 0:
                raise RuntimeError(f"events_archive row not found for event_id={event_id}")
        conn.commit()


# ---- per-message processing ----------------------------------------------

def process_one_alert(scored_dict: dict[str, Any], *, twilio_client, producer, log,
                      channel: str, recipient: str, from_number: str,
                      threshold: int, min_confidence: float) -> None:
    """Decide → idempotency → send → record. Routes failures to DLQ."""
    scored = ScoredEvent.from_dict(scored_dict)

    if not should_fire(scored.score, scored.confidence,
                       threshold=threshold, min_confidence=min_confidence):
        log.debug("below threshold; skip", event_id=scored.event_id,
                  score=scored.score, confidence=scored.confidence)
        return

    if has_been_alerted(scored.event_id):
        log.info("already alerted; skip", event_id=scored.event_id)
        return

    # Send path: archive read → format → Twilio.
    try:
        headline, source, ts_source, url = fetch_archive_context(scored.event_id)
        if has_alerted_on_headline_recently(headline, source):
            log.info("duplicate headline alerted recently; skip",
                     event_id=scored.event_id, headline=headline[:80])
            return
        body = format_alert(scored, headline=headline, source=source,
                            ts_source=ts_source, url=url)
        twilio_sid = send_message(twilio_client, channel=channel, to=recipient,
                                  from_number=from_number, body=body)
    except AlerterError as e:
        log.warning("alert send failed", event_id=scored.event_id,
                    stage=e.stage, error=str(e.original),
                    retry_count=e.retry_count)
        send_to_dlq(producer, stage=e.stage, service="alerter",
                    error=e.original or e, original_event=scored_dict,
                    retry_count=e.retry_count)
        flush(producer)
        return
    except Exception as e:
        log.error("alerter pre-send error", event_id=scored.event_id, error=str(e))
        send_to_dlq(producer, stage="alerter_unknown", service="alerter",
                    error=e, original_event=scored_dict, retry_count=0)
        flush(producer)
        return

    # Post-send writes: alert_history + events_archive + audit topic.
    try:
        insert_alert_history(scored.event_id, channel=channel,
                             recipient=recipient, twilio_sid=twilio_sid)
        mark_alerted(scored.event_id)
        produce(producer, "alerts.outgoing", key=scored.event_id, payload={
            "event_id": scored.event_id,
            "channel": channel,
            "recipient": recipient,
            "twilio_sid": twilio_sid,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        })
        flush(producer)
    except Exception as e:
        # Twilio already delivered — PG/Kafka writes failed.
        # DLQ with the twilio_sid embedded so an operator can manually reconcile.
        log.error("post-send write failed",
                  event_id=scored.event_id, error=str(e), twilio_sid=twilio_sid)
        send_to_dlq(producer, stage="alerter_unknown", service="alerter",
                    error=e,
                    original_event={**scored_dict, "_twilio_sid": twilio_sid},
                    retry_count=0)
        flush(producer)
        return

    log.info("alerted", event_id=scored.event_id, score=scored.score,
             direction=scored.direction, confidence=scored.confidence,
             twilio_sid=twilio_sid)


# ---- main loop -----------------------------------------------------------

def _consumer_group_id() -> str:
    return os.environ.get("ALERTER_GROUP_ID", "alerter-cg")


def main() -> int:
    configure_logging("alerter")
    log = get_logger()
    log.info("starting alerter")

    channel = os.environ.get("ALERT_CHANNEL", "whatsapp")
    recipient = os.environ.get("ALERT_RECIPIENT")
    if not recipient:
        raise RuntimeError("ALERT_RECIPIENT env var is required")
    from_number = os.environ.get("TWILIO_FROM")
    if not from_number:
        raise RuntimeError("TWILIO_FROM env var is required")
    threshold = int(os.environ.get("ALERT_THRESHOLD", "4"))
    min_confidence = float(os.environ.get("MIN_CONFIDENCE", "0.6"))

    log.info("alerter config",
             channel=channel, threshold=threshold,
             min_confidence=min_confidence, recipient=recipient[:14] + "...")

    twilio_client = build_client()
    producer = make_producer()
    consumer = make_consumer(_consumer_group_id(), ["events.scored"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error", error=str(msg.error()))
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error("payload decode failed", error=str(e))
                consumer.commit(message=msg, asynchronous=False)
                continue

            process_one_alert(
                payload,
                twilio_client=twilio_client,
                producer=producer,
                log=log,
                channel=channel,
                recipient=recipient,
                from_number=from_number,
                threshold=threshold,
                min_confidence=min_confidence,
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        flush(producer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
