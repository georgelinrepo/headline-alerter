"""Dead-letter-queue helper. Every service uses the same envelope format.

Envelope shape:
    {
      "stage":          "<stage tag, e.g. scorer_throttle>",
      "service":        "<service name, e.g. scorer>",
      "ts_dlq":         "<ISO-8601 UTC timestamp>",
      "error":          "<ExceptionClass: message>",
      "retry_count":    <int>,
      "original_event": <dict — NormalizedEvent or raw RSS item>,
    }
"""
import json
from datetime import datetime, timezone
from typing import Any
from confluent_kafka import Producer


def build_envelope(
    *,
    stage: str,
    service: str,
    error: BaseException,
    original_event: dict[str, Any] | None,
    retry_count: int = 0,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "service": service,
        "ts_dlq": datetime.now(timezone.utc).isoformat(),
        "error": f"{type(error).__name__}: {error}",
        "retry_count": retry_count,
        "original_event": original_event or {},
    }


def send_to_dlq(
    producer: Producer,
    *,
    stage: str,
    service: str,
    error: BaseException,
    original_event: dict[str, Any] | None,
    retry_count: int = 0,
) -> None:
    envelope = build_envelope(
        stage=stage,
        service=service,
        error=error,
        original_event=original_event,
        retry_count=retry_count,
    )
    key = (original_event or {}).get("event_id", "unknown")
    producer.produce(
        topic="events.dlq",
        key=str(key).encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
    )
