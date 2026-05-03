"""Event dataclasses shared between ingestor, scorer, alerter, dashboard."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class NormalizedEvent:
    """A news/social event after normalization, before scoring."""
    event_id: str
    source: str
    ts_source: datetime
    ts_ingested: datetime
    headline: str
    body: str | None = None
    url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "ts_source": self.ts_source.isoformat(),
            "ts_ingested": self.ts_ingested.isoformat(),
            "headline": self.headline,
            "body": self.body,
            "url": self.url,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NormalizedEvent:
        return cls(
            event_id=d["event_id"],
            source=d["source"],
            ts_source=datetime.fromisoformat(d["ts_source"]),
            ts_ingested=datetime.fromisoformat(d["ts_ingested"]),
            headline=d["headline"],
            body=d.get("body"),
            url=d.get("url"),
            metadata=d.get("metadata") or {},
        )


@dataclass
class ScoredEvent:
    """The output of the scorer service. Joined to NormalizedEvent by event_id."""
    event_id: str
    score: int
    direction: str
    confidence: float
    reasoning: str
    model: str
    scored_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "score": self.score,
            "direction": self.direction,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "model": self.model,
            "scored_at": self.scored_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ScoredEvent:
        return cls(
            event_id=d["event_id"],
            score=int(d["score"]),
            direction=d["direction"],
            confidence=float(d["confidence"]),
            reasoning=d["reasoning"],
            model=d["model"],
            scored_at=datetime.fromisoformat(d["scored_at"]),
        )
