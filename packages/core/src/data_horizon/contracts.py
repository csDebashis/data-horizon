"""Versioned event contracts and NYC service-time interpretation."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(UTC)


def timestamp(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC)
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timezone-aware timestamp required")
    return result.astimezone(UTC)


def service_time(service_date: str | date, value: str, *, fold: int | None = None) -> datetime:
    """Interpret >24h GTFS times. Require a choice for ambiguous local wall times."""
    day = date.fromisoformat(service_date) if isinstance(service_date, str) else service_date
    h, m, s = map(int, value.split(":"))
    if h < 0 or h > 71 or m not in range(60) or s not in range(60):
        raise ValueError("Invalid GTFS service time")
    naive = datetime.combine(day, datetime.min.time()) + timedelta(hours=h, minutes=m, seconds=s)
    zone = ZoneInfo("America/New_York")
    possibilities = []
    for f in (0, 1):
        candidate = naive.replace(tzinfo=zone, fold=f)
        if candidate.astimezone(UTC).astimezone(zone).replace(tzinfo=None) == naive:
            possibilities.append(candidate)
    if not possibilities:
        raise ValueError("Nonexistent local time during daylight-saving transition")
    if len({p.utcoffset() for p in possibilities}) > 1 and fold is None:
        raise ValueError("Ambiguous local time requires fold")
    return naive.replace(tzinfo=zone, fold=fold or 0).astimezone(UTC)


class TransitEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    event_id: str = Field(default="", max_length=128)
    source: str = Field(min_length=1, max_length=64)
    city: str = "nyc"
    route_id: str = Field(min_length=1, max_length=32)
    direction_id: str = Field(default="0", max_length=8)
    trip_id: str = Field(min_length=1, max_length=200)
    stop_id: str = Field(min_length=1, max_length=100)
    event_time: datetime
    ingested_at: datetime = Field(default_factory=utcnow)
    scheduled_time: datetime | None = None
    observed_time: datetime | None = None
    predicted_time: datetime | None = None
    latitude: float | None = Field(default=None, ge=-90, le=90, allow_inf_nan=False)
    longitude: float | None = Field(default=None, ge=-180, le=180, allow_inf_nan=False)
    time_basis: Literal["observed", "predicted", "inferred", "unavailable"]
    static_version: str | None = None
    replay_run_id: str | None = None
    match_confidence: Literal["exact", "ambiguous", "unmatched", "tentative"] = "unmatched"
    service_date: str | None = None
    cancelled: bool = False
    scheduled_headway_seconds: float | None = Field(default=None, ge=0)

    @field_validator("source", "city", "route_id", "trip_id", "stop_id", "direction_id", mode="before")
    @classmethod
    def nonblank_identifiers(cls, value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Non-blank identifier required")
        return value.strip()

    @field_validator(
        "event_time", "ingested_at", "scheduled_time", "observed_time", "predicted_time", mode="before"
    )
    @classmethod
    def aware_time(cls, v):
        return timestamp(v) if v is not None else None

    @model_validator(mode="after")
    def validate_basis_and_identity(self):
        if self.time_basis == "observed" and (self.observed_time is None or self.match_confidence != "exact"):
            raise ValueError("Actual delay requires an observed time and exact schedule match")
        if self.time_basis == "predicted" and self.predicted_time is None:
            raise ValueError("Predicted basis requires predicted_time")
        if not self.event_id:
            content = self.model_dump(mode="json", exclude={"event_id", "ingested_at", "replay_run_id"})
            self.event_id = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
        return self
