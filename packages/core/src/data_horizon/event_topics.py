"""Closed public-source Kafka domains and complete, bounded snapshot envelopes.

Auxiliary snapshots are inspection/replay mirrors. Consumers must wait for every
part and validate the complete checksum before replacing an authoritative view.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .contracts import timestamp

EVENT_TYPES = (
    "transit-events", "service-alerts", "vehicle-status", "weather-observations", "static-snapshots",
)
AUXILIARY_EVENT_TYPES = EVENT_TYPES[1:]
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
MAX_AUXILIARY_RECORD_BYTES = 256 * 1024
SNAPSHOT_FRAGMENT_BYTES = 128 * 1024
MAX_SNAPSHOT_PARTS = MAX_SNAPSHOT_BYTES // SNAPSHOT_FRAGMENT_BYTES


def canonical_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def event_topic(mode: str, event_type: str) -> str:
    if mode not in {"live", "synthetic", "recorded"} or event_type not in EVENT_TYPES:
        raise ValueError("Unknown Kafka event type or data mode")
    return f"data-horizon.{mode}.{event_type}.v1"


class SnapshotEvent(BaseModel):
    """One part of a source snapshot; no private account/inbox event domain exists."""
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    event_type: Literal["service-alerts", "vehicle-status", "weather-observations", "static-snapshots"]
    mode: Literal["live", "synthetic", "recorded"]
    source: str = Field(min_length=1, max_length=100)
    retrieved_at: datetime
    source_time: datetime | None = None
    snapshot_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    snapshot_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    part_index: int = Field(ge=0, lt=MAX_SNAPSHOT_PARTS)
    part_count: int = Field(ge=1, le=MAX_SNAPSHOT_PARTS)
    completion: Literal["all-parts-required"] = "all-parts-required"
    encoding: Literal["json", "base64-json"] = "json"
    data: dict | None = None
    fragment: str | None = Field(default=None, max_length=MAX_AUXILIARY_RECORD_BYTES)
    event_id: str = ""

    @field_validator("retrieved_at", "source_time", mode="before")
    @classmethod
    def aware_clock(cls, value):
        return timestamp(value) if value is not None else None

    @field_validator("source")
    @classmethod
    def source_not_blank(cls, value):
        if not value.strip():
            raise ValueError("Non-blank source required")
        return value.strip()

    @model_validator(mode="after")
    def valid_part_and_identity(self):
        if self.part_index >= self.part_count:
            raise ValueError("Snapshot part index exceeds declared count")
        if self.encoding == "json":
            if self.part_count != 1 or self.part_index != 0 or self.data is None or self.fragment is not None:
                raise ValueError("JSON snapshot must be one complete data part")
            if hashlib.sha256(canonical_bytes(self.data)).hexdigest() != self.snapshot_sha256:
                raise ValueError("Snapshot payload checksum mismatch")
        elif self.data is not None or not self.fragment:
            raise ValueError("Fragmented snapshot requires fragment bytes only")
        else:
            try:
                decoded = base64.b64decode(self.fragment, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise ValueError("Invalid snapshot fragment") from exc
            if not decoded or len(decoded) > SNAPSHOT_FRAGMENT_BYTES:
                raise ValueError("Snapshot fragment size exceeds bound")
        snapshot_context = {
            "event_type": self.event_type, "mode": self.mode, "source": self.source,
            "retrieved_at": self.retrieved_at.isoformat(),
            "source_time": self.source_time.isoformat() if self.source_time else None,
            "snapshot_sha256": self.snapshot_sha256,
        }
        if hashlib.sha256(canonical_bytes(snapshot_context)).hexdigest() != self.snapshot_id:
            raise ValueError("Snapshot identity mismatch")
        content = self.model_dump(mode="json", exclude={"event_id"})
        expected = hashlib.sha256(canonical_bytes(content)).hexdigest()
        if self.event_id and self.event_id != expected:
            raise ValueError("Snapshot event identity mismatch")
        self.event_id = expected
        if len(canonical_bytes(self.model_dump(mode="json"))) > MAX_AUXILIARY_RECORD_BYTES:
            raise ValueError("Snapshot record exceeds byte bound")
        return self


def build_snapshot_events(event_type: str, mode: str, payload: dict, *, source: str,
                          retrieved_at, source_time=None) -> list[dict]:
    event_topic(mode, event_type)
    if event_type not in AUXILIARY_EVENT_TYPES or not isinstance(payload, dict):
        raise ValueError("Auxiliary snapshot type and object payload required")
    raw = canonical_bytes(payload)
    if len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Auxiliary snapshot exceeds byte bound")
    retrieved = timestamp(retrieved_at)
    original = timestamp(source_time) if source_time is not None else None
    context = {
        "event_type": event_type, "mode": mode, "source": source.strip(),
        "retrieved_at": retrieved.isoformat(),
        "source_time": original.isoformat() if original else None,
        "snapshot_sha256": hashlib.sha256(raw).hexdigest(),
    }
    base = {**context, "snapshot_id": hashlib.sha256(canonical_bytes(context)).hexdigest()}
    # Leave headroom for envelope/identity fields before selecting plain JSON.
    if len(raw) <= MAX_AUXILIARY_RECORD_BYTES - 2048:
        return [SnapshotEvent(**base, data=payload, part_index=0, part_count=1).model_dump(mode="json")]
    fragments = [raw[start:start + SNAPSHOT_FRAGMENT_BYTES]
                 for start in range(0, len(raw), SNAPSHOT_FRAGMENT_BYTES)]
    return [SnapshotEvent(**base, encoding="base64-json", part_index=index, part_count=len(fragments),
                          fragment=base64.b64encode(part).decode("ascii")).model_dump(mode="json")
            for index, part in enumerate(fragments)]


def reassemble_snapshot_events(records: list[dict]) -> dict:
    if not records or len(records) > MAX_SNAPSHOT_PARTS:
        raise ValueError("Complete bounded snapshot parts required")
    events = [SnapshotEvent.model_validate(record) for record in records]
    first = events[0]
    if any((e.snapshot_id, e.part_count, e.encoding) !=
           (first.snapshot_id, first.part_count, first.encoding) for e in events):
        raise ValueError("Mixed snapshot identities or encodings")
    by_index = {e.part_index: e for e in events}
    if len(by_index) != len(events) or set(by_index) != set(range(first.part_count)):
        raise ValueError("Incomplete or duplicate snapshot parts")
    if first.encoding == "json":
        return first.data
    raw = b"".join(base64.b64decode(by_index[index].fragment, validate=True)
                   for index in range(first.part_count))
    if len(raw) > MAX_SNAPSHOT_BYTES or hashlib.sha256(raw).hexdigest() != first.snapshot_sha256:
        raise ValueError("Complete snapshot checksum or size mismatch")
    try:
        result = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Invalid complete snapshot JSON") from exc
    if not isinstance(result, dict) or canonical_bytes(result) != raw:
        raise ValueError("Snapshot payload must be a canonical JSON object")
    return result
