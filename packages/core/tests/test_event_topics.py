"""CHG-0062 complete public-source snapshot contracts, including large snapshots."""
import copy
from datetime import timedelta

import pytest

from data_horizon.config import Settings
from data_horizon.contracts import utcnow
from data_horizon.event_topics import (
    AUXILIARY_EVENT_TYPES, EVENT_TYPES, MAX_AUXILIARY_RECORD_BYTES,
    SnapshotEvent, build_snapshot_events, canonical_bytes, reassemble_snapshot_events,
)

pytestmark = pytest.mark.case("F003-TC10")


def snapshot(payload=None, **kwargs):
    return build_snapshot_events("service-alerts", "synthetic", payload or {"alerts": [], "authoritative": True},
                                 source="fixture", retrieved_at=utcnow(), **kwargs)


@pytest.mark.parametrize("mode", ["live", "synthetic", "recorded"])
def test_closed_topic_registry_is_mode_scoped_and_has_no_generic_alias(mode):
    settings = Settings(mode=mode)
    assert tuple(settings.event_topics) == EVENT_TYPES
    assert settings.transit_topic == f"data-horizon.{mode}.transit-events.v1"
    assert len(set(settings.event_topics.values())) == 5
    assert not hasattr(settings, "topic")
    with pytest.raises(ValueError, match="Unknown"):
        settings.topic_for("user-notifications")


@pytest.mark.parametrize("kind", AUXILIARY_EVENT_TYPES)
def test_auxiliary_identity_is_deterministic_and_preserves_source_clocks(kind):
    retrieved = utcnow()
    observed = retrieved - timedelta(minutes=5)
    kwargs = {"source": "official-fixture", "retrieved_at": retrieved, "source_time": observed}
    records = build_snapshot_events(kind, "recorded", {"items": []}, **kwargs)
    assert records == build_snapshot_events(kind, "recorded", {"items": []}, **kwargs)
    event = SnapshotEvent.model_validate(records[0])
    assert event.source_time == observed and event.retrieved_at == retrieved
    assert event.completion == "all-parts-required" and event.part_count == 1
    assert reassemble_snapshot_events(records) == {"items": []}
    assert len(canonical_bytes(records[0])) <= MAX_AUXILIARY_RECORD_BYTES


def test_empty_authoritative_alert_snapshot_is_not_dropped():
    records = snapshot()
    assert len(records) == 1
    assert reassemble_snapshot_events(records) == {"alerts": [], "authoritative": True}


def test_large_unicode_snapshot_requires_all_valid_parts_and_reassembles_exactly():
    payload = {"alerts": [{"id": "long-notice", "details": "मराठी-தமிழ்-🚆" * 25000}], "authoritative": True}
    records = snapshot(payload)
    assert len(records) > 1 and all(len(canonical_bytes(r)) <= MAX_AUXILIARY_RECORD_BYTES for r in records)
    assert {r["snapshot_id"] for r in records} == {records[0]["snapshot_id"]}
    assert reassemble_snapshot_events(list(reversed(records))) == payload
    with pytest.raises(ValueError, match="Incomplete"):
        reassemble_snapshot_events(records[:-1])
    with pytest.raises(ValueError, match="duplicate"):
        reassemble_snapshot_events(records + [records[0]])
    another = snapshot({"alerts": [{"details": "different"}] * 15000})
    with pytest.raises(ValueError, match="Mixed"):
        reassemble_snapshot_events([records[0], *another[1:]])


@pytest.mark.parametrize("field,value", [
    ("event_type", "user-account"), ("mode", "other"), ("event_id", "0" * 64),
    ("snapshot_id", "0" * 64), ("source_time", "2026-01-01T12:00:00"),
    ("part_count", 0), ("part_index", 1), ("schema_version", 2),
])
def test_forged_envelope_identity_scope_or_clock_is_rejected(field, value):
    record = copy.deepcopy(snapshot()[0])
    record[field] = value
    with pytest.raises(ValueError):
        SnapshotEvent.model_validate(record)


def test_tampered_plain_snapshot_payload_is_rejected_before_receipt():
    record = snapshot()[0]
    record["data"]["alerts"] = [{"id": "forged"}]
    with pytest.raises(ValueError, match="checksum"):
        SnapshotEvent.model_validate(record)


def test_unknown_or_transit_wrapped_type_and_nonfinite_payload_are_rejected():
    for kind in ("private-inbox", "transit-events"):
        with pytest.raises(ValueError):
            build_snapshot_events(kind, "live", {}, source="test", retrieved_at=utcnow())
    with pytest.raises(ValueError):
        snapshot({"temperature_c": float("nan")})
