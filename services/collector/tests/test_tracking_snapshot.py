"""Independent vehicle-status snapshots, without changing canonical events."""

import hashlib
import json

from google.transit import gtfs_realtime_pb2 as gtfs
import pytest

from data_horizon import collector
from data_horizon.config import Settings
from data_horizon.contracts import utcnow
from data_horizon.store import Store

pytestmark = [pytest.mark.case("F002-TC09"), pytest.mark.evidence("synthetic", "unit")]


def message(now=None):
    now = now or utcnow()
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(now.timestamp())
    return feed


def vehicle(feed, *, trip="T1", route="1", date="20261115", stop="A", status=1, sequence=3):
    entity = feed.entity.add()
    entity.id = f"vehicle-{len(feed.entity)}"
    row = entity.vehicle
    row.trip.route_id = route
    row.trip.trip_id = trip
    row.trip.start_date = date
    row.stop_id = stop
    row.timestamp = feed.header.timestamp - 600
    if status is not None:
        row.current_status = status
    if sequence is not None:
        row.current_stop_sequence = sequence
    return entity


def decode(feed, settings=None):
    return collector.decode_tracking_snapshot(
        feed.SerializeToString(), settings or Settings(), retrieved_at=utcnow()
    )


def test_status_presence_and_independent_clocks():
    feed = message()
    vehicle(feed)
    vehicle(feed, trip="T2", status=None)
    vehicle(feed, trip="T3", status=0, sequence=None)
    snapshot = decode(feed)
    rows = {row["trip_id"]: row for row in snapshot["vehicles"]}
    assert rows["T1"]["status"] == "STOPPED_AT" and rows["T1"]["status_explicit"] is True
    assert rows["T2"]["status"] == "UNSPECIFIED" and rows["T2"]["status_explicit"] is False
    assert rows["T3"]["status"] == "UNKNOWN"
    assert rows["T1"]["last_movement_at"] != snapshot["feed_timestamp"]
    assert rows["T1"]["service_date"] == "2026-11-15"
    assert rows["T1"]["direction_id"] is None
    assert all("latitude" not in row and "longitude" not in row for row in rows.values())
    assert snapshot["snapshot_id"] == hashlib.sha256(feed.SerializeToString()).hexdigest()


def test_invalid_deleted_cancelled_and_trip_update_only_are_excluded():
    feed = message()
    vehicle(feed, trip="good")
    vehicle(feed, trip="other", route="7")
    vehicle(feed, trip="invalid date", date="20260230")
    vehicle(feed, trip="bad-stop", stop="<script>")
    vehicle(feed, trip="deleted").is_deleted = True
    vehicle(feed, trip="cancelled").vehicle.trip.schedule_relationship = gtfs.TripDescriptor.CANCELED
    entity = feed.entity.add()
    entity.id = "trip-only"
    entity.trip_update.trip.route_id = "1"
    entity.trip_update.trip.trip_id = "future-trip"
    row = entity.trip_update.stop_time_update.add()
    row.stop_id = "A"
    row.arrival.time = feed.header.timestamp + 300
    result = decode(feed)
    assert [row["trip_id"] for row in result["vehicles"]] == ["good"]
    assert result["quality"]["excluded_route"] == 1
    assert result["quality"]["rejected"] == 2
    assert result["quality"]["deleted"] == result["quality"]["cancelled"] == 1


def test_duplicate_identity_is_stable_across_order_and_dates_remain_distinct():
    feed = message()
    vehicle(feed, stop="A")
    vehicle(feed, stop="B")
    vehicle(feed, date="20261116")
    vehicle(feed, route="2")
    first = decode(feed)
    reordered = message()
    for item in reversed(feed.entity):
        reordered.entity.add().CopyFrom(item)
    second = decode(reordered)
    assert first["vehicles"] == second["vehicles"]
    assert len(first["vehicles"]) == 3 and len({v["id"] for v in first["vehicles"]}) == 3
    assert first["quality"]["duplicates"] == 1


def test_snapshot_membership_is_bounded_and_not_trip_forecasts():
    feed = message()
    for i in range(1003):
        vehicle(feed, trip=f"T{i}")
    result = decode(feed)
    assert len(result["vehicles"]) == 1000 and result["truncated"] is True
    assert result["quality"]["reported_by_route"] == {"1": 1003}
    assert result["quality"]["omitted_by_bound"] == 3
    assert decode(message())["vehicles"] == []


@pytest.mark.parametrize("raw", [b"", b"not protobuf", b"x" * (32 * 1024 * 1024 + 1)])
def test_invalid_or_oversized_source_never_publishes_complete_snapshot(raw):
    with pytest.raises(ValueError):
        collector.decode_tracking_snapshot(raw, Settings())


def test_differential_feed_cannot_clear_full_membership():
    feed = message()
    feed.header.incrementality = gtfs.FeedHeader.DIFFERENTIAL
    with pytest.raises(ValueError, match="full"):
        decode(feed)


def prepare_collection(store, monkeypatch, raw, *, fail_transit=False, fail_alerts=False):
    monkeypatch.setattr(
        collector,
        "archive_response",
        lambda *a, **kw: {"sha256": hashlib.sha256(raw).hexdigest(), "retrieved_at": utcnow().isoformat()},
    )
    monkeypatch.setattr(collector, "publish", lambda *a, **kw: None)

    def fetch(url, **kwargs):
        if url == store.settings.realtime_url:
            if fail_transit:
                raise OSError("private-source-detail")
            return raw
        if url == store.settings.alerts_url:
            if fail_alerts:
                raise OSError("private-alert-detail")
            return message().SerializeToString()
        return json.dumps(
            {"properties": {"timestamp": utcnow().isoformat(), "temperature": {"value": 8}}}
        ).encode()

    monkeypatch.setattr(collector, "fetch", fetch)


def test_atomic_complete_replacement_empty_clear_and_transit_only_degradation(isolated_data_dir, monkeypatch):
    store = Store()
    feed = message()
    vehicle(feed)
    raw = feed.SerializeToString()
    prepare_collection(store, monkeypatch, raw, fail_alerts=True)
    collector.collect_once(store, store.settings)
    original = store.get_metadata("tracking_snapshot")
    assert original["collection_status"] == "ok" and len(original["vehicles"]) == 1
    prepare_collection(store, monkeypatch, raw, fail_transit=True)
    with pytest.raises(OSError):
        collector.collect_once(store, store.settings)
    degraded = store.get_metadata("tracking_snapshot")
    assert degraded["collection_status"] == "degraded"
    assert {k: v for k, v in degraded.items() if k not in {"collection_status", "reason"}} == {
        k: v for k, v in original.items() if k != "collection_status"
    }
    assert "private" not in degraded["reason"]
    prepare_collection(store, monkeypatch, message().SerializeToString())
    collector.collect_once(store, store.settings)
    assert store.get_metadata("tracking_snapshot")["vehicles"] == []
    assert store.get_metadata("tracking_snapshot")["collection_status"] == "ok"


def test_tracking_decode_does_not_change_canonical_transit_events():
    feed = message()
    vehicle(feed)
    raw = feed.SerializeToString()
    before = collector.decode_feed(raw, Settings())
    collector.decode_tracking_snapshot(raw, Settings())
    after = collector.decode_feed(raw, Settings())
    # Normalisation ingested_at is expected to advance between calls.
    for batch in (before[0], after[0]):
        for row in batch:
            row.pop("ingested_at")
    assert before == after


def test_unknown_status_enum_is_rejected_instead_of_becoming_unspecified():
    feed = message()
    entity = vehicle(feed)
    entity.vehicle.ClearField("current_status")
    # Protobuf2 retains an unknown enum value as an unknown field. It must not
    # become a missing/UNSPECIFIED status merely because the binding is older.
    encoded = entity.vehicle.SerializeToString() + b"\x20\xe7\x07"
    entity.vehicle.ParseFromString(encoded)
    result = decode(feed)
    assert result["vehicles"] == [] and result["quality"]["rejected"] == 1


def test_unrelated_extension_does_not_discard_valid_vehicle_status():
    feed = message()
    entity = vehicle(feed)
    # Unknown field1000 is unrelated to current_status(field4).
    entity.vehicle.ParseFromString(entity.vehicle.SerializeToString() + b"\xc0\x3e\x01")
    assert decode(feed)["vehicles"][0]["status"] == "STOPPED_AT"


@pytest.mark.parametrize("version", ["", "not-gtfs", "999.0"])
def test_unsupported_feed_version_cannot_establish_a_current_full_snapshot(version):
    feed = message()
    feed.header.gtfs_realtime_version = version
    with pytest.raises(ValueError, match="tracking feed"):
        decode(feed)


def test_explicit_cancelled_trip_update_removes_matching_vehicle_membership():
    feed = message()
    vehicle(feed)
    update = feed.entity.add()
    update.id = "cancel-trip"
    update.trip_update.trip.route_id = "1"
    update.trip_update.trip.trip_id = "T1"
    update.trip_update.trip.start_date = "20261115"
    update.trip_update.trip.schedule_relationship = gtfs.TripDescriptor.CANCELED
    result = decode(feed)
    assert result["vehicles"] == [] and result["quality"]["cancelled"] == 1


def test_kafka_publication_failure_does_not_mark_a_fresh_vehicle_source_failed(
    isolated_data_dir, monkeypatch
):
    store = Store()
    feed = message()
    vehicle(feed)
    prepare_collection(store, monkeypatch, feed.SerializeToString())

    def unavailable(*args, **kwargs):
        raise ConnectionError("unavailable broker")

    monkeypatch.setattr(collector, "publish", unavailable)
    with pytest.raises(ConnectionError):
        collector.collect_once(store, store.settings)
    result = store.get_metadata("tracking_snapshot")
    assert result["collection_status"] == "ok" and len(result["vehicles"]) == 1


def test_unknown_incrementality_cannot_masquerade_as_a_complete_snapshot():
    feed = message()
    vehicle(feed)
    # Header field2 is incrementality. Protobuf2 exposes default FULL_DATASET
    # while retaining unsupported enum999 in its unknown wire fields.
    feed.header.ParseFromString(feed.header.SerializeToString() + b"\x10\xe7\x07")
    assert feed.header.incrementality == gtfs.FeedHeader.FULL_DATASET
    with pytest.raises(ValueError, match="full source snapshot"):
        decode(feed)


@pytest.mark.parametrize("explicit_default", [False, True])
def test_supported_default_and_unrelated_header_extensions_remain_usable(explicit_default):
    feed = message()
    vehicle(feed)
    if explicit_default:
        feed.header.incrementality = gtfs.FeedHeader.FULL_DATASET
    # Provider field1000 is unrelated to snapshot incrementality.
    feed.header.ParseFromString(feed.header.SerializeToString() + b"\xc0\x3e\x01")
    assert len(decode(feed)["vehicles"]) == 1
