"""Independent reconciliation against the frozen genuine vehicle protobuf."""

import hashlib

from google.transit import gtfs_realtime_pb2 as gtfs
import pytest

from data_horizon import collector
from data_horizon.config import Settings
from data_horizon.contracts import timestamp
import test_recorded
from test_recorded import input_path


frozen_capture = test_recorded.frozen_capture


@pytest.mark.case("F002-TC10")
@pytest.mark.evidence("recorded", "integration")
def test_frozen_genuine_vehicle_station_snapshot_matches_raw_fields(frozen_capture):
    record = next(item for item in frozen_capture["inputs"] if item["kind"] == "realtime")
    path = input_path(frozen_capture, "realtime")
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == record["sha256"]
    feed = gtfs.FeedMessage()
    feed.ParseFromString(raw)
    expected = [
        e.vehicle
        for e in feed.entity
        if e.HasField("vehicle") and e.vehicle.trip.route_id in {"1", "2", "3", "4", "5", "6"}
    ]
    # Independently checked immutable capture counts, not decoder-derived values.
    assert len(expected) == 82 and sum(v.HasField("position") for v in expected) == 0
    assert sum(v.HasField("current_status") for v in expected) == 49
    assert sum(feed.header.timestamp - v.timestamp > 120 for v in expected) == 3
    snapshot = collector.decode_tracking_snapshot(
        raw, Settings(mode="recorded"), retrieved_at=record["retrieved_at"]
    )
    assert snapshot["mode"] == "recorded" and len(snapshot["vehicles"]) == 82
    by_identity = {(v["route_id"], v["trip_id"], v["service_date"]): v for v in snapshot["vehicles"]}
    for vehicle in expected:
        date = vehicle.trip.start_date
        key = (vehicle.trip.route_id, vehicle.trip.trip_id, f"{date[:4]}-{date[4:6]}-{date[6:]}")
        row = by_identity[key]
        assert (
            row["stop_id"] == vehicle.stop_id
            and row["current_stop_sequence"] == vehicle.current_stop_sequence
        )
        assert row["status_explicit"] == vehicle.HasField("current_status")
        assert row["status"] == (
            gtfs.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
            if vehicle.HasField("current_status")
            else "UNSPECIFIED"
        )
        assert timestamp(row["last_movement_at"]).timestamp() == vehicle.timestamp
        assert row["direction_id"] is None and "latitude" not in row and "longitude" not in row
    assert timestamp(snapshot["feed_timestamp"]).timestamp() == feed.header.timestamp
