"""Failure/reconnect mechanics and real Protobuf contract fixtures."""

from datetime import timedelta
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs

from data_horizon.analytics import aggregate, alert_active
from data_horizon.collector import (
    ScheduleMatcher,
    collect_once,
    decode_feed,
    drain_outbox,
    ingest_static,
    publish,
    refresh_static,
    static_refresh_intervals,
)
from data_horizon.config import Settings
from data_horizon.contracts import TransitEvent, utcnow
from data_horizon.store import Store


def feed(*, cancelled=False, vehicle=False, alerts=False, now=None):
    now = now or utcnow()
    message = gtfs.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = int(now.timestamp())
    if alerts:
        a = message.entity.add()
        a.id = "alert-new"
        a.alert.informed_entity.add().route_id = "1"
        a.alert.header_text.translation.add(text="Signal disruption", language="en")
        return message.SerializeToString()
    if vehicle:
        e = message.entity.add()
        e.id = "vehicle"
        e.vehicle.trip.trip_id = "T1"
        e.vehicle.trip.route_id = "1"
        e.vehicle.stop_id = "A"
        e.vehicle.timestamp = int(now.timestamp())
    else:
        e = message.entity.add()
        e.id = "trip"
        e.trip_update.trip.route_id = "1"
        e.trip_update.trip.trip_id = "T1"
        e.trip_update.trip.start_date = now.strftime("%Y%m%d")
        if cancelled:
            e.trip_update.trip.schedule_relationship = gtfs.TripDescriptor.CANCELED
        else:
            s = e.trip_update.stop_time_update.add()
            s.stop_id = "A"
            s.arrival.time = int((now + timedelta(minutes=2)).timestamp())
    return message.SerializeToString()


@pytest.mark.case("F003-TC07")
def test_native_no_stop_cancellation_is_not_dropped():
    now = utcnow()
    settings = Settings()
    updates, _, _ = decode_feed(feed(now=now), settings)
    controls, _, _ = decode_feed(feed(cancelled=True, now=now + timedelta(seconds=1)), settings)
    assert controls[0]["stop_id"] == "__trip__" and controls[0]["cancelled"]
    rows = aggregate(updates + controls, as_of=now + timedelta(seconds=2))
    assert len(rows) == 1 and rows[0]["event_count"] == 0 and rows[0]["health_level"] == "unknown"
    assert all(r["stop_id"] != "__trip__" for r in rows)


def test_vehicle_status_is_not_an_actual_arrival_or_prediction_replacement():
    now = utcnow()
    settings = Settings()
    updates, _, _ = decode_feed(feed(now=now), settings)
    vehicles, _, _ = decode_feed(feed(vehicle=True, now=now + timedelta(seconds=1)), settings)
    assert vehicles[0]["time_basis"] == "inferred" and vehicles[0]["observed_time"] is None
    assert vehicles[0]["latitude"] is None
    rows = aggregate(updates + vehicles, as_of=now + timedelta(seconds=2))
    assert rows[0]["time_basis"] == "predicted" and rows[0]["event_count"] == 1


@pytest.mark.case("F003-TC01")
@pytest.mark.parametrize("raw", [b"<Error>NoSuchKey</Error>", b"", b"not protobuf"])
def test_malformed_source_is_validation_error(raw):
    with pytest.raises(ValueError, match="GTFS-Realtime"):
        decode_feed(raw, Settings())


def test_alert_disjoint_period_gap_is_inactive():
    now = utcnow()
    a = {
        "active_periods": [
            {
                "start_time": (now - timedelta(hours=2)).isoformat(),
                "end_time": (now - timedelta(hours=1)).isoformat(),
            },
            {
                "start_time": (now + timedelta(hours=1)).isoformat(),
                "end_time": (now + timedelta(hours=2)).isoformat(),
            },
        ]
    }
    assert not alert_active(a, now)
    assert alert_active(a, now + timedelta(minutes=90))
    assert alert_active({"active_periods": [{"start_time": None, "end_time": None}]}, now)


class Producer:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def send(self, topic, *, key, value):
        self.sent.append((topic, key, value))
        return self

    def get(self, timeout):
        if self.fail:
            raise TimeoutError("injected broker failure")

    def flush(self, timeout):
        pass


@pytest.mark.feature("F004")
def test_publication_outbox_survives_partial_failure_and_reconnect(isolated_data_dir):
    settings = Settings.load()
    now = utcnow()
    events, _, _ = decode_feed(feed(now=now), settings)
    failed = Producer(fail=True)
    with pytest.raises(TimeoutError):
        publish(settings, events, failed)
    pending = list((isolated_data_dir / "outbox" / "synthetic" / "pending").glob("*.json"))
    assert len(pending) == 1
    source_id = failed.sent[0][2]["event_id"]
    healthy = Producer()
    result = drain_outbox(settings, healthy)
    assert result == {"published_events": 1, "pending_batches": 0}
    assert healthy.sent[0][2]["event_id"] == source_id
    assert len(list((isolated_data_dir / "outbox" / "synthetic" / "published").glob("*.json"))) == 1
    assert drain_outbox(settings, healthy)["published_events"] == 0


def test_unavailable_broker_keeps_archive_pending(isolated_data_dir):
    settings = Settings(kafka_servers="unavailable:9092")
    events, _, _ = decode_feed(feed(), settings)
    with pytest.raises(ConnectionError, match="durable outbox"):
        publish(settings, events)
    entry = json.loads(
        next((isolated_data_dir / "outbox" / "synthetic" / "pending").glob("*.json")).read_text()
    )
    assert entry["manifest"]["status"] == "archived"


def test_outbox_refuses_tampered_archive(isolated_data_dir):
    settings = Settings.load()
    events, _, _ = decode_feed(feed(), settings)
    with pytest.raises(TimeoutError):
        publish(settings, events, Producer(fail=True))
    p = next((isolated_data_dir / "outbox" / "synthetic" / "pending").glob("*.json"))
    entry = json.loads(p.read_text())
    from pathlib import Path

    Path(entry["manifest"]["path"]).write_text("changed")
    with pytest.raises(ValueError, match="checksum"):
        drain_outbox(settings, Producer())
    assert p.exists()


def test_alert_outage_keeps_last_good_and_source_failure_recovers(isolated_data_dir, monkeypatch):
    store = Store()
    settings = Settings.load()
    now = utcnow()
    previous = {
        "id": "known",
        "route_ids": ["1"],
        "severity": 1,
        "description": "Known disruption",
        "end_time": (now + timedelta(hours=1)).isoformat(),
        "available_at": (now - timedelta(minutes=1)).isoformat(),
    }
    store.replace_assets("alerts", [previous])
    success = (now - timedelta(seconds=30)).isoformat()
    store.put_metadata("sources", {"alerts": {"status": "ok", "last_success": success}})
    store.put_metadata("weather", {"temperature_c": 10, "retrieved_at": now.isoformat()})

    def transport(url, **kwargs):
        if url == settings.alerts_url:
            raise OSError("injected alert outage")
        return feed(now=now)

    monkeypatch.setattr("data_horizon.collector.fetch", transport)
    collect_once(store, settings)
    assert store.alerts() == [previous]
    status = store.get_metadata("sources")
    assert status["alerts"]["status"] == "degraded" and status["alerts"]["last_success"] == success

    def unavailable(*args, **kwargs):
        raise OSError("transit outage")

    monkeypatch.setattr("data_horizon.collector.fetch", unavailable)
    with pytest.raises(OSError):
        collect_once(store, settings)
    assert store.get_metadata("sources")["transit"]["status"] == "degraded"
    monkeypatch.setattr(
        "data_horizon.collector.fetch",
        lambda url, **kwargs: feed(alerts=url == settings.alerts_url, now=utcnow()),
    )
    collect_once(store, settings)
    assert store.get_metadata("sources")["transit"]["status"] == "ok"
    assert store.get_metadata("sources")["alerts"]["status"] == "ok"


def test_replay_identity_preserves_source_revision():
    e = decode_feed(feed(), Settings())[0][0]
    copy = {**e, "event_id": "", "replay_run_id": "new-run"}
    assert TransitEvent.model_validate(copy).event_id == e["event_id"]


def schedule_zip(minute=5):
    files = {
        "routes.txt": "route_id,route_short_name,route_long_name\n1,1,Line One\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nA,Alpha,40.7,-74\nB,Beta,40.71,-74.01\n",
        "trips.txt": "route_id,service_id,trip_id,direction_id\n1,weekday,T1,0\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            f"T1,10:00:00,10:00:00,A,1\nT1,10:{minute:02}:00,10:{minute:02}:00,B,2\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "weekday,1,1,1,1,1,1,1,20260101,20261231\n"
        ),
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return output.getvalue()


@pytest.fixture
def static_context(isolated_data_dir, monkeypatch):
    monkeypatch.setenv("DATA_MODE", "live")
    monkeypatch.setenv("MIN_FREE_GB", "0")
    monkeypatch.delenv("STATIC_REFRESH_SECONDS", raising=False)
    monkeypatch.delenv("STATIC_RETRY_SECONDS", raising=False)
    now = utcnow()
    monkeypatch.setattr("data_horizon.collector.utcnow", lambda: now)
    settings = Settings.load()
    store = Store()
    report = ingest_static(schedule_zip(), store, settings)
    return store, settings, now, report


@pytest.mark.feature("F001")
def test_static_fresh_cache_reuses_loaded_matcher_without_request(static_context, monkeypatch):
    store, settings, now, report = static_context
    loads = []

    def load(path):
        loads.append(path)
        return ScheduleMatcher(path)

    def forbidden(*args, **kwargs):
        pytest.fail("A fresh static cache must not request the network")

    monkeypatch.setattr("data_horizon.collector.ScheduleMatcher", load)
    monkeypatch.setattr("data_horizon.collector.fetch", forbidden)
    matcher = refresh_static(store, settings, now=now + timedelta(seconds=1))
    assert refresh_static(store, settings, matcher, now=now + timedelta(seconds=30)) is matcher
    assert loads == [report["schedule_path"]]
    status = store.get_metadata("sources")["static"]
    assert status["status"] == "ok" and status["last_success"] == report["retrieved_at"]
    assert status["mode"] == "live" and "last_attempt" not in status


@pytest.mark.feature("F001")
@pytest.mark.feature("F002")
def test_expired_static_refresh_updates_matcher_and_preserves_old_version(static_context, monkeypatch):
    store, settings, now, old = static_context
    clock = now + timedelta(days=1)
    monkeypatch.setattr("data_horizon.collector.utcnow", lambda: clock)
    requests = []

    def transport(url, **kwargs):
        requests.append((url, kwargs))
        return schedule_zip(8)

    monkeypatch.setattr("data_horizon.collector.fetch", transport)
    previous = ScheduleMatcher(old["schedule_path"])
    matcher = refresh_static(store, settings, previous, now=clock)
    report = store.get_metadata("static")
    assert requests == [(settings.static_url, {"max_bytes": 64 * 1024 * 1024})]
    assert matcher is not previous and report["static_version"] != old["static_version"]
    assert Path(old["schedule_path"]).is_file() and Path(old["path"]).is_file()
    assert matcher.match("T1", "B", "2026-11-14")[0].endswith("15:08:00+00:00")
    assert previous.match("T1", "B", "2026-11-14")[0].endswith("15:05:00+00:00")
    assert store.assets("edges")[0]["travel_seconds"] == 480
    source = store.get_metadata("sources")["static"]
    assert source["status"] == "ok" and source["last_success"] == clock.isoformat()
    assert (
        source["last_attempt"] == clock.isoformat() and source["static_version"] == report["static_version"]
    )


@pytest.mark.feature("F001")
def test_failed_static_refresh_retains_cache_and_persisted_retry_backoff(static_context, monkeypatch):
    store, settings, now, old = static_context
    clock = now + timedelta(days=2)
    matcher = ScheduleMatcher(old["schedule_path"])
    requests = []

    def unavailable(*args, **kwargs):
        requests.append(clock)
        raise OSError("injected static failure")

    monkeypatch.setattr("data_horizon.collector.fetch", unavailable)
    assert refresh_static(store, settings, matcher, now=clock) is matcher
    source = store.get_metadata("sources")["static"]
    assert source["status"] == "degraded" and source["using_cached_schedule"] is True
    assert source["last_success"] == old["retrieved_at"] and source["last_attempt"] == clock.isoformat()
    assert store.get_metadata("static") == old
    # A new Store simulates a restart; the retry deadline is not only in process memory.
    restarted = Store()
    cached = refresh_static(restarted, settings, now=clock + timedelta(seconds=299))
    assert cached.match("T1", "B", "2026-11-14") == matcher.match("T1", "B", "2026-11-14")
    assert len(requests) == 1
    refresh_static(restarted, settings, cached, now=clock + timedelta(seconds=300))
    assert len(requests) == 2
    recovered = clock + timedelta(seconds=600)
    monkeypatch.setattr("data_horizon.collector.utcnow", lambda: recovered)
    monkeypatch.setattr("data_horizon.collector.fetch", lambda *args, **kwargs: schedule_zip(9))
    replacement = refresh_static(restarted, settings, cached, now=recovered)
    source = restarted.get_metadata("sources")["static"]
    assert replacement is not cached and source["status"] == "ok" and "reason" not in source
    assert source["last_success"] == recovered.isoformat()


@pytest.mark.feature("F002")
def test_invalid_static_refresh_archived_without_replacing_active_assets(static_context, monkeypatch):
    store, settings, now, old = static_context
    before = {kind: store.assets(kind) for kind in ("routes", "stops", "edges")}
    matcher = ScheduleMatcher(old["schedule_path"])
    malformed = b"<Error>upstream failure</Error>"
    monkeypatch.setattr("data_horizon.collector.fetch", lambda *args, **kwargs: malformed)
    assert refresh_static(store, settings, matcher, now=now + timedelta(days=1)) is matcher
    assert store.get_metadata("static") == old
    assert {kind: store.assets(kind) for kind in before} == before
    archived = list(settings.data_dir.rglob(hashlib.sha256(malformed).hexdigest() + ".bin"))
    assert len(archived) == 1 and archived[0].read_bytes() == malformed
    assert store.get_metadata("sources")["static"]["reason"] == "ValueError"


@pytest.mark.feature("F002")
def test_static_lookup_publication_failure_preserves_old_graph(static_context, monkeypatch):
    from data_horizon.collector import atomic_write

    store, settings, now, old = static_context
    previous = ScheduleMatcher(old["schedule_path"])
    old_edges = store.assets("edges")

    def interrupted(path, content):
        if path.name.endswith(".schedule-v2.jsonl"):
            raise OSError("injected schedule publication failure")
        return atomic_write(path, content)

    monkeypatch.setattr("data_horizon.collector.fetch", lambda *args, **kwargs: schedule_zip(8))
    monkeypatch.setattr("data_horizon.collector.atomic_write", interrupted)
    assert refresh_static(store, settings, previous, now=now + timedelta(days=1)) is previous
    assert store.get_metadata("static") == old and store.assets("edges") == old_edges
    assert store.get_metadata("sources")["static"]["status"] == "degraded"


@pytest.mark.feature("F002")
def test_static_metadata_failure_rolls_back_graph_and_retry_publishes_together(static_context, monkeypatch):
    store, settings, now, old = static_context
    previous = ScheduleMatcher(old["schedule_path"])
    old_assets = {kind: store.assets(kind) for kind in ("routes", "stops", "edges")}
    clock = now + timedelta(days=1)
    monkeypatch.setattr("data_horizon.collector.utcnow", lambda: clock)
    monkeypatch.setattr("data_horizon.collector.fetch", lambda *args, **kwargs: schedule_zip(8))
    original_put = store._put

    def fail_static_metadata(connection, key, value):
        if key == "static":
            raise OSError("injected metadata write failure after asset replacement")
        return original_put(connection, key, value)

    monkeypatch.setattr(store, "_put", fail_static_metadata)
    assert refresh_static(store, settings, previous, now=clock) is previous
    assert store.get_metadata("static") == old
    assert {kind: store.assets(kind) for kind in old_assets} == old_assets
    assert store.get_metadata("sources")["static"]["status"] == "degraded"
    # The completed immutable candidate may remain on disk; no reader adopted it.
    assert len(list((settings.data_dir / "static").glob("*.schedule-v2.jsonl"))) == 2
    monkeypatch.setattr(store, "_put", original_put)
    clock += timedelta(seconds=300)
    replacement = refresh_static(store, settings, previous, now=clock)
    current = store.get_metadata("static")
    assert current["static_version"] != old["static_version"] and replacement is not previous
    assert {edge["static_version"] for edge in store.assets("edges")} == {current["static_version"]}
    assert store.assets("edges")[0]["travel_seconds"] == 480
    assert store.get_metadata("sources")["static"]["status"] == "ok"


@pytest.mark.feature("F001")
def test_missing_static_cache_failure_is_visible_and_cannot_collect(static_context, monkeypatch):
    store, settings, now, old = static_context
    Path(old["schedule_path"]).unlink()

    def unavailable(*args, **kwargs):
        raise OSError("injected static failure")

    monkeypatch.setattr("data_horizon.collector.fetch", unavailable)
    with pytest.raises(OSError):
        refresh_static(store, settings, now=now)
    source = store.get_metadata("sources")["static"]
    assert source["status"] == "degraded" and source["using_cached_schedule"] is False
    with pytest.raises(RuntimeError, match="retry is pending"):
        refresh_static(store, settings, now=now + timedelta(seconds=1))


@pytest.mark.feature("F001")
@pytest.mark.parametrize("name", ["STATIC_REFRESH_SECONDS", "STATIC_RETRY_SECONDS"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "59", "604801", "secret-looking-invalid"])
def test_static_refresh_intervals_fail_safely(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name) as captured:
        static_refresh_intervals()
    if value == "secret-looking-invalid":
        assert value not in str(captured.value)


@pytest.mark.feature("F001")
def test_static_refresh_intervals_default_and_boundaries(monkeypatch):
    monkeypatch.delenv("STATIC_REFRESH_SECONDS", raising=False)
    monkeypatch.delenv("STATIC_RETRY_SECONDS", raising=False)
    assert static_refresh_intervals() == (3600, 300)
    monkeypatch.setenv("STATIC_REFRESH_SECONDS", "60")
    monkeypatch.setenv("STATIC_RETRY_SECONDS", "604800")
    assert static_refresh_intervals() == (60, 604800)
