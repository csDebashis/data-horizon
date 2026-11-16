"""Independent reference cases for ingestion, state, analytics and graph rules."""

import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zipfile

import pytest

from data_horizon.analytics import aggregate, delay_seconds, health_score
from data_horizon.collector import (
    ScheduleMatcher,
    archive_response,
    atomic_write,
    fetch,
    ingest_static,
    synthetic_events,
    synthetic_static,
    validate_gtfs,
)
from data_horizon.config import Settings, trusted_url
from data_horizon.contracts import TransitEvent, service_time
from data_horizon.graph import canonical_edges, haversine_metres, what_if
from data_horizon.store import Store

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


def event(*, trip="a", stop="A", route="1", direction="0", when=NOW, delay=120, basis="observed", **extra):
    fields = {
        "source": "synthetic",
        "route_id": route,
        "direction_id": direction,
        "trip_id": trip,
        "stop_id": stop,
        "event_time": when,
        "scheduled_time": when - timedelta(seconds=delay),
        "time_basis": basis,
        "match_confidence": "exact",
        "scheduled_headway_seconds": 300,
    }
    fields["observed_time" if basis == "observed" else "predicted_time"] = when
    return TransitEvent(**{**fields, **extra}).model_dump(mode="json")


def gtfs_zip(*, missing=None, unsafe=False, version=1):
    files = {
        "routes.txt": "route_id,route_short_name,route_long_name\n1,1,Line One\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon,parent_station\nA,Alpha,40.7,-74.0,\nB,Beta,40.71,-74.01,\nC,Gamma,40.72,-74.02,\n",
        "trips.txt": "route_id,service_id,trip_id,direction_id\n1,weekday,T1,0\n",
        "stop_times.txt": f"trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,10:00:00,10:00:00,A,1\nT1,10:0{version}:00,10:0{version}:00,B,2\nT1,10:06:00,10:06:00,C,3\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nweekday,1,1,1,1,1,1,1,20260101,20261231\n",
    }
    if missing:
        files.pop(missing)
    if unsafe:
        files["../outside.txt"] = "malicious"
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return out.getvalue()


@pytest.mark.case("F001-TC01")
def test_configuration_defaults():
    s = Settings()
    assert s.routes == ("1", "2", "3", "4", "5", "6") and s.timezone == "America/New_York"
    assert Settings.from_yaml("configs/nyc.yaml").routes == s.routes


@pytest.mark.case("F001-TC02")
@pytest.mark.parametrize("routes", [(), ("7",), ("1", "Z")])
def test_reject_route_scope(routes):
    with pytest.raises(ValueError, match="route"):
        Settings(routes=routes)


@pytest.mark.case("F001-TC03")
def test_settings_fail_safe(monkeypatch):
    monkeypatch.setenv("DATA_MODE", "invalid")
    with pytest.raises(ValueError, match="DATA_MODE"):
        Settings()
    monkeypatch.setenv("DATA_MODE", "live")
    with pytest.raises(ValueError, match="Production requires"):
        Settings(environment="production", api_token="", database_url="")


@pytest.mark.case("F001-TC04")
@pytest.mark.integration
def test_retries_are_bounded():
    attempts = []
    sleeps = []

    def unavailable(*args, **kwargs):
        attempts.append(1)
        raise TimeoutError("source unavailable")

    with pytest.raises(TimeoutError):
        fetch(Settings().realtime_url, opener=unavailable, sleep=sleeps.append)
    assert len(attempts) == 3 and sleeps == [1, 2]


@pytest.mark.case("F001-TC05")
@pytest.mark.evidence("synthetic", "security")
@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "http://api-endpoint.mta.info/a",
        "https://127.0.0.1/a",
        "https://example.com/a",
        "https://api-endpoint.mta.info@127.0.0.1/a",
        "https://api.weather.gov:8000/a",
    ],
)
def test_source_allowlist(url):
    with pytest.raises(ValueError):
        trusted_url(url)


@pytest.mark.case("F002-TC01", "F002-TC05")
@pytest.mark.integration
def test_static_ingestion_and_repeat(isolated_data_dir):
    store = Store()
    raw = gtfs_zip()
    a = ingest_static(raw, store)
    b = ingest_static(raw, store)
    assert a["sha256"] == b["sha256"] and a["stop_count"] == 3
    assert len(store.graph_edges()) == 2 and len(store.routes()) == 1
    assert Path(a["path"]).read_bytes() == raw
    assert a["size_bytes"] == len(raw)


@pytest.mark.case("F002-TC02")
def test_corrupt_archive():
    with pytest.raises(ValueError, match="Corrupt"):
        validate_gtfs(b"notzip")


@pytest.mark.case("F002-TC03")
def test_missing_file_preserves_active_schedule(isolated_data_dir):
    store = Store()
    original = ingest_static(gtfs_zip(), store)
    with pytest.raises(ValueError, match="stops.txt"):
        ingest_static(gtfs_zip(missing="stops.txt"), store)
    assert store.get_metadata("static")["sha256"] == original["sha256"]


@pytest.mark.case("F002-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_zip_slip_rejected(tmp_path):
    with pytest.raises(ValueError, match="Unsafe"):
        validate_gtfs(gtfs_zip(unsafe=True))
    assert not (tmp_path / "outside.txt").exists()


@pytest.mark.case("F002-TC06")
@pytest.mark.recovery
def test_atomic_publication_failure(tmp_path, monkeypatch):
    path = tmp_path / "published.bin"
    path.write_bytes(b"old")
    original = Path.replace

    def fail(self, target):
        raise OSError("injected publication failure")

    monkeypatch.setattr(Path, "replace", fail)
    with pytest.raises(OSError):
        atomic_write(path, b"new")
    assert path.read_bytes() == b"old" and not list(tmp_path.glob("*.tmp"))
    monkeypatch.setattr(Path, "replace", original)
    atomic_write(path, b"new")
    assert path.read_bytes() == b"new"


@pytest.mark.case("F003-TC01")
@pytest.mark.parametrize(
    "patch",
    [{"trip_id": ""}, {"event_time": "2026-11-14T12:00:00"}, {"latitude": float("nan")}, {"stop_id": " "}],
)
def test_contract_rejection(patch):
    with pytest.raises(ValueError):
        TransitEvent.model_validate({**event(), **patch})


@pytest.mark.case("F003-TC02")
def test_duplicate_is_not_another_event(isolated_data_dir):
    store = Store()
    e = event()
    result = store.apply_batch([e, e], batch_id="a")
    assert result["processed"] == 1 and result["duplicates"] == 1 and len(store.events()) == 1


@pytest.mark.case("F003-TC03")
def test_revised_prediction_has_distinct_identity(isolated_data_dir):
    a = event(basis="predicted")
    b = {**a, "event_id": "", "predicted_time": (NOW + timedelta(minutes=1)).isoformat()}
    b = TransitEvent.model_validate(b).model_dump(mode="json")
    assert a["event_id"] != b["event_id"]
    store = Store()
    assert store.apply_batch([a, b], batch_id="revisions")["processed"] == 2


@pytest.mark.case("F003-TC04")
def test_ambiguous_match_does_not_invent_actuals():
    matcher = ScheduleMatcher()
    matcher.suffix["ambiguous"] = [{"stops": [(1, "A", "10:00:00")]}, {"stops": [(1, "A", "10:00:00")]}]
    assert matcher.match("ambiguous", "A", "2026-11-14") == (None, "ambiguous")
    with pytest.raises(ValueError):
        event(match_confidence="ambiguous")


@pytest.mark.case("F003-TC05")
def test_service_day_over_midnight():
    result = service_time("2026-11-14", "25:05:00")
    assert result.isoformat() == "2026-11-15T06:05:00+00:00"


@pytest.mark.case("F003-TC06")
def test_dst_explicit():
    with pytest.raises(ValueError, match="Nonexistent"):
        service_time("2026-03-08", "02:30:00")
    with pytest.raises(ValueError, match="Ambiguous"):
        service_time("2026-11-01", "01:30:00")
    assert (
        service_time("2026-11-01", "01:30:00", fold=1) - service_time("2026-11-01", "01:30:00", fold=0)
    ).total_seconds() == 3600


@pytest.mark.case("F003-TC07")
def test_cancellation_and_alert_expiry():
    cancelled = event(cancelled=True)
    assert delay_seconds(cancelled) is None
    rows = aggregate(
        [event()],
        as_of=NOW,
        alerts=[{"severity": 1, "route_ids": ["1"], "end_time": (NOW - timedelta(seconds=1)).isoformat()}],
    )
    assert rows[0]["alert_count"] == 0


@pytest.mark.case("F004-TC04")
def test_watermark_boundary_and_correction(isolated_data_dir):
    store = Store()
    store.apply_batch([event()], batch_id="first")
    result = store.apply_batch(
        [
            event(trip="inside", when=NOW - timedelta(seconds=600)),
            event(trip="outside", when=NOW - timedelta(seconds=601)),
        ],
        batch_id="late",
    )
    assert result["processed"] == 1 and result["late"] == 1
    assert store.pipeline_status()["counts"]["quarantine"] == 1


def test_transaction_marker_retries_and_rollback(isolated_data_dir):
    store = Store()
    e = event()
    store.apply_batch([e], batch_id="committed")
    assert store.apply_batch([e], batch_id="committed")["batch_replayed"]

    def crash(*args, **kwargs):
        raise RuntimeError("before transaction commit")

    with pytest.raises(RuntimeError):
        store.apply_batch([event(trip="rolled-back")], batch_id="uncommitted", aggregate_fn=crash)
    assert len(store.events()) == 1
    assert store.apply_batch([event(trip="rolled-back")], batch_id="uncommitted")["processed"] == 1


@pytest.mark.case("F004-TC08")
@pytest.mark.integration
def test_modes_are_isolated(isolated_data_dir, monkeypatch):
    store = Store()
    store.apply_batch([event()], batch_id="synthetic")
    monkeypatch.setenv("DATA_MODE", "live")
    live = Store(data_dir=isolated_data_dir)
    assert live.events() == [] and live.pipeline_status()["counts"]["metrics"] == 0


@pytest.mark.case("F005-TC01")
def test_delay_reference():
    assert delay_seconds(event(delay=120)) == 120


@pytest.mark.case("F005-TC02")
def test_prediction_basis():
    row = aggregate([event(basis="predicted")], as_of=NOW)[0]
    assert row["time_basis"] == "predicted" and row["delay_mean_seconds"] == 120


@pytest.mark.case("F005-TC03")
def test_headway_reference():
    rows = aggregate(
        [event(trip="first", when=NOW - timedelta(seconds=360)), event(trip="second")], as_of=NOW
    )
    assert rows[0]["headway_seconds"] == 360 and rows[0]["headway_deviation"] == pytest.approx(0.2)


@pytest.mark.case("F005-TC04")
def test_directions_are_separate():
    rows = aggregate([event(), event(trip="other", direction="1")], as_of=NOW)
    assert len(rows) == 2 and all(r["headway_seconds"] is None for r in rows)


@pytest.mark.case("F005-TC05")
def test_rolling_windows_do_not_include_future_or_left_boundary():
    rows = aggregate(
        [
            event(trip="boundary", when=NOW - timedelta(minutes=5), delay=900),
            event(trip="future", when=NOW + timedelta(seconds=1), delay=700),
            event(delay=100),
        ],
        as_of=NOW,
    )
    assert rows[0]["rolling_5m_mean"] == 100 and rows[0]["rolling_15m_mean"] == 500


@pytest.mark.case("F005-TC06")
def test_missing_values_stay_missing():
    e = event()
    e["scheduled_time"] = None
    e["scheduled_headway_seconds"] = 0
    row = aggregate([e], as_of=NOW)[0]
    assert row["delay_mean_seconds"] is None and row["health_score"] is None


@pytest.mark.case("F005-TC08")
@pytest.mark.integration
def test_historical_correction_touches_only_requested_window(isolated_data_dir):
    store = Store()
    store.apply_batch([event()], batch_id="initial")
    untouched = store.metrics()[0]
    previous = NOW - timedelta(minutes=1)
    store.apply_batch(
        [event(trip="late-correction", when=previous, delay=300)],
        batch_id="correction",
        allow_late=True,
        as_of=previous,
    )
    assert next(m for m in store.metrics() if m["window_start"] == untouched["window_start"]) == untouched


def edges():
    return [
        {"source": "1|0|A", "target": "1|0|B", "travel_seconds": 60, "edge_type": "travel"},
        {"source": "1|0|B", "target": "1|0|C", "travel_seconds": 60, "edge_type": "travel"},
        {"source": "1|0|C", "target": "1|0|A", "travel_seconds": 60, "edge_type": "travel"},
        {"source": "1|0|B", "target": "2|0|D", "travel_seconds": 30, "edge_type": "transfer"},
        {"source": "3|0|X", "target": "3|0|Y", "travel_seconds": 10, "edge_type": "travel"},
    ]


@pytest.mark.case("F006-TC01", "F006-TC02")
def test_directed_edges_and_transfer():
    graph = canonical_edges(edges())
    assert not any(e["source"] == "1|0|B" and e["target"] == "1|0|A" for e in graph)
    assert next(e for e in graph if e["edge_type"] == "transfer")["travel_seconds"] == 30


@pytest.mark.case("F006-TC03")
def test_duplicate_edges_have_deterministic_minimum_cost():
    a = edges()[0]
    b = {**a, "travel_seconds": 90}
    assert canonical_edges([a, b]) == canonical_edges([b, a]) and len(canonical_edges([a, b])) == 1


@pytest.mark.case("F006-TC04")
@pytest.mark.parametrize("cost", [-1, float("inf"), float("nan")])
def test_invalid_costs(cost):
    with pytest.raises(ValueError):
        canonical_edges([{**edges()[0], "travel_seconds": cost}])


@pytest.mark.case("F006-TC05", "F006-TC06", "F008-TC05")
def test_cycles_terminate_and_disconnected_nodes_stay_out():
    result = what_if(edges(), "1", "A", 300)
    ids = [r["node_id"] for r in result["affected_stations"]]
    assert set(ids) == {"1|0|B", "1|0|C", "2|0|D"} and len(ids) == len(set(ids))


@pytest.mark.case("F006-TC07")
def test_coordinate_validation():
    assert haversine_metres(40, -74, 40, -74) == 0
    assert 1000 < haversine_metres(40, -74, 40.01, -74) < 1200
    with pytest.raises(ValueError):
        haversine_metres(100, 0, 40, 0)


@pytest.mark.case("F007-TC01", "F007-TC02", "F007-TC03", "F007-TC04")
def test_health_independent_reference():
    assert health_score(0, 0, 0)["health_score"] == 100
    assert health_score(300, 0.5, 1)["health_score"] == 45
    assert health_score(9000, 90, 2)["health_score"] == 0
    assert health_score(None, 0, 0)["health_level"] == "unknown"


@pytest.mark.case("F007-TC05")
def test_stale_network_is_unknown(isolated_data_dir):
    store = Store()
    synthetic_static(store)
    store.apply_batch(synthetic_events(NOW), batch_id="old")
    assert store.pipeline_status()["stale"] and store.network_health()["health_score"] is None


@pytest.mark.case("F007-TC06")
@pytest.mark.parametrize("delay,expected", [(299, None), (300, "high_delay"), (301, "high_delay")])
def test_anomaly_boundaries(delay, expected):
    assert aggregate([event(delay=delay)], as_of=NOW)[0]["anomaly"] == expected


@pytest.mark.case("F007-TC07")
@pytest.mark.integration
def test_anomaly_recovers_and_history_is_retained(isolated_data_dir):
    store = Store()
    store.apply_batch([event(delay=600)], batch_id="disruption")
    later = NOW + timedelta(minutes=31)
    store.apply_batch(
        [
            event(trip="healthy1", when=later - timedelta(minutes=5), delay=0),
            event(trip="healthy2", when=later, delay=0),
        ],
        batch_id="recovered",
    )
    assert store.anomalies() == [] and any(m["anomaly"] == "high_delay" for m in store.metrics())


@pytest.mark.case("F008-TC01")
def test_what_if_paths_and_limit():
    result = what_if(edges(), "1", "A", 300, limit=2)
    assert len(result["affected_stations"]) == 2
    assert result["affected_stations"][0]["path"] == ["1|0|A", "1|0|B"]


@pytest.mark.case("F008-TC02")
def test_missing_origin():
    with pytest.raises(LookupError):
        what_if(edges(), "6", "missing", 1)


@pytest.mark.case("F008-TC03")
@pytest.mark.evidence("synthetic", "contract")
@pytest.mark.parametrize(
    "fields",
    [
        {"delay_seconds": -1},
        {"delay_seconds": 7201},
        {"horizon_minutes": 0},
        {"horizon_minutes": 61},
        {"limit": 0},
        {"limit": 101},
    ],
)
def test_scenario_limits(fields):
    with pytest.raises(ValueError):
        what_if(edges(), "1", "A", **{"delay_seconds": 1, **fields})


@pytest.mark.case("F008-TC04")
def test_horizon_boundary():
    graph = [
        {"source": "1|0|A", "target": "1|0|B", "travel_seconds": 60},
        {"source": "1|0|B", "target": "1|0|C", "travel_seconds": 1},
    ]
    assert [r["stop_id"] for r in what_if(graph, "1", "A", 300, horizon_minutes=1)["affected_stations"]] == [
        "B"
    ]


@pytest.mark.case("F008-TC06")
def test_zero_delay():
    assert what_if(edges(), "1", "A", 0)["affected_stations"] == []


@pytest.mark.case("F008-TC07")
@pytest.mark.integration
def test_scenarios_are_read_only(isolated_data_dir):
    store = Store()
    synthetic_static(store)
    before = store.pipeline_status()["counts"]
    graph = store.graph_edges()
    what_if(graph, "1", "101N", 500)
    assert before == store.pipeline_status()["counts"] and graph == store.graph_edges()


@pytest.mark.case("F008-TC08")
def test_rank_is_stable():
    assert what_if(edges(), "1", "A", 300) == what_if(list(reversed(edges())), "1", "A", 300)


def test_future_weather_is_not_joined():
    rows = aggregate(
        [event()],
        as_of=NOW,
        weather={
            "temperature_c": 30,
            "observed_at": (NOW + timedelta(hours=1)).isoformat(),
            "retrieved_at": (NOW + timedelta(hours=1)).isoformat(),
        },
    )
    assert rows[0]["weather_temperature_c"] is None


def test_disk_pressure_preserves_inputs(isolated_data_dir, monkeypatch):
    settings = Settings.load()
    existing = isolated_data_dir / "frozen.bin"
    existing.write_bytes(b"keep")
    monkeypatch.setenv("MIN_FREE_GB", "999999")
    with pytest.raises(OSError, match="DATA_DISK_LOW"):
        archive_response(settings, "test", b"new")
    assert existing.read_bytes() == b"keep" and not (isolated_data_dir / "raw").exists()


def test_obsolete_schema_is_not_silently_upgraded(isolated_data_dir):
    import sqlite3

    path = isolated_data_dir / "horizon.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE analytics_result(namespace TEXT,item_key TEXT,window_start TEXT,payload TEXT,PRIMARY KEY(namespace,item_key))"
        )
        record = {
            "route_id": "1",
            "direction_id": "0",
            "stop_id": "A",
            "window_start": NOW.isoformat(),
            "delay_mean_seconds": 123,
        }
        conn.execute(
            "INSERT INTO analytics_result VALUES(?,?,?,?)",
            ("synthetic", "legacy", NOW.isoformat(), json.dumps(record)),
        )
    with pytest.raises(sqlite3.OperationalError, match="no such column"):
        Store()
    with sqlite3.connect(path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(analytics_result)")}
        assert "route_id" not in columns and "stop_id" not in columns
        assert conn.execute("SELECT payload FROM analytics_result").fetchone()[0] == json.dumps(record)


@pytest.mark.feature("F004", "F010")
def test_pipeline_status_exposes_domain_registry_and_mirror_diagnostics(isolated_data_dir):
    store = Store()
    store.put_metadata("event_mirrors", {"service-alerts": {"status": "degraded", "reason": "ConnectionError"}})
    status = store.pipeline_status()
    assert set(status["event_topics"]) == {
        "transit-events", "service-alerts", "vehicle-status", "weather-observations", "static-snapshots"
    }
    assert status["event_topics"]["transit-events"] == "data-horizon.synthetic.transit-events.v1"
    assert status["event_mirrors"]["service-alerts"]["status"] == "degraded"


@pytest.mark.case('F003-COL02', 'F005-COL01', 'F007-COL01')
@pytest.mark.parametrize('confidence', ['tentative', 'unmatched', 'ambiguous'])
@pytest.mark.parametrize('explicit_headway', [None, 300])
def test_untrusted_schedule_cannot_create_delay_or_comparison(confidence, explicit_headway):
    """An arrival forecast 17,280 seconds before a guessed schedule is not a delay."""
    rows = [event(trip=f'forecast-{index}', when=NOW - timedelta(minutes=1 - index),
                  basis='predicted', delay=-17280, match_confidence=confidence,
                  scheduled_headway_seconds=explicit_headway) for index in range(2)]
    result = aggregate(rows, as_of=NOW)[0]
    assert all(delay_seconds(row) is None for row in rows)
    assert result['event_count'] == 2 and result['time_basis'] == 'predicted'
    assert result['headway_seconds'] == 60  # Independent of the untrusted timetable.
    for field in ('delay_mean_seconds', 'delay_stddev_seconds', 'scheduled_headway_seconds',
                  'headway_deviation', 'health_score'):
        assert result[field] is None
    for minutes in (5, 15, 30):
        assert result[f'rolling_{minutes}m_mean'] is None
        assert result[f'rolling_{minutes}m_stddev'] is None
    assert result['health_level'] == 'unknown' and result['anomaly'] is None


@pytest.mark.case('F003-COL02', 'F005-COL01', 'F007-COL01')
@pytest.mark.parametrize('explicit_headway', [None, 60])
def test_exact_forecasts_and_mixed_schedule_confidence_keep_only_trusted_comparisons(explicit_headway):
    exact = [event(trip=f'exact-{index}', when=NOW - timedelta(minutes=3 - index),
                   basis='predicted', delay=120, scheduled_headway_seconds=explicit_headway)
             for index in range(2)]
    untrusted = [event(trip=f'tentative-{index}', when=NOW - timedelta(minutes=1 - index),
                       basis='predicted', delay=-17280, match_confidence='tentative',
                       scheduled_headway_seconds=9999) for index in range(2)]
    control, mixed = aggregate(exact, as_of=NOW)[0], aggregate(exact + untrusted, as_of=NOW)[0]
    for result in (control, mixed):
        assert result['delay_mean_seconds'] == 120 and result['delay_stddev_seconds'] == 0
        assert result['scheduled_headway_seconds'] == 60 and result['headway_seconds'] == 60
        assert result['headway_deviation'] == 0 and result['health_score'] == 88
        for minutes in (5, 15, 30):
            assert result[f'rolling_{minutes}m_mean'] == 120
            assert result[f'rolling_{minutes}m_stddev'] == 0
    assert mixed['event_count'] == 4


@pytest.mark.case('F003-COL02', 'F005-COL01')
def test_latest_tentative_revision_does_not_reuse_older_exact_schedule():
    rows = [event(trip='revised', when=NOW - timedelta(minutes=3), basis='predicted',
                  scheduled_headway_seconds=None),
            event(trip='other', when=NOW - timedelta(minutes=2), basis='predicted',
                  scheduled_headway_seconds=None),
            event(trip='revised', when=NOW - timedelta(minutes=1), basis='predicted', delay=-17280,
                  match_confidence='tentative', scheduled_headway_seconds=9999)]
    result = aggregate(rows, as_of=NOW)[0]
    assert result['event_count'] == 2 and result['headway_seconds'] == 60
    assert result['delay_mean_seconds'] == 120  # Only the still-exact other trip.
    assert result['scheduled_headway_seconds'] is None and result['headway_deviation'] is None
    assert result['health_level'] == 'unknown' and result['health_score'] is None
    assert all(result[f'rolling_{minutes}m_mean'] == 120 for minutes in (5, 15, 30))
