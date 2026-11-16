"""Frozen genuine-source tests; missing prerequisites remain explicit blockers."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from zoneinfo import ZoneInfo

import pytest

from data_horizon.analytics import aggregate
from data_horizon.collector import (
    ScheduleMatcher, archive_response, decode_feed, fetch, ingest_static, replay, validate_gtfs,
)
from data_horizon.config import Settings
from data_horizon.contracts import timestamp
from data_horizon.processor import aggregate_spark, create_spark
from data_horizon.store import Store, dumps

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = Path(os.getenv("RECORDED_MANIFEST_FILE", str(ROOT / "reports/dataset-manifest.json"))).resolve()
LIVE_EVIDENCE = Path(os.getenv("RECORDED_EVIDENCE_FILE", str(ROOT / "reports/live-source-evidence.json"))).resolve()
# Verified as link 1 on https://www.mta.info/developers on 14 November 2026.
SUPPLEMENTED_URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"


def digest(path):
    checksum = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def resolve_input(path):
    target = Path(path)
    return target if target.is_absolute() else ROOT / target


def evidence(case_id, status, reason, *, basis="recorded", level="integration", details=None):
    records = json.loads(LIVE_EVIDENCE.read_text()) if LIVE_EVIDENCE.exists() else []
    records.append({
        "case_id": case_id, "status": status, "reason": reason, "data_basis": basis,
        "level": level, "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(MANIFEST.relative_to(ROOT)), "details": details or {},
    })
    LIVE_EVIDENCE.parent.mkdir(exist_ok=True)
    LIVE_EVIDENCE.write_text(json.dumps(records, indent=2) + "\n")


def freeze_capture():
    """Freeze content-addressed capture identities without modifying the live DB."""
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    database = ROOT / ".local-data/live/horizon.db"
    if not database.exists():
        pytest.skip("BLOCKED: no genuine live capture is available to freeze")
    connection = sqlite3.connect("file:" + str(database) + "?mode=ro", uri=True)
    try:
        metadata = {key: json.loads(payload) for key, payload in connection.execute(
            "SELECT key,payload FROM metadata WHERE namespace='live'"
        )}
        graph = [json.loads(row[0]) for row in connection.execute(
            "SELECT payload FROM assets WHERE namespace='live' AND kind='edges' ORDER BY item_key"
        )]
    finally:
        connection.close()
    static = metadata.get("static", {})
    transit = metadata.get("sources", {}).get("transit", {})
    if not static.get("path") or not transit.get("manifest", {}).get("path"):
        pytest.skip("BLOCKED: genuine schedule and successfully decoded transit capture are required")
    selected = [
        ("static", static),
        ("realtime", transit["manifest"]),
    ]
    for kind, folder in [("alerts", "service-alerts"), ("weather", "weather")]:
        candidates = []
        for path in (ROOT / ".local-data/live/raw/live" / folder).rglob("*.manifest.json"):
            record = json.loads(path.read_text())
            content = resolve_input(record["path"])
            try:
                if kind == "alerts":
                    decode_feed(content.read_bytes(), Settings(mode="recorded"))
                else:
                    assert "properties" in json.loads(content.read_text())
            except (AssertionError, ValueError, UnicodeDecodeError):
                continue
            candidates.append(record)
        if candidates:
            selected.append((kind, max(candidates, key=lambda item: item["retrieved_at"])))
    inputs = []
    for kind, record in selected:
        path = resolve_input(record["path"])
        calculated = digest(path)
        if calculated != record["sha256"]:
            raise AssertionError(f"Captured input checksum mismatch for {kind}")
        inputs.append({
            "kind": kind, "path": str(path.relative_to(ROOT)), "sha256": calculated,
            "size_bytes": path.stat().st_size, "source_url": record.get("source_url"),
            "retrieved_at": record.get("retrieved_at"), "data_basis": "recorded",
        })
    schedule = resolve_input(static["schedule_path"])
    inputs.append({
        "kind": "schedule_lookup", "path": str(schedule.relative_to(ROOT)), "sha256": digest(schedule),
        "size_bytes": schedule.stat().st_size, "static_version": static["static_version"],
        "data_basis": "recorded",
    })
    instant = datetime.now(timezone.utc)
    result = {
        "schema_version": 1, "frozen_at_utc": instant.isoformat(),
        "frozen_at_ist": instant.astimezone(ZoneInfo("Asia/Kolkata")).isoformat(),
        "data_basis": "recorded", "city": "nyc", "routes": ["1", "2", "3", "4", "5", "6"],
        "inputs": inputs, "static_version": static["static_version"],
        "source_quality_at_capture": transit.get("quality", {}),
        "captured_processing": metadata.get("processing", {}),
        "weather_at_capture": metadata.get("weather", {}),
        "graph": {"edge_count": len(graph), "sha256": hashlib.sha256(dumps(graph).encode()).hexdigest()},
        "limitations": [
            "One short genuine capture does not establish representative service performance",
            "Public arrival estimates remain forecasts rather than measured arrivals",
            "Recorded Spark parity is not Kafka transport, failure-recovery or scaling evidence",
        ],
    }
    MANIFEST.parent.mkdir(exist_ok=True)
    MANIFEST.write_text(json.dumps(result, indent=2) + "\n")
    return result


@pytest.fixture(scope="session")
def frozen_capture():
    if os.environ.get("RUN_RECORDED") != "1":
        pytest.skip("BLOCKED: RUN_RECORDED=1 and frozen genuine input are required")
    manifest = freeze_capture()
    for item in manifest["inputs"]:
        assert digest(resolve_input(item["path"])) == item["sha256"]
    return manifest


def input_path(capture, kind):
    return resolve_input(next(item["path"] for item in capture["inputs"] if item["kind"] == kind))


@pytest.fixture(scope="session")
def recorded_events(frozen_capture):
    settings = Settings(mode="recorded")
    matcher = ScheduleMatcher(input_path(frozen_capture, "schedule_lookup"))
    events, alerts, quality = decode_feed(
        input_path(frozen_capture, "realtime").read_bytes(), settings, matcher=matcher,
        static_version=frozen_capture["static_version"],
    )
    return events, alerts, quality


@pytest.fixture
def isolated_recorded(monkeypatch):
    directory = ROOT / ".local-data/verification"
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="recorded-", dir=directory) as folder:
        monkeypatch.setenv("DATA_DIR", folder)
        monkeypatch.setenv("DATA_MODE", "recorded")
        monkeypatch.setenv("APP_ENV", "test")
        for name in ("DATABASE_URL", "DATABASE_URL_FILE", "KAFKA_BOOTSTRAP_SERVERS"):
            monkeypatch.delenv(name, raising=False)
        yield Path(folder)


@pytest.mark.case("F003-TC08")
@pytest.mark.evidence("recorded", "integration")
def test_genuine_recorded_quality_counts_reconcile(frozen_capture, recorded_events):
    from google.transit import gtfs_realtime_pb2

    events, _, quality = recorded_events
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(input_path(frozen_capture, "realtime").read_bytes())
    candidates = 0
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.route_id in frozen_capture["routes"]:
            update = entity.trip_update
            candidates += len(update.stop_time_update)
            if update.trip.schedule_relationship == 3 and not update.stop_time_update:
                candidates += 1
        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            if vehicle.trip.route_id in frozen_capture["routes"] and vehicle.trip.trip_id and vehicle.stop_id:
                candidates += 1
    assert quality["entities"] == len(feed.entity)
    assert quality["accepted"] == len(events)
    assert quality["accepted"] + quality["rejected"] == candidates
    assert quality["matched"] + quality["tentative"] + quality["unmatched"] == len(events)
    assert events and all(item["source"] != "synthetic" for item in events)
    assert {item["time_basis"] for item in events}.issubset({"predicted", "inferred", "unavailable"})
    actual = sum(item["time_basis"] == "observed" and item["match_confidence"] == "exact" for item in events)
    evidence("F003-TC08", "Passed", "Genuine captured Protobuf quality counts reconcile to decoded candidates",
             details={**quality, "verified_actual_observations": actual})


@pytest.mark.case("F006-TC08")
@pytest.mark.evidence("recorded", "integration")
def test_genuine_graph_rebuild_is_reproducible(frozen_capture, isolated_recorded):
    body = input_path(frozen_capture, "static").read_bytes()
    store = Store()
    first = ingest_static(body, store)
    first_graph = store.graph_edges()
    second = ingest_static(body, store)
    second_graph = store.graph_edges()
    assert first["static_version"] == second["static_version"] == frozen_capture["static_version"]
    assert first_graph == second_graph
    assert len(first_graph) == frozen_capture["graph"]["edge_count"] > 0
    assert all(item["travel_seconds"] >= 0 for item in first_graph)
    assert hashlib.sha256(dumps(first_graph).encode()).hexdigest() == frozen_capture["graph"]["sha256"]


@pytest.mark.case("F002-TC07")
@pytest.mark.evidence("recorded", "integration")
def test_two_official_schedule_products_preserve_original_event_version(
    frozen_capture, recorded_events, isolated_recorded,
):
    cached = list((ROOT / ".local-data/verified-static").rglob("*.manifest.json"))
    supplement = next((json.loads(path.read_text()) for path in cached
                       if json.loads(path.read_text()).get("source_url") == SUPPLEMENTED_URL), None)
    if supplement is None:
        if os.environ.get("RUN_LIVE") != "1":
            pytest.skip("BLOCKED: a second genuine supplemented schedule capture requires RUN_LIVE=1")
        settings = Settings(mode="recorded", data_dir=ROOT / ".local-data/verified-static",
                            static_url=SUPPLEMENTED_URL)
        content = fetch(SUPPLEMENTED_URL, max_bytes=64 * 1024 * 1024)
        validate_gtfs(content)
        supplement = archive_response(settings, "gtfs-supplemented", content, source_url=SUPPLEMENTED_URL)
    supplement_path = resolve_input(supplement["path"])
    assert digest(supplement_path) == supplement["sha256"]
    store = Store()
    regular = ingest_static(input_path(frozen_capture, "static").read_bytes(), store)
    store.apply_batch(recorded_events[0][:1], batch_id="before-schedule-refresh")
    other_settings = Settings(mode="recorded", data_dir=isolated_recorded, static_url=SUPPLEMENTED_URL)
    supplemented = ingest_static(supplement_path.read_bytes(), store, other_settings)
    assert regular["static_version"] != supplemented["static_version"]
    assert Path(regular["schedule_path"]).exists() and Path(supplemented["schedule_path"]).exists()
    assert store.events()[0]["static_version"] == regular["static_version"]
    snapshot = {
        "kind": "supplemented_static", "path": str(supplement_path.relative_to(ROOT)),
        "sha256": supplement["sha256"], "size_bytes": supplement_path.stat().st_size,
        "source_url": SUPPLEMENTED_URL, "retrieved_at": supplement["retrieved_at"], "data_basis": "recorded",
        "source_discovery": "Link 1 on https://www.mta.info/developers",
        "limitation": "Different official schedule products, not a claim of two chronological hourly updates",
    }
    if not any(item["sha256"] == snapshot["sha256"] for item in frozen_capture["inputs"]):
        frozen_capture["inputs"].append(snapshot)
        MANIFEST.write_text(json.dumps(frozen_capture, indent=2) + "\n")


@pytest.mark.spark
@pytest.mark.feature("F005")
@pytest.mark.evidence("recorded", "integration")
def test_genuine_recorded_spark_matches_reference(recorded_events):
    if os.environ.get("RUN_SPARK") != "1":
        pytest.skip("BLOCKED: RUN_SPARK=1 with the documented Java runtime is required")
    events = recorded_events[0]
    instant = max(timestamp(item["event_time"]) for item in events)
    expected = aggregate(events, as_of=instant)
    spark = create_spark("DataHorizonRecordedParity")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        actual = aggregate_spark(spark, events, as_of=instant)
    finally:
        spark.stop()
    assert len(actual) == len(expected) > 0
    for received, wanted in zip(actual, expected):
        assert received.keys() == wanted.keys()
        for name in wanted:
            if isinstance(wanted[name], float):
                assert received[name] == pytest.approx(wanted[name], rel=1e-9, abs=1e-9)
            else:
                assert received[name] == wanted[name]


@pytest.mark.case("F004-TC02", "F004-TC03")
@pytest.mark.evidence("recorded", "integration")
def test_recorded_replay_preserves_event_time_and_canonical_output(
    recorded_events, isolated_recorded, monkeypatch,
):
    events = [item for item in recorded_events[0] if item.get("predicted_time") and not item.get("cancelled")]
    latest = max(timestamp(item["event_time"]) for item in events)
    by_route = defaultdict(list)
    for item in sorted(events, key=lambda row: (row["route_id"], row["stop_id"], row["trip_id"])):
        if timestamp(item["event_time"]) >= latest - timedelta(minutes=5):
            by_route[item["route_id"]].append(item)
    subset = [item for route in sorted(by_route) for item in by_route[route][:5]]
    assert 1 <= len(subset) <= 30
    path = isolated_recorded / "recorded-events.jsonl"
    path.write_text("\n".join(dumps(row) for row in subset) + "\n")
    outcomes = []
    for number, speed in enumerate((1.0, 1000.0)):
        folder = isolated_recorded / f"run-{number}"
        folder.mkdir()
        monkeypatch.setenv("DATA_DIR", str(folder))
        settings = Settings(mode="recorded", data_dir=folder)
        waits = []
        result = replay(path, settings, speed=speed, sleep=waits.append)
        store = Store()
        rows = store.events()
        canonical = sorted(
            [{key: value for key, value in row.items() if key not in {"event_id", "replay_run_id", "ingested_at"}}
             for row in rows], key=lambda row: (row["route_id"], row["stop_id"], row["trip_id"])
        )
        outcomes.append((result, canonical, aggregate(rows, as_of=latest), waits))
    assert outcomes[0][0]["run_id"] != outcomes[1][0]["run_id"]
    assert outcomes[0][1] == outcomes[1][1]
    assert outcomes[0][2] == outcomes[1][2]
    assert Counter(row["event_time"] for row in outcomes[0][1]) == Counter(row["event_time"] for row in subset)
    assert sum(outcomes[0][3]) == pytest.approx(sum(outcomes[1][3]) * 1000)
    # Pacing waits were injected; this verifies pacing arithmetic, not throughput.
    # PostgreSQL/Kafka transport and restart recovery require their own tests.


@pytest.mark.live
@pytest.mark.case("F001-TC06")
@pytest.mark.evidence("live", "live")
def test_current_live_feed_and_weather_fetch_decode_archive():
    if os.environ.get("RUN_LIVE") != "1":
        pytest.skip("BLOCKED: RUN_LIVE=1 is required for public-source requests")
    folder = ROOT / ".local-data/verified-live"
    settings = Settings(mode="live", data_dir=folder)
    try:
        raw = fetch(settings.realtime_url)
        events, _, quality = decode_feed(raw, settings)
        feed_manifest = archive_response(settings, "verified-realtime", raw, source_url=settings.realtime_url)
        weather = fetch(settings.weather_url)
        properties = json.loads(weather)["properties"]
        weather_manifest = archive_response(settings, "verified-weather", weather, source_url=settings.weather_url)
    except OSError as exc:
        evidence("F001-TC06", "Blocked", f"Public-source access failed: {type(exc).__name__}",
                 basis="live", level="live")
        pytest.skip(f"BLOCKED: current public-source access failed: {type(exc).__name__}")
    assert quality["entities"] > 0 and events
    assert feed_manifest["sha256"] == digest(resolve_input(feed_manifest["path"]))
    assert weather_manifest["sha256"] == digest(resolve_input(weather_manifest["path"]))
    assert properties.get("timestamp") and "temperature" in properties
    assert quality["feed_timestamp"]
    evidence("F001-TC06", "Passed", "Current genuine MTA and NWS responses fetched, decoded and archived",
             basis="live", level="live", details={
                 "transit_manifest": feed_manifest, "weather_manifest": weather_manifest,
                 "quality": quality, "weather_observed_at": properties["timestamp"],
                 "limitation": "Source connectivity is not app/Kafka readiness or actual-delay ground truth",
             })

