"""Regressions for event-time poisoning and independently stale entities."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest

from data_horizon.collector import synthetic_static
from data_horizon.contracts import TransitEvent
from data_horizon.store import Store, dumps

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr("data_horizon.store.utcnow", lambda: NOW)
    monkeypatch.setenv("MAX_FUTURE_SKEW_SECONDS", "120")
    return NOW


def event(at, *, trip="trip", stop="A", delay=0):
    return TransitEvent(
        source="synthetic", route_id="1", trip_id=trip, stop_id=stop,
        event_time=at, observed_time=at, scheduled_time=at - timedelta(seconds=delay),
        time_basis="observed", match_confidence="exact", scheduled_headway_seconds=300,
    ).model_dump(mode="json")


@pytest.mark.case("F003-TC01")
def test_future_event_cannot_poison_watermark_or_delete_valid_history(isolated_data_dir, clock):
    store = Store()
    store.apply_batch([event(NOW, trip="normal")], batch_id="normal")
    invalid = store.apply_batch([event(NOW + timedelta(days=365), trip="poison")], batch_id="bad-future")
    following = store.apply_batch([event(NOW + timedelta(seconds=1), trip="next")], batch_id="following")
    assert invalid["invalid"] == 1 and invalid["processed"] == 0
    assert following["processed"] == 1 and following["late"] == 0
    assert {row["trip_id"] for row in store.events()} == {"normal", "next"}
    assert store.get_metadata("processing")["as_of"] == (NOW + timedelta(seconds=1)).isoformat()
    with store.connect() as conn:
        row = store.execute(conn, "SELECT payload FROM quarantine WHERE namespace=?", (store.mode,)).fetchone()
    assert json.loads(row["payload"])["reason"] == "future_event_time"


@pytest.mark.case("F003-TC01")
def test_future_clock_allowance_boundary_and_configuration(isolated_data_dir, clock, monkeypatch):
    store = Store()
    accepted = store.apply_batch([event(NOW + timedelta(seconds=120), trip="boundary")], batch_id="boundary")
    rejected = store.apply_batch([event(NOW + timedelta(seconds=121), trip="outside")], batch_id="outside")
    assert accepted["processed"] == 1
    assert rejected["invalid"] == 1
    monkeypatch.setenv("MAX_FUTURE_SKEW_SECONDS", "300")
    changed = store.apply_batch([event(NOW + timedelta(seconds=180), trip="configured")], batch_id="configured")
    assert changed["processed"] == 1


@pytest.mark.case("F001-TC03")
@pytest.mark.parametrize("setting", ["-1", "nan", "inf", "not-a-number"])
def test_invalid_clock_allowance_does_not_change_state(isolated_data_dir, clock, monkeypatch, setting):
    store = Store()
    monkeypatch.setenv("MAX_FUTURE_SKEW_SECONDS", setting)
    with pytest.raises(ValueError, match="MAX_FUTURE_SKEW_SECONDS"):
        store.apply_batch([event(NOW)], batch_id="must-not-commit")
    assert store.events() == []
    assert store.pipeline_status()["counts"]["batches"] == 0


@pytest.mark.case("F007-TC04")
def test_rejected_only_batch_does_not_claim_source_freshness(isolated_data_dir, clock):
    store = Store()
    store.apply_batch([event(NOW + timedelta(days=1))], batch_id="only-invalid")
    status = store.pipeline_status()
    assert status["as_of"] is None
    assert status["freshness_seconds"] is None
    assert status["stale"] is True
    assert status["time_basis"] == "unavailable"


@pytest.mark.case("F007-TC05")
def test_old_entity_cannot_borrow_freshness_from_another_stop(isolated_data_dir, clock):
    store = Store()
    synthetic_static(store)
    store.apply_batch([
        event(NOW - timedelta(minutes=10), trip="old-a", stop="A"),
        event(NOW - timedelta(minutes=5), trip="old-b", stop="A"),
    ], batch_id="old")
    store.apply_batch([
        event(NOW - timedelta(minutes=5), trip="new-a", stop="B", delay=600),
        event(NOW, trip="new-b", stop="B", delay=600),
    ], batch_id="fresh-other-stop")
    rows = {row["stop_id"]: row for row in store.latest_metrics()}
    assert store.pipeline_status()["stale"] is False
    assert rows["A"]["as_of"] == NOW.isoformat()
    assert rows["A"]["source_as_of"] == (NOW - timedelta(minutes=5)).isoformat()
    assert rows["A"]["stale"] is True and rows["A"]["health_score"] is None
    assert rows["B"]["stale"] is False and rows["B"]["health_score"] == 40
    assert store.network_health()["health_score"] == 40
    assert any(row["health_score"] == 100 for row in store.metrics(stop_id="A"))


@pytest.mark.case("F007-TC05")
def test_legacy_window_time_fallback_and_unknown_time_do_not_mutate_history(isolated_data_dir, clock):
    store = Store()
    payloads = [
        {"route_id": "1", "direction_id": "0", "stop_id": "A", "window_start": NOW.isoformat(),
         "health_score": 100, "health_level": "good"},
        {"route_id": "1", "direction_id": "0", "stop_id": "B", "health_score": 100, "health_level": "good"},
    ]
    with store.connect() as conn:
        for index, row in enumerate(payloads):
            store.execute(conn,
                "INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)",
                (store.mode, str(index), "1", row["stop_id"], "0", NOW.isoformat(), dumps(row)))
    latest = {row["stop_id"]: row for row in store.latest_metrics()}
    assert latest["A"]["stale"] is False
    assert latest["A"]["source_as_of"] == NOW.isoformat()
    assert latest["B"]["stale"] is None
    assert latest["B"]["freshness_seconds"] is None
    assert latest["B"]["health_score"] is None
    assert all(row["health_score"] == 100 for row in store.metrics())


@pytest.mark.case("F007-TC04")
def test_live_health_requires_fresh_successful_alert_observation(isolated_data_dir, clock, monkeypatch):
    monkeypatch.setenv("DATA_MODE", "live")
    store = Store()
    samples = [event(NOW - timedelta(minutes=5), trip="a"), event(NOW, trip="b")]
    store.apply_batch(samples, batch_id="missing-alert-source")
    assert store.latest_metrics()[0]["health_score"] is None
    assert "service-alert" in store.latest_metrics()[0]["reason"]
    store.put_metadata("sources", {"alerts": {"status": "ok", "last_success": NOW.isoformat()}})
    store.apply_batch(samples, batch_id="source-recovered")
    assert store.latest_metrics()[0]["health_score"] == 100
    store.put_metadata("sources", {"alerts": {"status": "degraded", "last_success": (NOW - timedelta(seconds=121)).isoformat()}})
    assert store.latest_metrics()[0]["health_score"] is None
    assert store.metrics()[0]["health_score"] == 100


@pytest.mark.case("F007-TC04")
def test_recent_last_good_alert_observation_survives_temporary_source_degradation(isolated_data_dir, clock, monkeypatch):
    monkeypatch.setenv("DATA_MODE", "live")
    store = Store()
    store.put_metadata("sources", {"alerts": {"status": "degraded", "last_success": (NOW - timedelta(seconds=30)).isoformat()}})
    store.apply_batch([event(NOW - timedelta(minutes=5), trip="a"), event(NOW, trip="b")], batch_id="recent-cache")
    assert store.latest_metrics()[0]["health_score"] == 100
    assert store.pipeline_status()["sources"]["alerts"]["status"] == "degraded"


@pytest.mark.case("F005-TC05")
def test_live_snapshot_clock_does_not_relabel_old_source_as_fresh(isolated_data_dir, clock, monkeypatch):
    monkeypatch.setenv("DATA_MODE", "live")
    store = Store()
    store.put_metadata("sources", {"alerts": {"last_success": NOW.isoformat()}})
    store.apply_batch([event(NOW - timedelta(minutes=5))], batch_id="old-source")
    row = store.metrics()[0]
    assert row["as_of"] == NOW.isoformat()
    assert row["source_as_of"] == (NOW - timedelta(minutes=5)).isoformat()
    assert store.latest_metrics()[0]["stale"] is True


@pytest.mark.case("F007-TC07")
def test_current_anomalies_exclude_stale_entities_but_history_remains(isolated_data_dir, clock):
    store = Store()
    store.apply_batch([event(NOW - timedelta(minutes=5), delay=600)], batch_id="stale-anomaly")
    assert store.anomalies() == []
    assert store.metrics()[0]["anomaly"] == "high_delay"


@pytest.mark.case("F004-TC06")
def test_pipeline_exposes_publication_backlog_and_collector_failure(isolated_data_dir, clock):
    store = Store()
    store.put_metadata("kafka", {"status": "degraded", "pending": 2})
    store.put_metadata("collector_error", {"type": "TimeoutError", "at": NOW.isoformat()})
    status = store.pipeline_status()
    assert status["kafka"] == {"status": "degraded", "pending": 2}
    assert status["collector_error"]["type"] == "TimeoutError"


@pytest.mark.case("F003-TC07")
def test_served_alerts_honour_disjoint_active_periods(isolated_data_dir, clock):
    store = Store()
    store.replace_assets("alerts", [{
        "id": "split-periods", "route_ids": ["1"],
        "start_time": (NOW - timedelta(hours=2)).isoformat(),
        "end_time": (NOW + timedelta(hours=2)).isoformat(),
        "active_periods": [
            {"end_time": (NOW - timedelta(hours=1)).isoformat()},
            {"start_time": (NOW + timedelta(hours=1)).isoformat()},
        ],
    }])
    assert store.alerts() == []

