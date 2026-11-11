"""Genuine Spark SQL parity checks; synthetic fixtures do not certify live data."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

import pytest

from data_horizon.analytics import aggregate
from data_horizon.contracts import TransitEvent
from data_horizon.processor import aggregate_spark, create_spark, validate_spatial

pytestmark = [pytest.mark.spark, pytest.mark.integration,
              pytest.mark.evidence("synthetic", "integration"), pytest.mark.feature("F005")]
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    if os.environ.get("RUN_SPARK") != "1":
        pytest.skip("RUN_SPARK=1 with the documented Java/PySpark runtime is required")
    session = create_spark("DataHorizonSparkParity")
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def event(trip, minutes=5, delay=120, **changes):
    arrival = NOW - timedelta(minutes=minutes)
    values = {
        "source": "synthetic", "route_id": "1", "direction_id": "0", "stop_id": "A",
        "trip_id": trip, "event_time": arrival, "observed_time": arrival,
        "scheduled_time": arrival - timedelta(seconds=delay),
        "time_basis": "observed", "match_confidence": "exact",
        "scheduled_headway_seconds": 300,
    }
    values.update(changes)
    return TransitEvent(**values).model_dump(mode="json")


def assert_equivalent(actual, expected):
    """Compare independently computed values, including missing/basis metadata."""
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for name in expected:
            assert_equivalent(actual[name], expected[name])
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for received, wanted in zip(actual, expected):
            assert_equivalent(received, wanted)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-9, abs=1e-9)
    else:
        assert actual == expected


@pytest.mark.case("F005-TC01", "F005-TC03", "F005-TC04")
def test_real_spark_delay_headway_and_direction_separation(spark):
    rows = [event("a", 10, 60), event("b", 5, 120),
            event("opposite", 4, 900, direction_id="1")]
    expected = aggregate(rows, as_of=NOW)
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, expected)
    assert result[0]["delay_mean_seconds"] == 90
    assert result[0]["headway_seconds"] == 300
    assert result[0]["headway_deviation"] == 0
    assert result[1]["direction_id"] == "1"
    assert result[1]["event_count"] == 1


@pytest.mark.case("F005-TC06")
def test_zero_explicit_headways_are_excluded_from_median(spark):
    rows = [event("a", 10, 0, scheduled_headway_seconds=0),
            event("b", 5, 0, scheduled_headway_seconds=300)]
    expected = aggregate(rows, as_of=NOW)
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, expected)
    assert result[0]["scheduled_headway_seconds"] == 300
    assert result[0]["headway_deviation"] == 0
    assert result[0]["health_score"] == 100


@pytest.mark.case("F003-TC07")
def test_all_cancelled_group_emits_unknown_snapshot(spark):
    before = [event("a", 10, 0), event("b", 5, 0)]
    assert aggregate(before, as_of=NOW)[0]["health_score"] == 100
    cancellations = [event(trip, 1, event_id="cancel-" + trip, cancelled=True,
                           time_basis="unavailable", observed_time=None, scheduled_time=None,
                           match_confidence="unmatched") for trip in ("a", "b")]
    expected = aggregate(before + cancellations, as_of=NOW)
    result = aggregate_spark(spark, before + cancellations, as_of=NOW)
    assert_equivalent(result, expected)
    assert len(result) == 1  # Omitting this row would retain old healthy state.
    assert result[0]["event_count"] == 0
    assert result[0]["health_score"] is None
    assert result[0]["time_basis"] == "unavailable"
    assert result[0]["health_level"] == "unknown"


@pytest.mark.case("F003-TC07")
def test_trip_level_cancellation_control_removes_previous_predictions(spark):
    before = [event("a", 10, 0), event("b", 5, 0)]
    controls = [event(trip, 1, event_id="control-" + trip, stop_id="__trip__", cancelled=True,
                      time_basis="unavailable", observed_time=None, scheduled_time=None,
                      match_confidence="unmatched") for trip in ("a", "b")]
    expected = aggregate(before + controls, as_of=NOW)
    result = aggregate_spark(spark, before + controls, as_of=NOW)
    assert_equivalent(result, expected)
    assert all(row["stop_id"] != "__trip__" for row in result)
    assert len(result) == 1
    assert result[0]["event_count"] == 0
    assert result[0]["health_score"] is None


@pytest.mark.case("F005-TC05")
def test_same_timestamp_revision_tie_breaking_matches_all_windows(spark):
    # Deliberately reverse lexical identity order to catch input-order dependence.
    rows = [event("same-trip", 1, 300, event_id="z-revision"),
            event("same-trip", 1, 60, event_id="a-revision")]
    expected = aggregate(rows, as_of=NOW)
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, expected)
    assert result[0]["event_count"] == 1
    assert result[0]["delay_mean_seconds"] == 300
    for minutes in (5, 15, 30):
        assert result[0][f"rolling_{minutes}m_mean"] == 300


@pytest.mark.case("F005-TC05")
def test_half_open_history_windows_and_future_exclusion(spark):
    rows = [event("outside", 30, 900), event("five-boundary", 5, 60),
            event("fifteen-boundary", 15, 120), event("current", 0, 180),
            event("future", -1, 900)]
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, aggregate(rows, as_of=NOW))
    assert result[0]["event_count"] == 3
    assert result[0]["rolling_5m_mean"] == 180
    assert result[0]["rolling_15m_mean"] == 120
    assert result[0]["rolling_30m_mean"] == 120


@pytest.mark.case("F005-TC02")
def test_predicted_basis_alert_filtering_and_asof_weather(spark):
    rows = [event("predicted", 1, 90, observed_time=None,
                  predicted_time=NOW - timedelta(minutes=1), time_basis="predicted")]
    alerts = [
        {"route_ids": ["1"], "severity": 1.0, "start_time": (NOW - timedelta(minutes=1)).isoformat(),
         "end_time": (NOW + timedelta(minutes=1)).isoformat()},
        {"route_ids": ["1"], "severity": 1.0, "end_time": (NOW - timedelta(seconds=1)).isoformat()},
        {"route_ids": ["2"], "severity": 1.0},
        {"route_ids": ["1"], "severity": 1.0,
         "available_at": (NOW + timedelta(seconds=1)).isoformat()},
    ]
    weather = {"temperature_c": 17, "observed_at": (NOW - timedelta(hours=1)).isoformat(),
               "retrieved_at": (NOW + timedelta(seconds=1)).isoformat()}
    result = aggregate_spark(spark, rows, as_of=NOW, alerts=alerts, weather=weather)
    assert_equivalent(result, aggregate(rows, as_of=NOW, alerts=alerts, weather=weather))
    assert result[0]["time_basis"] == "predicted"
    assert result[0]["alert_count"] == 1
    assert result[0]["weather_temperature_c"] is None


@pytest.mark.case("F005-TC09")
def test_native_station_selector_alternatives_do_not_cross_product(spark):
    rows = [event(f"{route}-{stop}-{minutes}", minutes, 0, route_id=route, stop_id=stop)
            for route in ("1", "2") for stop in ("A", "B") for minutes in (5, 10)]
    alerts = [{"id": "paired", "severity": 1.0,
               "informed_entities": [{"route_id": "1", "stop_id": "A"}, {"route_id": "2", "stop_id": "B"}]}]
    result = aggregate_spark(spark, rows, as_of=NOW, alerts=alerts)
    assert_equivalent(result, aggregate(rows, as_of=NOW, alerts=alerts))
    assert {(row["route_id"], row["stop_id"]): row["alert_count"] for row in result} == {
        ("1", "A"): 1, ("1", "B"): 0, ("2", "A"): 0, ("2", "B"): 1}
    assert {(row["route_id"], row["stop_id"]): row["health_score"] for row in result} == {
        ("1", "A"): 90, ("1", "B"): 100, ("2", "A"): 100, ("2", "B"): 90}


@pytest.mark.case("F005-TC09")
def test_native_trip_direction_static_agency_and_unsupported_constraints(spark):
    rows = [event("run-A", 5, 0, service_date="2026-11-14"), event("run-B", 10, 0)]
    alerts = [
        {"id": "exact-trip", "informed_entities": [{"route_id": "1", "direction_id": 0,
             "trip": {"trip_id": "run-A", "start_date": "20261114"}}]},
        {"id": "agency", "informed_entities": [{"agency_id": "MTA", "route_type": 1}]},
        {"id": "wrong-trip", "informed_entities": [{"trip": {"trip_id": "absent"}}]},
        {"id": "unknown", "informed_entities": [{"route_id": "1", "unsupported_wire_restriction": True}]},
        {"id": "parent-guess", "informed_entities": [{"stop_id": "AN"}]},
    ]
    facts = {"1": {"agency_id": "MTA", "route_type": "1"}}
    result = aggregate_spark(spark, rows, as_of=NOW, alerts=alerts, route_context=facts)
    assert_equivalent(result, aggregate(rows, as_of=NOW, alerts=alerts, route_context=facts))
    assert result[0]["alert_count"] == 2


def test_empty_and_entirely_expired_inputs(spark):
    assert aggregate_spark(spark, [], as_of=NOW) == []
    rows = [event("expired", 31)]
    assert aggregate_spark(spark, rows, as_of=NOW) == aggregate(rows, as_of=NOW) == []


@pytest.mark.case("F003-TC07")
def test_disjoint_alert_period_gap_is_not_active(spark):
    rows = [event("a", 10, 0), event("b", 5, 0)]
    alert = {
        "route_ids": ["1"], "severity": 1.0,
        "start_time": (NOW - timedelta(hours=2)).isoformat(),
        "end_time": (NOW + timedelta(hours=2)).isoformat(),
        "active_periods": [
            {"start_time": (NOW - timedelta(hours=2)).isoformat(),
             "end_time": (NOW - timedelta(hours=1)).isoformat()},
            {"start_time": (NOW + timedelta(hours=1)).isoformat(),
             "end_time": (NOW + timedelta(hours=2)).isoformat()},
        ],
    }
    result = aggregate_spark(spark, rows, as_of=NOW, alerts=[alert])
    assert_equivalent(result, aggregate(rows, as_of=NOW, alerts=[alert]))
    assert result[0]["alert_count"] == 0
    assert result[0]["health_score"] == 100


@pytest.mark.feature("F006")
@pytest.mark.case("F006-TC07")
def test_real_sedona_geometry_query(spark):
    if os.environ.get("RUN_SPATIAL") != "1":
        pytest.skip("RUN_SPATIAL=1 with matching Sedona JVM jars is required; geometry integration not executed")
    rows = validate_spatial(spark, [
        {"stop_id": "A", "stop_lon": -73.98, "stop_lat": 40.75},
        {"stop_id": "B", "stop_lon": 0.0, "stop_lat": 0.0},
    ])
    assert {r["stop_id"] for r in rows} == {"A", "B"}
    assert all(row["valid"] is True for row in rows)
    assert all(row["geometry"].startswith("POINT") for row in rows)


def forecast(trip, minutes, *, confidence='exact', delay=120, explicit_headway=None, stop='A'):
    return event(trip, minutes, delay, observed_time=None,
                 predicted_time=NOW - timedelta(minutes=minutes), time_basis='predicted',
                 match_confidence=confidence, scheduled_headway_seconds=explicit_headway, stop_id=stop)


@pytest.mark.case('F003-COL02', 'F005-COL01', 'F007-COL01', 'F009-COL01')
@pytest.mark.parametrize('explicit_headway', [None, 300])
def test_untrusted_schedule_comparisons_are_unknown_with_valid_arrival_gaps(spark, explicit_headway):
    rows = [forecast(f'{confidence}-{index}', 1 - index, confidence=confidence, delay=-17280,
                     explicit_headway=explicit_headway, stop=confidence)
            for confidence in ('tentative', 'unmatched', 'ambiguous') for index in range(2)]
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, aggregate(rows, as_of=NOW))
    assert len(result) == 3
    for row in result:
        assert row['event_count'] == 2 and row['time_basis'] == 'predicted'
        assert row['headway_seconds'] == 60
        for field in ('delay_mean_seconds', 'delay_stddev_seconds', 'scheduled_headway_seconds',
                      'headway_deviation', 'health_score'):
            assert row[field] is None
        for minutes in (5, 15, 30):
            assert row[f'rolling_{minutes}m_mean'] is None
            assert row[f'rolling_{minutes}m_stddev'] is None
        assert row['health_level'] == 'unknown' and row['anomaly'] is None


@pytest.mark.case('F003-COL02', 'F005-COL01', 'F007-COL01')
@pytest.mark.parametrize('explicit_headway', [None, 60])
def test_exact_and_mixed_forecasts_keep_exact_schedule_arithmetic(spark, explicit_headway):
    # Two groups let one Spark job compare an exact control and a mixed source.
    rows = [forecast(f'{stop}-exact-{index}', 3 - index, explicit_headway=explicit_headway, stop=stop)
            for stop in ('control', 'mixed') for index in range(2)]
    rows += [forecast(f'guessed-{index}', 1 - index, confidence='tentative', delay=-17280,
                      explicit_headway=9999, stop='mixed') for index in range(2)]
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, aggregate(rows, as_of=NOW))
    assert len(result) == 2
    for row in result:
        assert row['delay_mean_seconds'] == 120 and row['delay_stddev_seconds'] == 0
        assert row['headway_seconds'] == 60 and row['scheduled_headway_seconds'] == 60
        assert row['headway_deviation'] == 0 and row['health_score'] == 88
        for minutes in (5, 15, 30):
            assert row[f'rolling_{minutes}m_mean'] == 120
            assert row[f'rolling_{minutes}m_stddev'] == 0
    assert {row['stop_id']: row['event_count'] for row in result} == {'control': 2, 'mixed': 4}


@pytest.mark.case('F003-COL02', 'F005-COL01')
def test_latest_tentative_forecast_cannot_borrow_old_exact_comparison(spark):
    rows = [forecast('revised', 3), forecast('other', 2),
            forecast('revised', 1, confidence='tentative', delay=-17280, explicit_headway=9999)]
    result = aggregate_spark(spark, rows, as_of=NOW)
    assert_equivalent(result, aggregate(rows, as_of=NOW))
    row = result[0]
    assert row['event_count'] == 2 and row['headway_seconds'] == 60
    assert row['delay_mean_seconds'] == 120
    assert row['scheduled_headway_seconds'] is None and row['headway_deviation'] is None
    assert row['health_level'] == 'unknown' and row['health_score'] is None
