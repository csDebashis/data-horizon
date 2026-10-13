from datetime import timedelta

import pytest

from data_horizon.analytics import aggregate
from data_horizon.contracts import TransitEvent, utcnow

pytestmark = pytest.mark.feature("F005")


def test_native_selectors_are_and_clauses_with_or_alternatives():
    from data_horizon.alert_scope import alert_matches

    alert = {"informed_entities": [{"route_id": "1", "stop_id": "A"}, {"route_id": "2", "stop_id": "B"}]}
    for route, stop, expected in (("1", "A", True), ("2", "B", True), ("1", "B", False), ("2", "A", False)):
        assert alert_matches(alert, {"route_id": route, "stop_id": stop}) is expected
    assert alert_matches(alert, {"route_id": "1"}, allow_partial=True)
    assert not alert_matches(alert, {"route_id": "1"})


def test_trip_direction_unknown_fields_and_legacy_conjunction_fail_safely():
    from data_horizon.alert_scope import alert_matches

    scoped = {"informed_entities": [{"route_id": "1", "direction_id": 0, "trip": {"trip_id": "run-A"}}]}
    assert alert_matches(scoped, {"route_id": "1", "direction_id": "0", "trip_id": "run-A"})
    assert not alert_matches(scoped, {"route_id": "1"}, allow_partial=True)
    assert not alert_matches(scoped, {"route_id": "1", "direction_id": "1", "trip_id": "run-A"})
    assert not alert_matches({"informed_entities": [{"unknown_restriction": "x"}]}, {}, allow_partial=True)
    assert not alert_matches({"informed_entities": [{}]}, {}, allow_partial=True)
    legacy = {"route_ids": ["1"], "stop_ids": ["A"]}
    assert alert_matches(legacy, {"route_id": "1", "stop_id": "A"})
    assert not alert_matches(legacy, {"route_id": "2", "stop_id": "A"})
    assert not alert_matches(legacy, {"route_id": "1", "stop_id": "B"})


def test_reference_station_alert_counts_do_not_cross_selector_alternatives():
    now = utcnow()
    rows = []
    for route, stop in (("1", "A"), ("1", "B"), ("2", "A"), ("2", "B")):
        for index in range(2):
            instant = now - timedelta(minutes=5 * index)
            rows.append(
                TransitEvent(
                    source="synthetic",
                    route_id=route,
                    stop_id=stop,
                    trip_id=f"trip-{route}-{stop}-{index}",
                    event_time=now,
                    observed_time=instant,
                    scheduled_time=instant,
                    time_basis="observed",
                    match_confidence="exact",
                    scheduled_headway_seconds=300,
                ).model_dump(mode="json")
            )
    alert = {
        "severity": 1,
        "informed_entities": [{"route_id": "1", "stop_id": "A"}, {"route_id": "2", "stop_id": "B"}],
    }
    result = aggregate(rows, as_of=now, alerts=[alert])
    assert {(row["route_id"], row["stop_id"]): row["alert_count"] for row in result} == {
        ("1", "A"): 1,
        ("1", "B"): 0,
        ("2", "A"): 0,
        ("2", "B"): 1,
    }
    assert {(row["route_id"], row["stop_id"]): row["health_score"] for row in result} == {
        ("1", "A"): 90,
        ("1", "B"): 100,
        ("2", "A"): 100,
        ("2", "B"): 90,
    }
