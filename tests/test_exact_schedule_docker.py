"""CHG-0082 actual Spark SQL checks inside the built processor image."""
import json
import os

import pytest

from test_docker_acceptance import deployment as deployment

pytestmark = [
    pytest.mark.integration, pytest.mark.spark,
    pytest.mark.case('F003-COL02', 'F005-COL01', 'F007-COL01'),
    pytest.mark.evidence('synthetic', 'integration'),
    pytest.mark.skipif(os.getenv('RUN_DOCKER') != '1', reason='Needs the explicitly launched current synthetic stack'),
]

SCRIPT = r'''
import json
import math
from datetime import datetime, timedelta, timezone
from data_horizon.analytics import aggregate
from data_horizon.contracts import TransitEvent
from data_horizon.processor import aggregate_spark, create_spark

now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)

def forecast(trip, minutes, stop, confidence='exact', delay=120, headway=None):
    arrival = now - timedelta(minutes=minutes)
    return TransitEvent(
        source='synthetic', route_id='1', direction_id='0', stop_id=stop, trip_id=trip,
        event_time=arrival, predicted_time=arrival,
        scheduled_time=arrival - timedelta(seconds=delay), time_basis='predicted',
        match_confidence=confidence, scheduled_headway_seconds=headway,
    ).model_dump(mode='json')

def equivalent(actual, expected, path='result'):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), path + ': fields'
        for key in expected:
            equivalent(actual[key], expected[key], path + '.' + key)
    elif isinstance(expected, list):
        assert len(actual) == len(expected), path + ': count'
        for index, (left, right) in enumerate(zip(actual, expected)):
            equivalent(left, right, path + '[' + str(index) + ']')
    elif isinstance(expected, float):
        assert actual is not None and math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9), path
    else:
        assert actual == expected, path

rows, unknown_stops, exact_stops = [], [], []
for confidence in ('tentative', 'unmatched', 'ambiguous'):
    for headway in (None, 300):
        stop = confidence + ('-derived' if headway is None else '-declared')
        unknown_stops.append(stop)
        rows.extend(forecast(stop + str(index), 1 - index, stop, confidence, -17280, headway)
                    for index in range(2))
for headway in (None, 60):
    suffix = '-derived' if headway is None else '-declared'
    for kind in ('control', 'mixed'):
        stop = kind + suffix
        exact_stops.append(stop)
        rows.extend(forecast(stop + str(index), 3 - index, stop, headway=headway)
                    for index in range(2))
        if kind == 'mixed':
            rows.extend(forecast(stop + '-guess' + str(index), 1 - index, stop, 'tentative', -17280, 9999)
                        for index in range(2))
rows += [forecast('revised', 3, 'revision'), forecast('other', 2, 'revision'),
         forecast('revised', 1, 'revision', 'tentative', -17280, 9999)]

spark = create_spark('DataHorizonExactSchedule82')
spark.sparkContext.setLogLevel('ERROR')
try:
    actual = aggregate_spark(spark, rows, as_of=now)
    equivalent(actual, aggregate(rows, as_of=now))
    results = {row['stop_id']: row for row in actual}
    assert len(results) == 11
    for stop in unknown_stops:
        row = results[stop]
        assert row['event_count'] == 2 and row['time_basis'] == 'predicted'
        assert row['headway_seconds'] == 60
        for field in ('delay_mean_seconds', 'delay_stddev_seconds', 'scheduled_headway_seconds',
                      'headway_deviation', 'health_score'):
            assert row[field] is None, stop + ': ' + field
        for minutes in (5, 15, 30):
            assert row['rolling_' + str(minutes) + 'm_mean'] is None
            assert row['rolling_' + str(minutes) + 'm_stddev'] is None
        assert row['health_level'] == 'unknown' and row['anomaly'] is None
    for stop in exact_stops:
        row = results[stop]
        assert row['delay_mean_seconds'] == 120 and row['delay_stddev_seconds'] == 0
        assert row['headway_seconds'] == 60 and row['scheduled_headway_seconds'] == 60
        assert row['headway_deviation'] == 0 and row['health_score'] == 88
        assert row['event_count'] == (4 if stop.startswith('mixed') else 2)
        for minutes in (5, 15, 30):
            assert row['rolling_' + str(minutes) + 'm_mean'] == 120
            assert row['rolling_' + str(minutes) + 'm_stddev'] == 0
    revision = results['revision']
    assert revision['event_count'] == 2 and revision['headway_seconds'] == 60
    assert revision['delay_mean_seconds'] == 120
    assert revision['scheduled_headway_seconds'] is None and revision['headway_deviation'] is None
    assert revision['health_score'] is None and revision['health_level'] == 'unknown'
    print('DH_ACCEPTANCE_JSON=' + json.dumps({
        'passed': True, 'spark_version': spark.version, 'scenario_groups': len(results),
        'untrusted_groups': len(unknown_stops), 'exact_and_mixed_groups': len(exact_stops),
        'latest_revision_checked': True, 'python_spark_parity': True,
        'source_basis': 'synthetic fixtures; no measured-arrival or prediction-accuracy claim',
    }), flush=True)
finally:
    spark.stop()
'''


def test_real_spark_requires_exact_timetable_for_delay_and_schedule_comparisons(deployment, record_testsuite_property):
    response = deployment.application_script(SCRIPT, spark=True, timeout=300)
    result = deployment.json_output(response)
    assert result['passed'] and result['python_spark_parity']
    assert result['scenario_groups'] == 11 and result['latest_revision_checked']
    record_testsuite_property('exact_schedule_spark_result', json.dumps(result, sort_keys=True))
