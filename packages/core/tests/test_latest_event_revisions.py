"""Reducing forecast revisions preserves independent current and rolling calculations."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from data_horizon import analytics
from data_horizon.contracts import TransitEvent, timestamp

pytestmark = [pytest.mark.case('F004-COL01', 'F005-COL01'),
              pytest.mark.evidence('synthetic', 'contract')]
NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)


def event(trip, age, revision=0, **changes):
    when = NOW - timedelta(seconds=age)
    values = dict(source='synthetic', route_id='1', direction_id='0', stop_id='A',
        trip_id=trip, event_id=f'{trip}-{revision:03}', event_time=when, observed_time=when,
        scheduled_time=when - timedelta(seconds=revision), time_basis='observed',
        match_confidence='exact', static_version='a' * 64)
    values.update(changes)
    return TransitEvent(**values).model_dump(mode='json')


def revision_rows():
    rows = [event(f'trip{trip}', (trip % 29) * 60 + (39 - revision) * 0.5, revision)
            for trip in range(100) for revision in range(40)]
    rows += [event('tie', 60, 10, event_id='z-latest'), event('tie', 60, 99, event_id='a-old'),
             event('future', 60, 1), event('future', -1, 9), event('left', 1800, 1)]
    rows += [event('cancelled', 120, 1), event('cancelled', 60, 2, stop_id='__trip__', cancelled=True,
             time_basis='unavailable', observed_time=None, scheduled_time=None, match_confidence='unmatched')]
    rows += [event('tentative', 120, 1), event('tentative', 60, 9, time_basis='predicted',
             observed_time=None, predicted_time=NOW, match_confidence='tentative')]
    return rows


def test_reduction_keeps_latest_rows_and_all_reference_outputs_without_mutating_input():
    rows = revision_rows()
    before = deepcopy(rows)
    helper = getattr(analytics, 'latest_event_revisions', None)
    assert callable(helper), 'Missing shared latest-revision reduction'
    reduced = helper(rows, as_of=NOW)
    assert len(reduced) == 104 < len(rows) // 30
    assert rows == before
    assert analytics.aggregate(reduced, as_of=NOW) == analytics.aggregate(rows, as_of=NOW)
    by_trip = {row['trip_id']: row for row in reduced}
    assert by_trip['tie']['event_id'] == 'z-latest'
    assert timestamp(by_trip['future']['event_time']) == NOW - timedelta(seconds=60)
    assert by_trip['cancelled']['cancelled'] is True
    assert 'left' not in by_trip


def test_all_cancelled_entities_and_platform_direction_keys_remain_present():
    rows = [event('same', 120, 1), event('same', 60, 2, cancelled=True),
            event('same', 60, 3, stop_id='B'), event('same', 60, 4, direction_id='1')]
    helper = getattr(analytics, 'latest_event_revisions', None)
    assert callable(helper), 'Missing shared latest-revision reduction'
    reduced = helper(rows, as_of=NOW)
    assert len(reduced) == 3
    expected = analytics.aggregate(rows, as_of=NOW)
    assert analytics.aggregate(reduced, as_of=NOW) == expected
    cancelled = next(row for row in expected if row['stop_id'] == 'A' and row['direction_id'] == '0')
    assert cancelled['event_count'] == 0 and cancelled['health_score'] is None
