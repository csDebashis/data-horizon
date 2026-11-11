"""Last-valid scores are separate, clock-bound and reconstructed without API writes."""
from datetime import datetime, timedelta, timezone
import json

import pytest

from data_horizon.contracts import TransitEvent
from data_horizon.analytics import aggregate
from data_horizon.health_retention import scope_for
from data_horizon.store import Store, dumps

BASE = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
STATIC = {'static_version': 'a' * 64, 'source_profile': 'mta-nyct-supplemented-v1',
          'source_url': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip'}
pytestmark = [pytest.mark.case('F007-COL01', 'F007-COL02'),
              pytest.mark.evidence('synthetic', 'contract')]


@pytest.fixture
def health_store(isolated_data_dir, monkeypatch):
    monkeypatch.setenv('DATA_MODE', 'live')
    clock = {'now': BASE}
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: clock['now'])
    store = Store()
    store.replace_static([{'route_id': '1'}, {'route_id': '2'}], [
        {'stop_id': stop, 'route_ids': [route]} for route, stop in [('1', 'A'), ('1', 'B'), ('2', 'C')]], [], metadata=STATIC)
    sequence = [0]

    def write(entities=(('1', 'A', 120),), at=BASE):
        clock['now'] = at
        store.put_metadata('sources', {'alerts': {'status': 'ok', 'last_success': at.isoformat()},
            'transit': {'manifest': {'source_url': store.settings.realtime_url}}})
        rows = []
        for route, stop, delay in entities:
            for trip, gap in enumerate((60, 360)):
                scheduled = at + timedelta(seconds=gap)
                rows.append(TransitEvent(source='synthetic', route_id=route, stop_id=stop,
                    direction_id='0', trip_id=f'{route}-{stop}-{trip}', event_time=at,
                    predicted_time=scheduled + timedelta(seconds=delay), scheduled_time=scheduled,
                    time_basis='predicted', match_confidence='exact', static_version=STATIC['static_version']))
        sequence[0] += 1
        store.apply_batch(rows, query_id='health-grace-fixture', batch_id=str(sequence[0]), as_of=at)
        return store.metrics(route_id=entities[0][0], stop_id=entities[0][1], limit=1)[0]

    return store, clock, write


def alter_all_rows(store, transform):
    with store.connect() as conn:
        rows = store.execute(conn, 'SELECT item_key,payload FROM analytics_result WHERE namespace=?', (store.mode,)).fetchall()
        for row in rows:
            payload = transform(json.loads(row['payload']))
            store.execute(conn, 'UPDATE analytics_result SET payload=? WHERE namespace=? AND item_key=?',
                          (dumps(payload), store.mode, row['item_key']))


def test_writer_stamps_bound_scope_and_current_summary_original_clock(health_store):
    store, _, write = health_store
    row = write()
    scope = row['health_scope']
    assert scope['mode'] == 'live' and scope['static_version'] == STATIC['static_version']
    assert scope['source_profile'] == STATIC['source_profile']
    assert scope['realtime_source'] == store.settings.realtime_url
    assert scope['configured_alerts_source'] == store.settings.alerts_url
    assert scope['feature_version'] == 'v1' and scope['scoring_version'] == 'health-v2'
    assert scope['time_basis'] == 'predicted'
    summary = store.network_health()
    assert row['health_score'] == summary['health_score'] == 88
    assert summary['health_source_as_of'] == BASE.isoformat()
    assert summary['health_age_seconds'] == 0
    assert summary['health_retention_eligible'] is True
    assert summary.get('last_valid_health') is None


@pytest.mark.parametrize('age,retained', [(299, True), (300, True), (301, False)])
def test_entity_grace_uses_original_source_clock_and_expires(health_store, age, retained):
    store, clock, write = health_store
    write()
    clock['now'] = BASE + timedelta(seconds=age)
    row = Store().latest_metrics()[0]  # Cold reader; no browser or in-process cache.
    assert row['health_score'] is None and row['stale'] is True
    fallback = row.get('last_valid_health')
    if retained:
        assert fallback['score'] == 88 and fallback['source_as_of'] == BASE.isoformat()
        assert fallback['age_seconds'] == age and fallback['retention_seconds'] == 300
    else:
        assert fallback is None
    assert store.anomalies() == []


def test_brief_alert_gap_retains_old_score_without_changing_current_semantics(health_store):
    store, clock, write = health_store
    write()
    store.put_metadata('sources', {'alerts': {'status': 'degraded',
        'last_success': (BASE - timedelta(seconds=400)).isoformat()}})
    clock['now'] = BASE + timedelta(seconds=30)
    row = store.latest_metrics()[0]
    assert row['health_score'] is None and row['stale'] is False
    assert 'service-alert' in row['reason']
    assert row['last_valid_health']['score'] == 88
    clock['now'] += timedelta(seconds=1)
    assert store.latest_metrics()[0]['last_valid_health']['age_seconds'] == 31
    assert store.metrics()[0]['health_score'] == 88  # Stored arithmetic/history is not rewritten.


def test_summary_deduplicates_snapshots_and_preserves_route_weighting(health_store):
    store, clock, write = health_store
    write((('1', 'A', 120), ('1', 'B', 240), ('2', 'C', 600)))
    write((('1', 'A', 0),), BASE + timedelta(seconds=60))
    clock['now'] = BASE + timedelta(seconds=190)
    summary = store.network_health()
    assert summary['health_score'] is None
    routes = {r['route_id']: r for r in summary['routes']}
    assert routes['1']['last_valid_health']['score'] == 88  # Mean of100 and76, once each.
    assert routes['2']['last_valid_health']['score'] == 40
    fallback = summary['last_valid_health']
    assert fallback['score'] == 64  # Mean of the two route scores, not all three entities.
    assert fallback['entity_count'] == 3 and fallback['route_count'] == 2
    assert fallback['source_as_of'] == BASE.isoformat() and fallback['age_seconds'] == 190


def test_current_score_wins_and_provenance_uses_oldest_current_contributor(health_store):
    store, clock, write = health_store
    write((('1', 'A', 120), ('1', 'B', 240), ('2', 'C', 600)))
    write((('1', 'A', 0),), BASE + timedelta(seconds=60))
    clock['now'] = BASE + timedelta(seconds=70)
    summary = store.network_health()
    assert summary['health_score'] == 64 and summary.get('last_valid_health') is None
    assert summary['health_source_as_of'] == BASE.isoformat() and summary['health_age_seconds'] == 70


@pytest.mark.parametrize('field,value', [('static_version', 'b' * 64), ('source_profile', 'different'),
    ('realtime_source', 'https://api-endpoint.mta.info/different'), ('mode', 'synthetic'),
    ('feature_version', 'v2'), ('scoring_version', 'health-v1'), ('time_basis', 'observed')])
def test_scope_mismatch_cannot_be_retained(health_store, field, value):
    store, clock, write = health_store
    write()
    def change(row):
        row['health_scope'][field] = value
        return row
    alter_all_rows(store, change)
    clock['now'] = BASE + timedelta(seconds=180)
    assert store.latest_metrics()[0].get('last_valid_health') is None
    assert store.network_health().get('last_valid_health') is None


def test_unmarked_history_and_never_calculated_health_are_not_backfilled(health_store):
    store, clock, write = health_store
    write()
    alter_all_rows(store, lambda row: {key: value for key, value in row.items() if key != 'health_scope'})
    fresh = store.network_health()
    assert fresh['health_score'] == 88 and fresh['health_retention_eligible'] is False
    clock['now'] = BASE + timedelta(seconds=180)
    assert store.latest_metrics()[0].get('last_valid_health') is None
    # A matching scope is insufficient when no valid numerical score ever existed.
    clock['now'] = BASE
    write()
    alter_all_rows(store, lambda row: {**row, 'health_score': None, 'headway_deviation': None})
    assert store.latest_metrics()[0].get('last_valid_health') is None


@pytest.mark.parametrize('clock_field', ['source_as_of', 'as_of'])
@pytest.mark.parametrize('value', [None, 'bad-clock', '2026-11-19T12:00:00', '2026-11-19T13:00:00Z'])
def test_invalid_or_future_candidate_clock_is_not_retained(health_store, clock_field, value):
    store, clock, write = health_store
    first = write()
    alter_all_rows(store, lambda row: {**row, clock_field: value})
    current = {**first, 'as_of': (BASE + timedelta(seconds=60)).isoformat(),
        'window_start': (BASE + timedelta(seconds=60)).isoformat(),
        'source_as_of': (BASE + timedelta(seconds=60)).isoformat(),
        'health_score': None, 'headway_deviation': None}
    with store.connect() as conn:
        store.execute(conn, 'INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)',
            (store.mode, 'current-null', '1', 'A', '0', current['window_start'], dumps(current)))
    clock['now'] = BASE + timedelta(seconds=90)
    assert store.latest_metrics()[0].get('last_valid_health') is None


def test_retention_reads_are_bounded_once_and_do_not_write(health_store, monkeypatch):
    store, clock, write = health_store
    write((('1', 'A', 120), ('1', 'B', 240), ('2', 'C', 600)))
    clock['now'] = BASE + timedelta(seconds=180)
    original = store.execute
    queries = []
    def read_only(conn, sql, args=()):
        assert sql.lstrip().upper().startswith('SELECT'), sql
        queries.append((sql, args))
        return original(conn, sql, args)
    monkeypatch.setattr(store, 'execute', read_only)
    summary = store.network_health()
    histories = [(sql, args) for sql, args in queries if 'analytics_result' in sql and 'LIMIT ?' in sql]
    assert len(histories) == 1 and histories[0][1][-1] <= 10000
    assert summary['last_valid_health']['entity_count'] == 3


def test_active_static_source_change_and_namespace_cannot_reuse_previous_scores(health_store, monkeypatch):
    store, clock, write = health_store
    write()
    clock['now'] = BASE + timedelta(seconds=180)
    store.put_metadata('static', {**STATIC, 'static_version': 'b' * 64})
    assert store.latest_metrics()[0].get('last_valid_health') is None
    monkeypatch.setenv('DATA_MODE', 'synthetic')
    other = Store()
    assert other.latest_metrics() == [] and other.network_health().get('last_valid_health') is None


def test_processing_gap_can_expose_only_separate_retained_summary(health_store):
    store, clock, write = health_store
    write()
    clock['now'] = BASE + timedelta(seconds=30)
    processing = store.get_metadata('processing')
    store.put_metadata('processing', {**processing, 'as_of': (BASE - timedelta(seconds=400)).isoformat()})
    summary = store.network_health()
    assert summary['stale'] is True and summary['health_score'] is None
    assert summary['routes'][0]['health_score'] is None
    assert summary['last_valid_health']['score'] == 88 and summary['last_valid_health']['age_seconds'] == 30
    assert store.latest_metrics()[0]['health_score'] == 88  # Existing entity semantics are unchanged.


def test_source_age_boundary_is_not_rounded_to_the_snapshot_minute(health_store):
    store, clock, write = health_store
    source = BASE + timedelta(seconds=30)
    write(at=source)
    clock['now'] = source + timedelta(seconds=300)
    retained = store.network_health()['last_valid_health']
    assert retained['source_as_of'] == source.isoformat() and retained['age_seconds'] == 300


def test_configured_realtime_source_change_invalidates_retention(health_store):
    store, clock, write = health_store
    write()
    store.settings.realtime_url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/different'
    clock['now'] = BASE + timedelta(seconds=180)
    assert store.latest_metrics()[0].get('last_valid_health') is None


def test_configured_alert_source_change_invalidates_retention(health_store):
    store, clock, write = health_store
    write()
    store.settings.alerts_url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/different-alerts'
    clock['now'] = BASE + timedelta(seconds=180)
    assert store.latest_metrics()[0].get('last_valid_health') is None
    assert store.network_health().get('last_valid_health') is None


def timetable_event(trip, gap, version, *, at=BASE, cancelled=False, stop='A', event_id=''):
    scheduled = BASE + timedelta(seconds=gap)
    return TransitEvent(source='synthetic', route_id='1', stop_id=stop, direction_id='0',
        trip_id=trip, event_id=event_id, event_time=at, scheduled_time=scheduled,
        predicted_time=scheduled + timedelta(seconds=120), time_basis='predicted',
        match_confidence='exact', static_version=version, cancelled=cancelled)


def apply_contribution_fixture(store, rows, *, custom=False):
    store.put_metadata('sources', {'alerts': {'status': 'ok', 'last_success': BASE.isoformat()}})
    callback = None
    if custom:
        def callback(events, **kwargs):
            result = aggregate(events, **kwargs)
            # The store must verify contributors even if a callback supplies a marker.
            for row in result:
                row['health_scope'] = scope_for(store.settings, store.mode, STATIC, {}, row)
            return result
    store.apply_batch(rows, query_id='contributing-timetable', batch_id='0', as_of=BASE,
                      aggregate_fn=callback)
    return store.metrics()[0]


@pytest.mark.parametrize('custom', [False, True], ids=['reference', 'custom'])
@pytest.mark.parametrize('versions,eligible', [
    (('b' * 64, 'b' * 64), False),
    ((STATIC['static_version'], 'b' * 64), False),
    ((STATIC['static_version'], None), False),
    ((STATIC['static_version'], ''), False),
    ((STATIC['static_version'], STATIC['static_version']), True),
], ids=['old-only', 'mixed', 'missing', 'empty', 'all-active'])
def test_retention_marker_requires_every_current_contributor_matches_static(health_store, custom, versions, eligible):
    store, clock, _ = health_store
    row = apply_contribution_fixture(store, [
        timetable_event(f'trip-{index}', 60 + 300 * index, version)
        for index, version in enumerate(versions)], custom=custom)
    assert row['health_score'] == 88  # The current calculation is unchanged.
    assert ('health_scope' in row) is eligible
    current = store.network_health()
    assert current['health_score'] == 88 and current['health_retention_eligible'] is eligible
    clock['now'] = BASE + timedelta(seconds=180)
    fallback = store.network_health().get('last_valid_health')
    assert (fallback is not None) is eligible
    if eligible:
        assert row['health_scope']['scoring_version'] == 'health-v2'
        assert fallback['score'] == 88 and fallback['source_as_of'] == BASE.isoformat()


@pytest.mark.parametrize('control', ['stop', 'trip'])
def test_cancelled_old_version_does_not_block_active_contributors(health_store, control):
    store, clock, _ = health_store
    rows = [timetable_event('first', 60, STATIC['static_version']),
            timetable_event('second', 360, STATIC['static_version'])]
    rows.append(timetable_event('cancelled', 660, 'b' * 64, cancelled=control == 'stop'))
    if control == 'trip':
        rows.append(timetable_event('cancelled', 660, None, stop='__trip__', cancelled=True))
    row = apply_contribution_fixture(store, rows)
    assert row['health_score'] == 88 and row['health_scope']['scoring_version'] == 'health-v2'
    clock['now'] = BASE + timedelta(seconds=180)
    assert store.network_health()['last_valid_health']['score'] == 88


@pytest.mark.parametrize('old_offset', [-1, 0])
def test_superseded_old_revisions_do_not_block_active_contributors(health_store, old_offset):
    store, _, _ = health_store
    rows = [timetable_event('first', 60, 'b' * 64, at=BASE + timedelta(seconds=old_offset), event_id='a-old'),
            timetable_event('first', 60, STATIC['static_version'], event_id='z-current'),
            timetable_event('second', 360, STATIC['static_version'])]
    row = apply_contribution_fixture(store, rows)
    assert row['health_score'] == 88 and row['health_scope']['scoring_version'] == 'health-v2'


def test_no_uncancelled_contributors_cannot_receive_retention_marker(health_store):
    store, _, _ = health_store
    row = apply_contribution_fixture(store, [timetable_event('cancelled', 60, STATIC['static_version'], cancelled=True)])
    assert row['health_score'] is None and 'health_scope' not in row
