"""Independent calendar/timetable fixtures; these are synthetic, not Live accuracy evidence."""
from datetime import datetime, timezone
import hashlib
import io
from pathlib import Path
import zipfile

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs

from data_horizon.analytics import aggregate
from data_horizon.collector import ScheduleMatcher, decode_feed, ingest_static, refresh_static
from data_horizon.config import Settings
from data_horizon.contracts import service_time
from data_horizon.store import Store

URL = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip'
REGULAR = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip'
TRIP = '090000_4..N01R'
CALENDAR = ('service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n'
            'weekday,1,1,1,1,1,0,0,20260101,20261231\n'
            'weekend,0,0,0,0,0,1,1,20260101,20261231\n')
pytestmark = [pytest.mark.case('F001-COL01', 'F003-COL01', 'F005-COL01', 'F007-COL02'),
              pytest.mark.evidence('synthetic', 'contract')]


def archive(*, calendar=CALENDAR, exceptions=None, validity=None, trips=None, times=None):
    trips = trips or [('WK_' + TRIP, 'weekday', '0'), ('WE_' + TRIP, 'weekend', '0')]
    times = times or [('A', '10:00:00'), ('B', '10:05:00'), ('C', '10:10:00')]
    files = {
        'routes.txt': 'route_id,agency_id,route_short_name,route_type\n4,MTA NYCT,4,1\n',
        'stops.txt': 'stop_id,stop_name,stop_lat,stop_lon\nA,Alpha,40,-74\nB,Beta,40.1,-74\nC,Gamma,40.2,-74\n',
        'trips.txt': 'route_id,service_id,trip_id,direction_id\n' + ''.join(
            f'4,{service},{trip},{direction}\n' for trip, service, direction in trips),
        'stop_times.txt': 'trip_id,arrival_time,departure_time,stop_id,stop_sequence\n' + ''.join(
            f'{trip},{value},{value},{stop},{index}\n'
            for trip, _, _ in trips for index, (stop, value) in enumerate(times, 1)),
    }
    if calendar is not None:
        files['calendar.txt'] = calendar
    if exceptions is not None:
        files['calendar_dates.txt'] = 'service_id,date,exception_type\n' + exceptions
    if validity is not None:
        files['feed_info.txt'] = 'feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date\n' + validity
    result = io.BytesIO()
    with zipfile.ZipFile(result, 'w') as out:
        for name, content in files.items():
            out.writestr(name, content)
    return result.getvalue()


@pytest.fixture
def schedule_context(isolated_data_dir, monkeypatch):
    monkeypatch.setenv('MIN_FREE_GB', '0')
    settings = Settings(mode='synthetic', data_dir=isolated_data_dir, static_url=URL)
    return Store(), settings


def matcher_for(schedule_context, **kwargs):
    store, settings = schedule_context
    report = ingest_static(archive(**kwargs), store, settings)
    return ScheduleMatcher(report['schedule_path']), report


def match(matcher, *, trip=TRIP, day='2026-11-19', route='4', direction='0', stops=('A', 'B', 'C'), stop='B'):
    return matcher.match(trip, stop, day, route_id=route, direction_id=direction, remaining_stop_ids=stops)


def test_active_day_selects_one_documented_complete_suffix(schedule_context):
    matcher, report = matcher_for(schedule_context)
    assert match(matcher) == ('2026-11-19T15:05:00+00:00', 'exact')
    assert match(matcher, day='2026-11-21') == ('2026-11-21T15:05:00+00:00', 'exact')
    assert report['lookup_schema_version'] == 2
    assert report['source_profile'] == 'mta-nyct-supplemented-v1'
    assert report['calendar_path'] != report['schedule_path']
    assert report['calendar_sha256'] == hashlib.sha256(Path(report['calendar_path']).read_bytes()).hexdigest()


def test_calendar_exceptions_override_weekly_service(schedule_context):
    matcher, _ = matcher_for(schedule_context, exceptions='weekday,20261119,2\nweekend,20261119,1\n')
    assert match(matcher)[1] == 'exact'
    # Removing the only normally active service yields no comparison, not the first inactive candidate.
    matcher, _ = matcher_for(schedule_context, exceptions='weekday,20261119,2\n')
    assert match(matcher) == (None, 'unmatched')


def test_calendar_dates_only_addition_is_supported(schedule_context):
    matcher, _ = matcher_for(schedule_context, calendar=None, exceptions='added,20261119,1\n',
                             trips=[('ADDED_' + TRIP, 'added', '0')])
    assert match(matcher)[1] == 'exact'
    assert match(matcher, day='2026-11-20') == (None, 'unmatched')


@pytest.mark.parametrize('changes', [
    {'day': None}, {'route': None}, {'direction': None}, {'direction': '9'}, {'direction': '1'},
    {'route': '5'}, {'trip': '090000_4..N'}, {'trip': '090000_4..N99R'},
    {'stops': None}, {'stops': ()}, {'stops': ('A', 'C', 'B')}, {'stops': ('A', 'unknown')},
    {'stops': ('A', 'B', 'B')}, {'stop': 'unknown'}, {'stops': ('A', 'C')}, {'stops': ('A', 'B')},
])
def test_missing_or_conflicting_identity_never_becomes_exact(schedule_context, changes):
    matcher, _ = matcher_for(schedule_context)
    assert match(matcher, **changes)[1] != 'exact'


def test_unverified_source_profile_cannot_promote_a_suffix(schedule_context):
    store, settings = schedule_context
    settings.static_url = REGULAR
    report = ingest_static(archive(trips=[('WK_' + TRIP, 'weekday', '0')]), store, settings)
    assert match(ScheduleMatcher(report['schedule_path']))[1] == 'tentative'


def test_two_active_full_suffix_candidates_are_ambiguous(schedule_context):
    matcher, _ = matcher_for(schedule_context, trips=[('ONE_' + TRIP, 'weekday', '0'), ('TWO_' + TRIP, 'weekday', '0')])
    assert match(matcher) == (None, 'ambiguous')


def test_repeated_static_stop_is_not_an_unambiguous_match(schedule_context):
    matcher, _ = matcher_for(schedule_context, times=[('A', '10:00:00'), ('B', '10:05:00'), ('A', '10:10:00')])
    assert match(matcher, stops=('A', 'B'))[1] != 'exact'


def test_feed_validity_and_extended_hour_service_day(schedule_context):
    matcher, _ = matcher_for(schedule_context, validity='MTA,https://www.mta.info,en,20261119,20261120\n',
                             times=[('A', '25:00:00'), ('B', '25:05:00'), ('C', '25:10:00')])
    assert match(matcher) == ('2026-11-20T06:05:00+00:00', 'exact')
    assert match(matcher, day='2026-11-21') == (None, 'unmatched')


@pytest.mark.parametrize('day,value', [('2026-03-08', '02:30:00'), ('2026-11-01', '01:30:00')])
def test_dst_ambiguous_or_nonexistent_schedule_stays_unavailable(schedule_context, day, value):
    matcher, _ = matcher_for(schedule_context, times=[('A', value), ('B', value), ('C', value)])
    assert match(matcher, day=day) == (None, 'ambiguous')


@pytest.mark.parametrize('changes', [
    {'calendar': CALENDAR + 'weekday,1,1,1,1,1,0,0,20260101,20261231\n'},
    {'calendar': CALENDAR.replace('weekday,1,', 'weekday,2,')},
    {'calendar': CALENDAR.replace('20261231', '20250230')},
    {'calendar': CALENDAR.replace('weekday,', 'other,')},
    {'exceptions': 'weekday,20261119,1\nweekday,20261119,2\n'},
    {'exceptions': 'weekday,20261119,9\n'},
    {'validity': 'MTA,https://www.mta.info,en,20261120,20261119\n'},
])
def test_malformed_calendar_cannot_replace_active_assets(schedule_context, changes):
    store, settings = schedule_context
    _, old = matcher_for(schedule_context)
    before = store.assets('edges')
    with pytest.raises((ValueError, KeyError)):
        ingest_static(archive(**changes), store, settings)
    assert store.get_metadata('static') == old
    assert store.assets('edges') == before


def test_tampered_calendar_or_schedule_cannot_load(schedule_context):
    matcher, report = matcher_for(schedule_context)
    calendar = Path(report['calendar_path'])
    old = calendar.read_bytes()
    calendar.write_bytes(old + b' ')
    with pytest.raises(ValueError):
        ScheduleMatcher(report['schedule_path'])
    calendar.write_bytes(old)
    schedule = Path(report['schedule_path'])
    schedule.write_bytes(schedule.read_bytes() + b'{}\n')
    with pytest.raises(ValueError):
        ScheduleMatcher(schedule)
    assert match(matcher)[1] == 'exact'  # Already loaded version is immutable in memory.


def test_source_switch_forces_immediate_refresh_and_failure_keeps_old_profile(schedule_context, monkeypatch):
    store, settings = schedule_context
    settings.static_url = REGULAR
    old = ingest_static(archive(trips=[('WK_' + TRIP, 'weekday', '0')]), store, settings)
    cached = ScheduleMatcher(old['schedule_path'])
    settings.static_url = URL
    calls = []
    def fail(*args, **kwargs):
        calls.append(args[0])
        raise OSError('injected network failure')
    monkeypatch.setattr('data_horizon.collector.fetch', fail)
    assert refresh_static(store, settings, cached) is cached
    assert calls == [URL]
    assert store.get_metadata('static') == old
    assert store.get_metadata('sources')['static']['status'] == 'degraded'
    assert match(cached)[1] == 'tentative'


def test_full_native_decode_uses_published_schedule_but_keeps_forecast_basis(schedule_context):
    matcher, _ = matcher_for(schedule_context)
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = '2.0'
    feed.header.timestamp = int(datetime(2026, 11, 19, 15, tzinfo=timezone.utc).timestamp())
    update = feed.entity.add(id='native').trip_update
    update.trip.trip_id, update.trip.route_id, update.trip.start_date = TRIP, '4', '20261119'
    for index, stop in enumerate(('A', 'B', 'C')):
        update.stop_time_update.add(stop_id=stop).arrival.time = int(service_time('2026-11-19', f'10:{index * 5:02}:30').timestamp())
    events, _, quality = decode_feed(feed.SerializeToString(), schedule_context[1], matcher=matcher)
    assert quality['matched'] == 3
    assert all(event['time_basis'] == 'predicted' and event['observed_time'] is None for event in events)
    rows = aggregate(events, as_of=datetime(2026, 11, 19, 15, 0, 1, tzinfo=timezone.utc))
    assert rows and all(row['delay_mean_seconds'] == 30 for row in rows)
    update.trip.ClearField('start_date')
    events, _, quality = decode_feed(feed.SerializeToString(), schedule_context[1], matcher=matcher)
    assert quality['matched'] == 0


def test_source_switch_publishes_new_bound_artifacts_without_removing_old_files(schedule_context, monkeypatch):
    store, settings = schedule_context
    settings.static_url = REGULAR
    body = archive()
    old = ingest_static(body, store, settings)
    old_files = {key: Path(old[key]).read_bytes() for key in ('schedule_path', 'calendar_path')}
    settings.static_url = URL
    monkeypatch.setattr('data_horizon.collector.fetch', lambda *args, **kwargs: body)
    replacement = refresh_static(store, settings, ScheduleMatcher(old['schedule_path']))
    new = store.get_metadata('static')
    assert new['static_version'] == old['static_version']  # Same archive does not reuse a wrong source profile.
    assert new['schedule_path'] != old['schedule_path']
    assert new['source_url'] == URL and match(replacement)[1] == 'exact'
    assert all(Path(old[key]).read_bytes() == value for key, value in old_files.items())


def test_calendar_publication_failure_never_replaces_active_metadata_or_graph(schedule_context, monkeypatch):
    from data_horizon.collector import atomic_write

    store, settings = schedule_context
    previous, old = matcher_for(schedule_context)
    before = store.assets('edges')
    body = archive(times=[('A', '10:00:00'), ('B', '10:06:00'), ('C', '10:10:00')])
    def interrupted(path, content):
        if path.name.endswith('.calendar-v2.json'):
            raise OSError('injected calendar publication failure')
        return atomic_write(path, content)
    monkeypatch.setattr('data_horizon.collector.atomic_write', interrupted)
    with pytest.raises(OSError):
        ingest_static(body, store, settings)
    assert store.get_metadata('static') == old and store.assets('edges') == before
    assert match(previous)[0] == '2026-11-19T15:05:00+00:00'


def test_fresh_schema_mismatch_is_refreshed_immediately(schedule_context, monkeypatch):
    store, settings = schedule_context
    previous, report = matcher_for(schedule_context)
    store.put_metadata('static', {**report, 'lookup_schema_version': 1})
    calls = []
    def transport(*args, **kwargs):
        calls.append(args[0])
        return archive()
    monkeypatch.setattr('data_horizon.collector.fetch', transport)
    refresh_static(store, settings, previous)
    assert calls == [URL] and store.get_metadata('static')['lookup_schema_version'] == 2
