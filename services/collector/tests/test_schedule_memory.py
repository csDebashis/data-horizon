"""Refresh must stream lookups while retaining the previous usable matcher."""
from pathlib import Path

import pytest

from data_horizon.collector import ScheduleMatcher, ingest_static
from data_horizon.config import Settings
from data_horizon.store import Store, dumps
from test_schedule_matching import URL, archive, match

pytestmark = [pytest.mark.case('F001-COL02', 'F003-COL02'),
              pytest.mark.evidence('synthetic', 'contract')]


def deny_whole_schedule_reads(monkeypatch, schedule_path):
    original = Path.read_bytes

    def bounded_read(path):
        if path == schedule_path or path.name.endswith('.schedule-v2.jsonl'):
            raise AssertionError('A schedule must be hashed and parsed as a stream')
        return original(path)

    monkeypatch.setattr(Path, 'read_bytes', bounded_read)


@pytest.mark.parametrize('versioned', [True, False])
def test_schedule_loading_never_reads_the_whole_jsonl(isolated_data_dir, monkeypatch, versioned):
    monkeypatch.setenv('MIN_FREE_GB', '0')
    if versioned:
        settings = Settings(mode='synthetic', data_dir=isolated_data_dir, static_url=URL)
        report = ingest_static(archive(), Store(), settings)
        path = Path(report['schedule_path'])
    else:
        path = isolated_data_dir / 'legacy.schedule.jsonl'
        path.write_text(dumps({'trip_id': 'literal', 'stops': [[1, 'A', '10:05:00']]}) + '\n')
    deny_whole_schedule_reads(monkeypatch, path)
    matcher = ScheduleMatcher(path)
    if versioned:
        assert match(matcher) == ('2026-11-19T15:05:00+00:00', 'exact')
    else:
        assert matcher.match('literal', 'A', '2026-11-19') == ('2026-11-19T15:05:00+00:00', 'exact')


def test_repeated_import_retains_old_matcher_without_whole_lookup_comparison(isolated_data_dir, monkeypatch):
    monkeypatch.setenv('MIN_FREE_GB', '0')
    settings = Settings(mode='synthetic', data_dir=isolated_data_dir, static_url=URL)
    store = Store()
    content = archive()
    first = ingest_static(content, store, settings)
    retained = ScheduleMatcher(first['schedule_path'])
    deny_whole_schedule_reads(monkeypatch, Path(first['schedule_path']))
    second = ingest_static(content, store, settings)
    replacement = ScheduleMatcher(second['schedule_path'])
    assert second['schedule_sha256'] == first['schedule_sha256']
    assert second['calendar_sha256'] == first['calendar_sha256']
    assert match(retained) == match(replacement) == ('2026-11-19T15:05:00+00:00', 'exact')
    assert store.get_metadata('static') == second


def test_oversized_calendar_is_rejected_even_with_a_matching_checksum(isolated_data_dir, monkeypatch):
    import hashlib
    import json
    from data_horizon.schedule import MAX_CALENDAR_BYTES

    monkeypatch.setenv('MIN_FREE_GB', '0')
    settings = Settings(mode='synthetic', data_dir=isolated_data_dir, static_url=URL)
    report = ingest_static(archive(), Store(), settings)
    calendar_path, schedule_path = Path(report['calendar_path']), Path(report['schedule_path'])
    body = calendar_path.read_bytes()
    body += b' ' * (MAX_CALENDAR_BYTES + 1 - len(body))  # Valid JSON but outside the input limit.
    calendar_path.write_bytes(body)
    header, rows = schedule_path.read_bytes().split(b'\n', 1)
    header = {**json.loads(header), 'calendar_sha256': hashlib.sha256(body).hexdigest()}
    schedule_path.write_bytes(dumps(header).encode() + b'\n' + rows)
    with pytest.raises(ValueError, match='Calendar lookup exceeds byte limit'):
        ScheduleMatcher(schedule_path)


@pytest.mark.parametrize('change', ['truncate', 'append', 'modify'])
def test_streamed_immutable_comparison_rejects_changed_lookup_bytes(isolated_data_dir, monkeypatch, change):
    monkeypatch.setenv('MIN_FREE_GB', '0')
    settings = Settings(mode='synthetic', data_dir=isolated_data_dir, static_url=URL)
    store = Store()
    content = archive()
    old = ingest_static(content, store, settings)
    retained = ScheduleMatcher(old['schedule_path'])
    path = Path(old['schedule_path'])
    body = path.read_bytes()
    mutations = {'truncate': body[:-1], 'append': body + b' ', 'modify': b' ' + body[1:]}
    path.write_bytes(mutations[change])
    deny_whole_schedule_reads(monkeypatch, path)
    with pytest.raises(ValueError, match='Existing immutable timetable lookup differs'):
        ingest_static(content, store, settings)
    assert store.get_metadata('static') == old
    assert match(retained) == ('2026-11-19T15:05:00+00:00', 'exact')
