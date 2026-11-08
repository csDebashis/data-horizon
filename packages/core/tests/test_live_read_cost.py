"""Routine status and latest readings avoid unrelated history payload work."""
from datetime import datetime, timedelta, timezone

import pytest

from data_horizon.store import Store, dumps

pytestmark = [pytest.mark.case('F004-COL01', 'F007-COL02'),
              pytest.mark.evidence('synthetic', 'contract')]
NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)


def test_status_snapshot_and_network_read_metadata_without_table_counts(isolated_data_dir, monkeypatch):
    store = Store()
    store.put_metadata('processing', {'as_of': NOW.isoformat(), 'time_basis': 'predicted'})
    store.put_metadata('sources', {'transit': {'status': 'ok'}})
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: NOW + timedelta(seconds=30))
    original = store.execute
    def without_counts(conn, sql, args=()):
        assert 'COUNT(' not in sql.upper(), sql
        return original(conn, sql, args)
    monkeypatch.setattr(store, 'execute', without_counts)
    snapshot = store.status_snapshot()
    assert snapshot['as_of'] == NOW.isoformat() and snapshot['freshness_seconds'] == 30
    assert snapshot['stale'] is False and snapshot['sources']['transit']['status'] == 'ok'
    assert 'counts' not in snapshot
    assert store.network_health()['stale'] is False


def test_pipeline_keeps_exact_counts_and_same_source_status(isolated_data_dir, monkeypatch):
    store = Store()
    store.put_metadata('processing', {'as_of': NOW.isoformat(), 'time_basis': 'predicted'})
    store.quarantine({'invalid': True}, 'fixture')
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: NOW)
    detailed = store.pipeline_status()
    assert detailed['counts'] == {'events': 0, 'metrics': 0, 'quarantine': 1, 'batches': 0}
    assert {key: value for key, value in detailed.items() if key != 'counts'} == store.status_snapshot()


def test_latest_selection_ranks_keys_then_fetches_winning_payloads_including_old_stops(isolated_data_dir, monkeypatch):
    store = Store()
    with store.connect() as conn:
        for stop, age in [('old', 86400), ('fresh', 0)]:
            for revision in range(3):
                instant = NOW - timedelta(seconds=age + revision * 60)
                row = {'route_id': '1', 'direction_id': '0', 'stop_id': stop,
                       'window_start': instant.isoformat(), 'as_of': instant.isoformat(),
                       'source_as_of': instant.isoformat(), 'health_score': None, 'revision': revision}
                store.execute(conn, 'INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)',
                    (store.mode, f'{stop}-{revision}', '1', stop, '0', instant.isoformat(), dumps(row)))
    original = store.execute
    selected = []
    def capture(conn, sql, args=()):
        if 'ROW_NUMBER()' in sql:
            selected.append(sql)
        return original(conn, sql, args)
    monkeypatch.setattr(store, 'execute', capture)
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: NOW)
    rows = store.latest_metrics()
    assert {row['stop_id']: row['revision'] for row in rows} == {'old': 0, 'fresh': 0}
    assert len(selected) == 1
    ranked_projection = selected[0].rsplit('SELECT', 1)[1].split('ROW_NUMBER()', 1)[0]
    assert 'payload' not in ranked_projection.lower(), selected[0]
