"""Cold API readers reconstruct retained health without an application cache or writes."""
from datetime import datetime, timedelta, timezone
import secrets

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app
from data_horizon.contracts import TransitEvent
from data_horizon.store import Store

pytestmark = [pytest.mark.case('F007-COL02', 'F010-COL01'),
              pytest.mark.evidence('synthetic', 'integration')]


def test_cold_network_api_keeps_current_null_and_returns_original_last_valid(isolated_data_dir, monkeypatch):
    base = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
    clock = {'now': base}
    monkeypatch.setenv('DATA_MODE', 'live')
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('PUBLIC_API_TOKEN', secrets.token_urlsafe(32))
    analytics = secrets.token_urlsafe(32)
    monkeypatch.setenv('API_TOKEN', analytics)
    for name in ('PUBLIC_API_TOKEN_FILE', 'API_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: clock['now'])
    store = Store()
    store.replace_static([{'route_id': '1'}], [{'stop_id': 'A', 'route_ids': ['1']}], [],
        metadata={'static_version': 'a' * 64, 'source_profile': 'mta-nyct-supplemented-v1',
                  'source_url': store.settings.static_url})
    store.put_metadata('sources', {'alerts': {'status': 'ok', 'last_success': base.isoformat()}})
    events = [TransitEvent(source='synthetic', route_id='1', direction_id='0', stop_id='A', trip_id=f'run{index}',
        event_time=base, scheduled_time=base + timedelta(seconds=seconds),
        predicted_time=base + timedelta(seconds=seconds + 120), time_basis='predicted', match_confidence='exact',
        static_version='a' * 64)
        for index, seconds in enumerate((60, 360))]
    store.apply_batch(events, query_id='grace-api-fixture', batch_id='0', as_of=base)
    clock['now'] = base + timedelta(seconds=180)
    before = store.metrics()
    observed = []
    for _ in range(2):
        reader = Store(ensure_schema=False)
        original = reader.execute
        def read_only(conn, sql, args=()):
            assert sql.lstrip().upper().startswith('SELECT'), sql
            return original(conn, sql, args)
        monkeypatch.setattr(reader, 'execute', read_only)
        with TestClient(create_app(reader)) as client:
            result = client.get('/api/v1/network/health', headers={'Authorization': 'Bearer ' + analytics})
            assert result.status_code == 200
            data = result.json()['data']
            assert data['health_score'] is None and data['stale'] is True
            retained = data['last_valid_health']
            assert retained['score'] == 88 and retained['source_as_of'] == base.isoformat()
            assert retained['age_seconds'] == 180 and retained['retention_seconds'] == 300
            assert data['routes'][0]['last_valid_health']['score'] == 88
            observed.append(retained)
    assert observed[0] == observed[1] and store.metrics() == before
