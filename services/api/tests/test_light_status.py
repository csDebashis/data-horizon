"""Ordinary reads must not pay for exact Pipeline table counts."""
from types import SimpleNamespace
import secrets

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app

pytestmark = [pytest.mark.case('F010-COL01', 'F010-COL02'),
              pytest.mark.evidence('synthetic', 'contract')]


class LightStatusStore:
    settings = SimpleNamespace(stale_seconds=120)

    def __init__(self):
        self.snapshot_reads = 0
        self.count_reads = 0
        self.allow_counts = False

    def metadata(self):
        return {'mode': 'synthetic', 'as_of': '2026-11-19T12:00:00+00:00',
                'freshness_seconds': 180, 'stale': True, 'time_basis': 'predicted',
                'sources': {'alerts': {'last_success': '2026-11-19T12:00:00+00:00', 'status': 'ok'}}}

    def status_snapshot(self):
        self.snapshot_reads += 1
        return self.metadata()

    def pipeline_status(self):
        self.count_reads += 1
        assert self.allow_counts, 'Exact table counts are not part of this read'
        return {**self.metadata(), 'counts': {'events': 17, 'metrics': 3, 'quarantine': 2, 'batches': 4}}

    def routes(self):
        return [{'route_id': '1'}]

    def stops(self, route_id=None):
        return [{'stop_id': 'A', 'route_ids': ['1']}]

    def network_health(self):
        return {'health_score': None, 'routes': [{'route_id': '1', 'stop_count': 0}]}

    def latest_metrics(self):
        return []

    def metrics(self, **kwargs):
        return []

    def anomalies(self):
        return []

    def alerts(self):
        return []

    def get_metadata(self, key, default=None):
        return default

    def graph_edges(self):
        return [{'source': '1|0|A', 'target': '1|0|B', 'edge_type': 'travel', 'travel_seconds': 120}]


@pytest.fixture
def light_client(monkeypatch):
    key = secrets.token_urlsafe(32)
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('API_TOKEN', key)
    monkeypatch.delenv('API_TOKEN_FILE', raising=False)
    monkeypatch.delenv('PUBLIC_API_TOKEN_FILE', raising=False)
    store = LightStatusStore()
    with TestClient(create_app(store), headers={'Authorization': 'Bearer ' + key}) as client:
        yield store, client


@pytest.mark.parametrize('path', ['/health/ready', '/api/v1/status', '/api/v1/routes',
    '/api/v1/stops', '/api/v1/network/health', '/api/v1/routes/1/coverage',
    '/api/v1/routes/1/metrics?stop_id=A', '/api/v1/alerts', '/api/v1/anomalies'])
def test_ordinary_reads_use_one_metadata_snapshot_and_no_counts(light_client, path):
    store, client = light_client
    response = client.get(path)
    assert response.status_code == 200
    assert store.snapshot_reads == 1 and store.count_reads == 0
    if path == '/health/ready':
        assert response.json()['data_status'] == 'stale'
    else:
        assert response.json()['meta']['as_of'] == store.metadata()['as_of']
        assert response.json()['meta']['mode'] == 'synthetic'
        if path != '/api/v1/alerts':
            assert response.json()['meta']['stale'] is True
            assert response.json()['meta']['freshness_seconds'] == 180


def test_bounded_scenario_envelope_does_not_count_tables(light_client):
    store, client = light_client
    response = client.post('/api/v1/what-if', json={'route_id': '1', 'stop_id': 'A', 'delay_seconds': 600})
    assert response.status_code == 200
    assert store.snapshot_reads == 1 and store.count_reads == 0
    assert response.json()['meta']['stale'] is True


@pytest.mark.parametrize('path', ['/api/v1/pipeline/status', '/metrics'])
def test_explicit_operational_counts_remain_available(light_client, path):
    store, client = light_client
    store.allow_counts = True
    response = client.get(path)
    assert response.status_code == 200
    assert store.count_reads == 1 and store.snapshot_reads == 0
    if path == '/metrics':
        assert 'data_horizon_events_total 17' in response.text
    else:
        assert response.json()['data']['counts'] == {'events': 17, 'metrics': 3, 'quarantine': 2, 'batches': 4}


def test_metadata_failure_is_safe_without_falling_back_to_counts(light_client, monkeypatch):
    store, client = light_client
    def unavailable():
        raise ConnectionError('private-storage-diagnostic')
    monkeypatch.setattr(store, 'status_snapshot', unavailable)
    response = client.get('/api/v1/status')
    assert response.status_code == 503 and store.count_reads == 0
    assert 'private-storage-diagnostic' not in response.text
