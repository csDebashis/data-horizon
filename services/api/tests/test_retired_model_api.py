"""Retired model routes must not leave a callable endpoint or status placeholder."""
import secrets

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app

pytestmark = [pytest.mark.case('F009-COL05'), pytest.mark.feature('F009'),
              pytest.mark.evidence('synthetic', 'contract')]


class TransitOnlyStore:
    def status_snapshot(self):
        return {'mode': 'synthetic', 'stale': False, 'time_basis': 'predicted'}

    def pipeline_status(self):
        return {**self.status_snapshot(), 'counts': {}}

    def routes(self):
        return [{'route_id': '1'}]

    def model_status(self):
        raise AssertionError('Retired model state must never be read')


@pytest.fixture
def transit_client(monkeypatch):
    keys = {scope: secrets.token_urlsafe(32) for scope in ('public', 'analytics')}
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('PUBLIC_API_TOKEN', keys['public'])
    monkeypatch.setenv('API_TOKEN', keys['analytics'])
    for name in ('API_TOKEN_FILE', 'PUBLIC_API_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    with TestClient(create_app(TransitOnlyStore())) as client:
        yield client, keys


@pytest.mark.parametrize('path', ['/model/status', '/predictions'])
@pytest.mark.parametrize('scope', [None, 'public', 'analytics'])
def test_retired_model_routes_are_absent_for_every_key_scope(transit_client, path, scope):
    client, keys = transit_client
    headers = {'Authorization': 'Bearer ' + keys[scope]} if scope else {}
    response = client.get('/api/v1' + path, headers=headers)
    assert response.status_code == 404


def test_openapi_has_no_model_routes_or_tags(transit_client):
    client, _ = transit_client
    schema = client.get('/openapi.json').json()
    assert '/api/v1/model/status' not in schema['paths']
    assert '/api/v1/predictions' not in schema['paths']
    assert all('Model' not in operation.get('tags', [])
               for operations in schema['paths'].values() for operation in operations.values()
               if isinstance(operation, dict))


@pytest.mark.parametrize('path,scope', [('/status', 'public'), ('/status', 'analytics'),
                                        ('/pipeline/status', 'analytics')])
def test_transit_status_does_not_read_or_project_retired_model_state(transit_client, path, scope):
    client, keys = transit_client
    response = client.get('/api/v1' + path, headers={'Authorization': 'Bearer ' + keys[scope]})
    assert response.status_code == 200
    data = response.json()['data']
    assert {'model', 'model_status', 'experiment', 'predictions'}.isdisjoint(data)
    assert data['mode'] == 'synthetic'
