"""CHG-0076: analytics stay usable while retired product routes disappear."""
import secrets

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app

pytestmark = [pytest.mark.case('F010-COL02'), pytest.mark.evidence('synthetic', 'contract')]


class AnalyticsStore:
    def status_snapshot(self):
        return {'mode': 'synthetic', 'stale': True}

    def pipeline_status(self):
        return {**self.status_snapshot(), 'counts': {}}


    def routes(self):
        return [{'route_id': '1'}]

    def stops(self, route_id=None):
        return [{'stop_id': 'A', 'route_ids': ['1']}]

    def network_health(self):
        return {'health_score': None, 'route_count': 1}

    def metrics(self, **kwargs):
        return []

    def anomalies(self):
        return []

    def alerts(self):
        return []

    def graph_edges(self):
        return [{'source': '1|0|A', 'target': '1|0|B', 'edge_type': 'travel', 'travel_seconds': 120, 'weight': 1}]


@pytest.fixture
def service_client(monkeypatch):
    keys = {'public': secrets.token_urlsafe(32), 'analytics': secrets.token_urlsafe(32)}
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('PUBLIC_API_TOKEN', keys['public'])
    monkeypatch.setenv('API_TOKEN', keys['analytics'])
    for name in ('API_TOKEN_FILE', 'PUBLIC_API_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    # Old protected files may still exist, but the API must no longer read accounts.
    monkeypatch.setenv('DEFAULT_BOOTSTRAP_USERS_FILE', '/unavailable/retired-user-file')
    monkeypatch.setenv('EXPERIENCE_DATABASE_URL_FILE', '/unavailable/retired-database-file')
    with TestClient(create_app(AnalyticsStore())) as client:
        yield client, keys


def headers(keys, scope):
    return {'Authorization': 'Bearer ' + keys[scope]}


@pytest.mark.parametrize('method,path', [
    ('POST', '/auth/login'), ('POST', '/auth/logout'), ('GET', '/auth/me'),
    ('POST', '/auth/register'), ('POST', '/auth/recovery-request'), ('POST', '/auth/access-request'),
    ('GET', '/me/preferences'), ('PUT', '/me/preferences'), ('GET', '/me/commutes'),
    ('GET', '/me/sessions'), ('POST', '/me/feedback'), ('GET', '/me/notification-settings'),
    ('GET', '/subscriptions'), ('POST', '/subscriptions'), ('GET', '/notifications'),
    ('POST', '/notifications/read-all'), ('GET', '/admin/users'), ('GET', '/admin/system'),
    ('GET', '/admin/accounts'), ('GET', '/admin/requests'), ('GET', '/admin/audit'),
    ('GET', '/admin/feedback'), ('GET', '/analysis/investigation'), ('GET', '/analysis/incidents'),
    ('POST', '/analysis/incidents'), ('GET', '/analysis/scenarios'), ('POST', '/analysis/scenarios'),
    ('GET', '/operations/diagnostics'), ('GET', '/journey-options'), ('GET', '/station-details'),
    ('GET', '/arrivals'), ('GET', '/commute'), ('GET', '/tracking'),
])
@pytest.mark.parametrize('scope', [None, 'public', 'analytics'])
@pytest.mark.case('F014-COL01', 'F015-COL01', 'F016-COL01')
def test_retired_routes_are_absent_for_every_service_key(service_client, method, path, scope):
    client, keys = service_client
    response = client.request(method, '/api/v1' + path, headers=headers(keys, scope) if scope else {})
    assert response.status_code == 404


@pytest.mark.parametrize('path', ['/status', '/routes', '/stops', '/network/health', '/alerts'])
def test_public_key_can_read_only_the_public_analytics_summary(service_client, path):
    client, keys = service_client
    response = client.get('/api/v1' + path, headers=headers(keys, 'public'))
    assert response.status_code == 200
    assert set(response.json()) == {'data', 'meta'}


@pytest.mark.parametrize('path', ['/routes/1/metrics', '/anomalies', '/pipeline/status'])
def test_analytics_key_is_required_for_detailed_reads(service_client, path):
    client, keys = service_client
    assert client.get('/api/v1' + path, headers=headers(keys, 'public')).status_code == 403
    assert client.get('/api/v1' + path, headers=headers(keys, 'analytics')).status_code == 200


def test_calculation_and_operational_metrics_need_analytics_key(service_client):
    client, keys = service_client
    body = {'route_id': '1', 'stop_id': 'A', 'delay_seconds': 600}
    assert client.post('/api/v1/what-if', json=body, headers=headers(keys, 'public')).status_code == 403
    result = client.post('/api/v1/what-if', json=body, headers=headers(keys, 'analytics'))
    assert result.status_code == 200 and result.json()['data']['affected_stations'][0]['stop_id'] == 'B'
    assert client.get('/metrics', headers=headers(keys, 'public')).status_code == 403
    assert client.get('/metrics', headers=headers(keys, 'analytics')).status_code == 200


def test_stale_browser_cookie_and_wrong_header_scheme_never_grant_service_access(service_client):
    client, keys = service_client
    client.cookies.set('horizon_session', keys['analytics'])
    assert client.get('/api/v1/routes').status_code == 401
    assert client.get('/api/v1/routes', headers={'Authorization': 'Basic ' + keys['analytics']}).status_code == 401


def test_origin_check_and_size_cap_still_protect_what_if(service_client):
    client, keys = service_client
    auth = headers(keys, 'analytics')
    response = client.post('/api/v1/what-if', json={'route_id': '1', 'stop_id': 'A', 'delay_seconds': 60},
                           headers={**auth, 'Origin': 'https://untrusted.example'})
    assert response.status_code == 403
    response = client.post('/api/v1/what-if', content='x' * 16385,
                           headers={**auth, 'Content-Type': 'application/json'})
    assert response.status_code == 413


def test_public_and_analytics_keys_cannot_accidentally_be_the_same(monkeypatch):
    key = secrets.token_urlsafe(32)
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('API_TOKEN', key)
    monkeypatch.setenv('PUBLIC_API_TOKEN', key)
    for name in ('API_TOKEN_FILE', 'PUBLIC_API_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match='must be different'):
        create_app(AnalyticsStore())


def test_retired_source_modules_have_no_active_imports(project_root):
    """Removing a menu must not leave hidden product code in another component."""
    import ast

    retired = {
        'auth', 'account_lifecycle', 'user_state', 'personalisation', 'notification_health',
        'notifications', 'travel_notifications', 'insights', 'insight_api', 'product_api',
        'journey_planner', 'experience', 'tracking', 'model_workflow',
    }
    source_roots = [project_root / 'packages/core/src', project_root / 'tools/operations/src']
    source_roots += [folder / 'src' for folder in (project_root / 'services').iterdir() if (folder / 'src').is_dir()]
    for source_root in source_roots:
        for filename in source_root.rglob('*.py'):
            assert filename.stem not in retired, f'Retired module still exists: {filename.relative_to(project_root)}'
            for node in ast.walk(ast.parse(filename.read_text())):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    owned = node.level > 0 or module == 'data_horizon' or module.startswith('data_horizon.')
                    if owned:
                        assert not set(module.split('.')) & retired, filename.relative_to(project_root)
                        assert not {alias.name for alias in node.names} & retired, filename.relative_to(project_root)
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith('data_horizon.'):
                            assert not set(alias.name.split('.')) & retired, filename.relative_to(project_root)
    assert not (project_root / 'services/notifier').exists()


class CoverageStore(AnalyticsStore):
    def __init__(self, now):
        from types import SimpleNamespace
        from datetime import timedelta

        self.settings = SimpleNamespace(stale_seconds=120)
        self.now = now
        self.catalogue = [
            {'stop_id': stop, 'stop_name': name, 'parent_station': stop, 'route_ids': ['1']}
            for stop, name in [('A', 'Alpha'), ('B', 'Beta'), ('C', 'Quiet platform'), ('D', 'Delta'), ('E', 'Echo')]]
        self.catalogue += [{'stop_id': 'Z', 'stop_name': 'Other route', 'route_ids': ['2']}]
        def reading(stop, age, direction='0', delay=120, window_age=0):
            return {'route_id': '1', 'stop_id': stop, 'direction_id': direction,
                    'window_start': (now - timedelta(seconds=window_age)).isoformat(),
                    'source_as_of': (now - timedelta(seconds=age)).isoformat(),
                    'delay_mean_seconds': delay, 'headway_seconds': 60, 'scheduled_headway_seconds': 60,
                    'headway_deviation': 0, 'health_score': 100 if delay == 0 else 88,
                    'health_level': 'good', 'event_count': 4, 'time_basis': 'predicted'}
        self.readings = [reading('A', 30, direction='0', delay=120, window_age=60),
                         reading('A', 20, direction='1', delay=0),
                         reading('B', 301, delay=900),
                         {**reading('D', 40, delay=None), 'scheduled_headway_seconds': None,
                          'health_score': None, 'health_level': 'unknown'},
                         reading('E', -60), reading('not-in-catalogue', 10)]

    def routes(self):
        return [{'route_id': '1'}, {'route_id': '2'}]

    def stops(self, route_id=None):
        return [row for row in self.catalogue if not route_id or route_id in row['route_ids']]

    def latest_metrics(self):
        return self.readings

    def metrics(self, **kwargs):
        raise AssertionError('Coverage must not use the bounded historical metrics query')

    def status_snapshot(self):
        return {'mode': 'synthetic', 'as_of': self.now.isoformat(), 'stale': False, 'time_basis': 'predicted'}

    def get_metadata(self, key, default=None):
        assert key == 'static'
        return {'retrieved_at': self.now.isoformat(), 'static_version': 'synthetic-fixture'}

    def network_health(self):
        return {'health_score': None, 'route_count': 2, 'stop_count': 999,
                'routes': [{'route_id': '1', 'stop_count': 8}, {'route_id': '2', 'stop_count': 0}]}


@pytest.fixture
def coverage_client(monkeypatch):
    from datetime import datetime, timezone

    now = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
    keys = {'public': secrets.token_urlsafe(32), 'analytics': secrets.token_urlsafe(32)}
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('PUBLIC_API_TOKEN', keys['public'])
    monkeypatch.setenv('API_TOKEN', keys['analytics'])
    for name in ('API_TOKEN_FILE', 'PUBLIC_API_TOKEN_FILE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr('data_horizon.api.utcnow', lambda: now)
    store = CoverageStore(now)
    with TestClient(create_app(store), headers=headers(keys, 'analytics')) as client:
        yield store, client, keys


@pytest.mark.case('F010-COL03', 'F007-COL02')
def test_coverage_lists_every_catalogue_platform_and_reconciles_availability(coverage_client):
    store, client, _ = coverage_client
    response = client.get('/api/v1/routes/1/coverage')
    assert response.status_code == 200
    result = response.json()['data']
    assert result['counts'] == {'catalogue': 5, 'fresh': 2, 'stale': 2, 'no_reading': 1, 'exact_delay_available': 1}
    assert result['returned'] == 5 and result['limit'] == 2000 and result['truncated'] is False
    assert {row['stop_id'] for row in result['stops']} == {row['stop_id'] for row in store.stops('1')}
    rows = {row['stop_id']: row for row in result['stops']}
    assert rows['A']['direction_id'] == '1' and rows['A']['delay_mean_seconds'] == 0
    assert rows['A']['reading_status'] == 'fresh' and rows['A']['freshness_seconds'] == 20
    assert rows['D']['reading_status'] == 'fresh' and rows['D']['delay_mean_seconds'] is None
    assert rows['D']['headway_seconds'] == 60 and rows['D']['health_level'] == 'unknown'
    assert rows['C']['reading_status'] == 'no_reading' and rows['C']['last_reading_at'] is None
    for stop in ('B', 'C', 'E'):
        for field in ('delay_mean_seconds', 'headway_seconds', 'scheduled_headway_seconds', 'health_score'):
            assert rows[stop][field] is None
        assert rows[stop]['health_level'] == 'unknown'
    assert rows['B']['reading_status'] == 'stale' and rows['B']['freshness_seconds'] == 301
    assert rows['E']['reading_status'] == 'stale'  # Future clocks cannot appear fresh.
    assert result['catalogue_as_of'] == store.now.isoformat() and result['catalogue_version'] == 'synthetic-fixture'


@pytest.mark.case('F010-COL03')
def test_coverage_ties_and_catalogue_duplicates_are_deterministic(coverage_client):
    store, client, _ = coverage_client
    store.catalogue += [dict(store.catalogue[0])]
    store.readings += [{**store.readings[1], 'direction_id': '0', 'delay_mean_seconds': 60}]
    first = client.get('/api/v1/routes/1/coverage').json()['data']
    store.catalogue.reverse()
    store.readings.reverse()
    second = client.get('/api/v1/routes/1/coverage').json()['data']
    assert first == second and first['counts']['catalogue'] == 5
    assert len([row for row in first['stops'] if row['stop_id'] == 'A']) == 1


@pytest.mark.case('F010-COL03')
def test_coverage_limit_is_explicit_and_counts_still_cover_the_full_catalogue(coverage_client):
    _, client, _ = coverage_client
    result = client.get('/api/v1/routes/1/coverage?limit=2').json()['data']
    assert result['counts']['catalogue'] == 5 and result['returned'] == 2 and result['truncated'] is True
    assert [row['stop_id'] for row in result['stops']] == ['A', 'B']


@pytest.mark.case('F010-COL03', 'F010-COL02')
@pytest.mark.parametrize('query', ['limit=0', 'limit=2001', 'offset=1', 'stop_id=A'])
def test_coverage_rejects_bad_or_unsupported_bounds(coverage_client, query):
    assert coverage_client[1].get('/api/v1/routes/1/coverage?' + query).status_code == 422


@pytest.mark.case('F010-COL03', 'F010-COL02')
def test_coverage_requires_analytics_key_and_known_route(coverage_client):
    _, client, keys = coverage_client
    assert client.get('/api/v1/routes/1/coverage', headers=headers(keys, 'public')).status_code == 403
    assert client.get('/api/v1/routes/1/coverage', headers={'Authorization': 'Bearer wrong'}).status_code == 401
    assert client.get('/api/v1/routes/missing/coverage').status_code == 404
    assert client.get('/api/v1/routes/1%27OR/coverage').status_code == 422


@pytest.mark.case('F010-COL03', 'F007-COL02')
def test_network_stop_counts_use_distinct_catalogue_platforms(coverage_client):
    store, client, keys = coverage_client
    store.catalogue += [dict(store.catalogue[0])]
    result = client.get('/api/v1/network/health', headers=headers(keys, 'public')).json()['data']
    assert result['stop_count'] == 6
    assert {row['route_id']: row['stop_count'] for row in result['routes']} == {'1': 5, '2': 1}
    assert result['routes'][0]['reading_entity_count'] == 8


@pytest.mark.case('F010-COL03')
def test_coverage_returns_more_than_the_history_limit_and_flags_a_large_catalogue(coverage_client):
    store, client, _ = coverage_client
    store.catalogue = [{'stop_id': f'S{index:04}', 'stop_name': f'Platform {index:04}', 'route_ids': ['1']}
                       for index in range(2001)]
    result = client.get('/api/v1/routes/1/coverage').json()['data']
    assert result['counts'] == {'catalogue': 2001, 'fresh': 0, 'stale': 0, 'no_reading': 2001, 'exact_delay_available': 0}
    assert result['returned'] == 2000 and result['truncated'] is True
    assert len({row['stop_id'] for row in result['stops']}) == 2000


@pytest.mark.case('F010-COL03')
@pytest.mark.evidence('synthetic', 'integration')
def test_real_store_coverage_keeps_quiet_platform_outside_the_newest_thousand_snapshots(
    coverage_client, isolated_data_dir, monkeypatch,
):
    from datetime import timedelta
    from data_horizon.store import Store, dumps

    fixture, _, keys = coverage_client
    now = fixture.now
    monkeypatch.setattr('data_horizon.store.utcnow', lambda: now)
    backend = Store()
    backend.replace_static([{'route_id': '1'}], [
        {'stop_id': stop, 'stop_name': stop, 'route_ids': ['1']} for stop in ('A', 'Q', 'missing')], [])
    with backend.connect() as connection:
        for index in range(1100):
            at = (now - timedelta(seconds=index)).isoformat()
            row = {'route_id': '1', 'stop_id': 'A', 'direction_id': '0', 'window_start': at,
                   'source_as_of': at, 'delay_mean_seconds': 0, 'headway_seconds': 60,
                   'scheduled_headway_seconds': 60, 'health_score': 100, 'health_level': 'good'}
            backend.execute(connection,
                'INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)',
                ('synthetic', f'busy-{index}', '1', 'A', '0', at, dumps(row)))
        at = (now - timedelta(seconds=2000)).isoformat()
        quiet = {'route_id': '1', 'stop_id': 'Q', 'direction_id': '0', 'window_start': at,
                 'source_as_of': at, 'delay_mean_seconds': 42}
        backend.execute(connection,
            'INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)',
            ('synthetic', 'quiet', '1', 'Q', '0', at, dumps(quiet)))
    assert {row['stop_id'] for row in backend.metrics(route_id='1', limit=1000)} == {'A'}
    with TestClient(create_app(backend), headers=headers(keys, 'analytics')) as client:
        response = client.get('/api/v1/routes/1/coverage')
    assert response.status_code == 200
    result = response.json()['data']
    assert result['counts'] == {'catalogue': 3, 'fresh': 1, 'stale': 1, 'no_reading': 1, 'exact_delay_available': 1}
    quiet_row = next(row for row in result['stops'] if row['stop_id'] == 'Q')
    assert quiet_row['reading_status'] == 'stale' and quiet_row['last_reading_at'] == at
    assert quiet_row['delay_mean_seconds'] is None
