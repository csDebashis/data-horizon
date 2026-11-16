"""Read-only acceptance checks against the explicitly started project stack.

Enable with RUN_LIVE_API_CONTRACT=1. This does not start containers, reset data,
create accounts or change stored application state. The what-if
POST is a bounded calculation only.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.case('F010-COL01', 'F010-COL02', 'F010-COL03', 'F008-COL01', 'F009-COL05',
                     'F014-COL01', 'F015-COL01', 'F016-COL01'),
    pytest.mark.evidence('live', 'integration'),
    pytest.mark.skipif(os.getenv('RUN_LIVE_API_CONTRACT') != '1', reason='Needs an explicitly started current stack'),
]


def _key(name, default_filename):
    filename = Path(os.getenv(name + '_FILE', str(ROOT / '.secrets' / default_filename)))
    if not filename.is_file():
        pytest.fail(f'{name}_FILE must point to the current service key file')
    value = filename.read_text().strip()
    if len(value) < 32:
        pytest.fail(f'{name} requires a current service key of at least 32 characters')
    return value


def _retired_model_fields(value):
    forbidden = {'model', 'model_status', 'experiment', 'ml_experiment', 'predictions',
                 'model_training', 'model_workflow'}
    if isinstance(value, dict):
        return bool(set(value) & forbidden) or any(_retired_model_fields(part) for part in value.values())
    if isinstance(value, list):
        return any(_retired_model_fields(part) for part in value)
    return False


def _report_output():
    requested = Path(os.getenv('LIVE_API_REPORT_FILE', 'reports/model-retirement-91/live-api-contract.json'))
    output = (ROOT / requested).resolve() if not requested.is_absolute() else requested.resolve()
    reports = (ROOT / 'reports').resolve()
    if not reports.is_relative_to(ROOT.resolve()) or output == reports or not output.is_relative_to(reports):
        raise ValueError('Live API reports must stay under the project reports directory')
    return output


def test_running_live_api_contract_and_removed_product_routes(record_testsuite_property):
    output = _report_output()
    analytics_key = _key('API_TOKEN', 'api_token')
    public_key = _key('PUBLIC_API_TOKEN', 'public_api_token')
    if analytics_key == public_key:
        pytest.fail("Public and analytics service keys must differ")
    base = os.getenv('LIVE_API_URL', 'http://127.0.0.1:8000').rstrip('/')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    report = {
        'executed_at_utc': datetime.now(timezone.utc).isoformat(), 'test_source_revision': revision,
        'scope': 'Actual local running API; read-only state checks and bounded unsaved what-if. No VM or sustained-performance claim.',
        'result': 'failed', 'checks': [],
    }

    def request(client, method, path, *, public=False, expected=200, **kwargs):
        response = client.request(method, path,
                                  headers={'Authorization': 'Bearer ' + (public_key if public else analytics_key)},
                                  **kwargs)
        report['checks'].append({'method': method, 'path': path.split('?')[0],
                                 'key_scope': 'public' if public else 'analytics',
                                 'expected_status': expected, 'actual_status': response.status_code})
        # Never include full response bodies or credential values in failure output.
        if analytics_key in response.text or public_key in response.text:
            pytest.fail('A service credential appeared in an API response')
        assert response.status_code == expected, f'{method} {path.split("?")[0]} returned {response.status_code}, expected {expected}'
        return response

    def envelope(response):
        body = response.json()
        assert set(body) == {'data', 'meta'}
        assert {'mode', 'as_of', 'freshness_seconds', 'stale', 'time_basis'} <= set(body['meta'])
        assert body['meta']['mode'] in {'live', 'recorded', 'synthetic'}
        assert isinstance(body['meta']['stale'], bool)
        return body

    try:
        with httpx.Client(base_url=base, timeout=25, follow_redirects=False) as client:
            request(client, 'GET', '/health/live')
            request(client, 'GET', '/health/ready')
            status = envelope(request(client, 'GET', '/api/v1/status', public=True))
            assert not _retired_model_fields(status['data']), 'Status still projects retired model fields'
            report['data_mode'] = status['meta']['mode']
            report['source_as_of'] = status['meta']['as_of']
            report['source_stale'] = status['meta']['stale']
            routes = envelope(request(client, 'GET', '/api/v1/routes', public=True))['data']
            assert routes, 'The configured route list is empty'
            route = str(routes[0]['route_id'])
            metrics = envelope(request(client, 'GET', f'/api/v1/routes/{route}/metrics?limit=20'))['data']
            assert all(str(row['route_id']) == route for row in metrics)
            report['route_metrics_returned'] = len(metrics)
            network = envelope(request(client, 'GET', '/api/v1/network/health', public=True))['data']
            network_stops = {str(row['route_id']): row['stop_count'] for row in network['routes']}
            assert {str(row['route_id']) for row in routes} == set('123456')
            report['route_coverage'] = {}
            for configured_route in routes:
                route_id = str(configured_route['route_id'])
                catalogue = envelope(request(client, 'GET', '/api/v1/stops', public=True,
                                             params={'route_id': route_id, 'limit': 2000}))['data']
                coverage = envelope(request(client, 'GET', f'/api/v1/routes/{route_id}/coverage'))['data']
                ids = {str(row['stop_id']) for row in catalogue}
                covered = coverage['stops']
                assert {str(row['stop_id']) for row in covered} == ids
                assert len(covered) == len(ids) == coverage['returned'] == coverage['counts']['catalogue']
                assert coverage['truncated'] is False and network_stops[route_id] == len(ids)
                if report['data_mode'] == 'live':
                    assert len(ids) > 5, 'Live route coverage must use its full timetable, not the five-stop sample'
                counts = coverage['counts']
                assert counts['fresh'] + counts['stale'] + counts['no_reading'] == len(ids)
                for status_name in ('fresh', 'stale', 'no_reading'):
                    assert counts[status_name] == sum(row['reading_status'] == status_name for row in covered)
                assert counts['exact_delay_available'] == sum(
                    row['reading_status'] == 'fresh' and row['delay_mean_seconds'] is not None for row in covered)
                for row in covered:
                    if row['reading_status'] != 'fresh':
                        assert all(row[field] is None for field in (
                            'delay_mean_seconds', 'headway_seconds', 'scheduled_headway_seconds', 'health_score'))
                report['route_coverage'][route_id] = {
                    'membership_matches_catalogue': True, **counts,
                    'catalogue_as_of': coverage['catalogue_as_of'], 'catalogue_version': coverage['catalogue_version'],
                }
            request(client, 'GET', f'/api/v1/routes/{route}/coverage', public=True, expected=403)
            alerts = envelope(request(client, 'GET', f'/api/v1/alerts?route_id={route}', public=True))
            assert 'stale_after_seconds' in alerts['meta'] and 'reason' in alerts['meta']
            envelope(request(client, 'GET', f'/api/v1/anomalies?route_id={route}'))
            pipeline = envelope(request(client, 'GET', '/api/v1/pipeline/status'))['data']
            assert pipeline.get('streaming', {}).get('query_active') is True, 'Spark streaming query is not active'
            report['record_counts'] = pipeline.get('counts', {})
            assert not _retired_model_fields(pipeline), 'Pipeline still projects retired model fields'
            schema = request(client, 'GET', '/openapi.json', public=True).json()
            for retired_path in ('/api/v1/model/status', '/api/v1/predictions'):
                assert retired_path not in schema.get('paths', {}), 'OpenAPI still advertises a retired model route'
                request(client, 'GET', retired_path, expected=404)
                request(client, 'GET', retired_path, public=True, expected=404)
            report['retired_model_routes_absent'] = True
            report['model_projections_absent'] = True
            request(client, 'GET', '/metrics')
            request(client, 'GET', '/api/v1/pipeline/status', public=True, expected=403)
            request(client, 'GET', f'/api/v1/routes/{route}/metrics', public=True, expected=403)

            # A terminal stop may have no downstream edge. Try bounded candidates
            # rather than guessing station names or saving a graph scenario.
            scenario = None
            for candidate_route in routes[:2]:
                route_id = str(candidate_route['route_id'])
                stops = envelope(request(client, 'GET', '/api/v1/stops', public=True,
                                         params={'route_id': route_id, 'limit': 20}))['data']
                for stop in stops:
                    body = {'route_id': route_id, 'stop_id': str(stop['stop_id']), 'delay_seconds': 600,
                            'horizon_minutes': 15, 'limit': 10}
                    response = client.post('/api/v1/what-if', json=body,
                                           headers={'Authorization': 'Bearer ' + analytics_key})
                    if analytics_key in response.text or public_key in response.text:
                        pytest.fail("A service credential appeared in a what-if response")
                    assert response.status_code in {200, 404}, 'The current station graph is unavailable'
                    if response.status_code == 200:
                        scenario = envelope(response)['data']
                        break
                if scenario is not None:
                    break
            assert scenario is not None, 'No valid graph origin was found in the bounded station sample'
            assert scenario['horizon_minutes'] == 15 and len(scenario['affected_stations']) <= 10
            report['checks'].append({'method': 'POST', 'path': '/api/v1/what-if',
                                     'key_scope': 'analytics', 'expected_status': 200, 'actual_status': 200})
            report['affected_stations_returned'] = len(scenario['affected_stations'])

            retired = [('POST', '/auth/login'), ('POST', '/auth/register'), ('GET', '/auth/me'),
                       ('GET', '/me/preferences'), ('GET', '/me/commutes'), ('GET', '/subscriptions'),
                       ('GET', '/notifications'), ('GET', '/admin/users'), ('GET', '/admin/accounts'),
                       ('GET', '/analysis/incidents'), ('GET', '/analysis/scenarios'),
                       ('GET', '/analysis/investigation'), ('GET', '/arrivals'), ('GET', '/tracking')]
            for method, path in retired:
                request(client, method, '/api/v1' + path, expected=404)
            report['result'] = 'passed'
            record_testsuite_property('live_api_data_mode', report['data_mode'])
            record_testsuite_property('live_api_test_source_revision', revision)
    finally:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + '\n')
