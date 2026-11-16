import test from 'node:test';
import assert from 'node:assert/strict';
import { routeFiltersFromHistory, routeHistoryState } from '../lib/route-state.mjs';
const filters = { platform: '257S', search: 'New Lots', status: 'no_reading' };
const defaults = { platform: '', search: '', status: 'all' };

test('history restores matching public route/source filters and retains framework state', () => {
  const framework = { __NA: true, tree: { kept: true } };
  const state = routeHistoryState(framework, '2', 'route:live', filters);
  assert.equal(state.__NA, true);
  assert.equal(state.tree, framework.tree);
  assert.deepEqual(routeFiltersFromHistory(state, '2', 'route:live'), filters);
  assert.equal(framework.dataHorizonRouteV1, undefined);
});
test('a different route or source never inherits the previous entry filters', () => {
  const state = routeHistoryState(null, '2', 'route:live', filters);
  assert.deepEqual(routeFiltersFromHistory(state, '1', 'route:live'), defaults);
  assert.deepEqual(routeFiltersFromHistory(state, '2', 'route:recorded'), defaults);
  for (const value of [null, undefined, [], {}, 'invalid']) assert.deepEqual(routeFiltersFromHistory(value, '2', 'route:live'), defaults);
});
test('invalid identifiers, unbounded search and unknown statuses are discarded', () => {
  for (const platform of ['../bad', '..', '.', 'x'.repeat(65), '<script>', 1]) {
    const state = { dataHorizonRouteV1: { route: '2', scope: 'route:live', platform, search: 'safe', status: 'all' } };
    assert.equal(routeFiltersFromHistory(state, '2', 'route:live').platform, '');
  }
  for (const search of ['x'.repeat(101), 'line\nbreak', 4]) {
    const state = routeHistoryState(null, '2', 'route:live', { ...filters, search, status: 'unknown' });
    assert.equal(routeFiltersFromHistory(state, '2', 'route:live').search, '');
    assert.equal(routeFiltersFromHistory(state, '2', 'route:live').status, 'all');
  }
});
test('entry state contains only allowlisted filters, never supplied result rows or credentials', () => {
  const state = routeHistoryState(null, '2', 'route:live', { ...filters, rows: [{ id: 'not-saved' }], token: 'not-a-credential' });
  assert.deepEqual(Object.keys(state.dataHorizonRouteV1).sort(), ['platform', 'route', 'scope', 'search', 'status']);
});
