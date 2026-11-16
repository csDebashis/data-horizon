import test from 'node:test';
import assert from 'node:assert/strict';
import { emptyPageFetchState, pageFetchSummary, shouldRefreshPage, updatePageFetchState } from '../lib/page-refresh-state.mjs';
const first = '2026-11-15T10:00:00.000Z';
const second = '2026-11-15T10:00:30.000Z';
const pending = path => ({ path, pending: true, fetchedAt: null, error: null });
const success = (path, fetchedAt = first) => ({ path, pending: false, fetchedAt, error: null });
const change = (state, id, record) => updatePageFetchState(state, id, record);

test('page completion waits for all registered requests, independent of source timestamps', () => {
  let state = change(emptyPageFetchState(), 'health', pending('/network/health'));
  state = change(state, 'alerts', pending('/alerts'));
  state = change(state, 'health', success('/network/health', second));
  assert.equal(state.lastSuccessfulAt, null);
  state = change(state, 'alerts', success('/alerts'));
  assert.equal(state.lastSuccessfulAt, second);
});

test('same-scope pending and partial failure retain the last fully successful fetch', () => {
  let state = change(emptyPageFetchState(), 'health', success('/network/health'));
  state = change(state, 'health', { ...success('/network/health'), pending: true });
  assert.equal(state.lastSuccessfulAt, first);
  assert.equal(pageFetchSummary(state).pending, true);
  state = change(state, 'health', { ...success('/network/health'), error: 'Temporarily unavailable' });
  assert.equal(state.lastSuccessfulAt, first);
  assert.equal(pageFetchSummary(state).failed, true);
  state = change(state, 'health', success('/network/health', second));
  assert.equal(state.lastSuccessfulAt, second);
});

test('one successful response cannot claim a failed or still pending page update', () => {
  let state = change(emptyPageFetchState(), 'health', success('/network/health'));
  state = change(state, 'alerts', success('/alerts'));
  state = change(state, 'alerts', { ...success('/alerts'), pending: true });
  state = change(state, 'health', success('/network/health', second));
  assert.equal(state.lastSuccessfulAt, first);
  state = change(state, 'alerts', { ...success('/alerts'), error: 'Unavailable' });
  assert.equal(state.lastSuccessfulAt, first);
});

test('changing request scope clears the previous successful page timestamp until completion', () => {
  let state = change(emptyPageFetchState(), 'route', success('/routes/1/metrics'));
  state = change(state, 'route', pending('/routes/2/metrics'));
  assert.equal(state.lastSuccessfulAt, null);
  state = change(state, 'route', success('/routes/2/metrics', second));
  assert.equal(state.lastSuccessfulAt, second);
});

test('unmounted and null-path queries stop counting; static pages have no invented fetch', () => {
  let state = change(emptyPageFetchState(), 'route', success('/routes/1/metrics'));
  state = change(state, 'closed', pending(null));
  assert.equal(pageFetchSummary(state).count, 1);
  state = change(state, 'route', null);
  assert.deepEqual(pageFetchSummary(state), { count: 0, pending: false, failed: false, lastSuccessfulAt: null });
});

test('invalid timestamps cannot qualify a successful fetch and unchanged records do not publish', () => {
  for (const fetchedAt of [null, undefined, '', 'invalid', Infinity]) {
    const state = change(emptyPageFetchState(), 'query', { ...success('/commute'), fetchedAt });
    assert.equal(state.lastSuccessfulAt, null);
  }
  const state = change(emptyPageFetchState(), 'query', success('/commute'));
  assert.equal(change(state, 'query', success('/commute')), state);
});

test('polling requires visible mounted settled data and never overlaps a requested update', () => {
  const idle = { count: 2, pending: false };
  assert.equal(shouldRefreshPage(idle, true), true);
  assert.equal(shouldRefreshPage(idle, false), false);
  assert.equal(shouldRefreshPage({ count: 0, pending: false }, true), false);
  assert.equal(shouldRefreshPage({ count: 2, pending: true }, true), false);
  assert.equal(shouldRefreshPage(idle, true, true), false);
});


test('newly opened data sections cannot inherit a completion time from earlier page requests', () => {
  let state = change(emptyPageFetchState(), 'stops', success('/stops'));
  state = change(state, 'station-detail', pending('/station-details?stop_id=101S'));
  assert.equal(state.lastSuccessfulAt, null);
  state = change(state, 'station-detail', { path: '/station-details?stop_id=101S', pending: false, fetchedAt: null, error: 'Unavailable' });
  assert.equal(state.lastSuccessfulAt, null);
});
