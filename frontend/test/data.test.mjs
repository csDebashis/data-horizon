import test from 'node:test';
import assert from 'node:assert/strict';
import { csv, number, localTime, localStamp, localise, series, lineSegments, readingAverage, validStationPoints } from '../lib/format.mjs';
import { snapshotClock } from '../lib/live.mjs';
import { displayPreferences, resolvedScheme, contrastRatio } from '../lib/display-preferences.mjs';
import { requestSnapshot, query } from '../lib/client.ts';

test('unavailable measurements do not become zero and numbers use Indian grouping', () => {
  for (const value of [null, undefined, '', Infinity, NaN]) assert.equal(number(value), 'Unavailable');
  assert.equal(number(1200000), '12,00,000');
  assert.equal(number(0, ' s'), '0 s');
});

test('local display and export preserve India time, New York DST and fractional precision', () => {
  assert.equal(localTime('2026-11-14T12:00:00Z', 'Asia/Kolkata'), '2026-11-14 17:30:00 +05:30');
  assert.equal(localStamp('2026-07-15T12:00:00Z', 'America/New_York'), '2026-07-15 08:00:00 -04:00 [America/New_York]');
  assert.equal(localStamp('2026-01-15T12:00:00Z', 'America/New_York'), '2026-01-15 07:00:00 -05:00 [America/New_York]');
  assert.equal(localStamp('2026-11-01T06:00:00.123456Z', 'America/New_York'), '2026-11-01 01:00:00.123456 -05:00 [America/New_York]');
  assert.equal(localTime(null, 'Asia/Kolkata'), 'Unavailable');
  assert.equal(localise({ timezone: 'UTC', updated_at: '2026-11-14T12:00:00Z' }, 'Asia/Kolkata').timezone, 'Asia/Kolkata');
});

test('CSV contains real rows, escapes formulas and quotes, and uses local timestamp offsets', () => {
  const result = csv([{ stop_id: '120N', value: -2, text: '=SUM(A1)', quote: 'a"b', at: '2026-11-14T12:00:00Z' }],
    ['stop_id', 'value', 'text', 'quote', 'at'], 'Asia/Kolkata');
  assert.ok(result.includes('120N'));
  assert.ok(result.includes('"-2"'));
  assert.ok(result.includes("'=SUM(A1)"));
  assert.ok(result.includes('a""b'));
  assert.ok(result.includes('2026-11-14 17:30:00 +05:30 [Asia/Kolkata]'));
});

test('charts average simultaneous readings and preserve absent measurements', () => {
  const result = series([{ window_start: '2026-01-01T00:00:00Z', delay: 2 },
    { window_start: '2026-01-01T00:00:00Z', delay: 4 },
    { window_start: '2026-01-01T00:01:00Z', delay: null }], ['delay']);
  assert.equal(result[0].delay, 3);
  assert.equal(result[1].delay, null);
});

test('historical summaries average displayed readings without pretending to weight matched trains', () => {
  const rows = [{ delay_mean_seconds: -60, event_count: 100 },
    { delay_mean_seconds: 20, event_count: 1 }, { delay_mean_seconds: null, event_count: 50 }];
  assert.equal(readingAverage(rows, 'delay_mean_seconds'), -20);
  assert.equal(readingAverage([{ value: 0 }, { value: '' }, { value: Infinity }, { value: undefined }], 'value'), 0);
  assert.equal(readingAverage([{ value: null }], 'value'), null);
  assert.equal(readingAverage([], 'value'), null);
});

test('chart lines stop at missing measurements and keep valid signed and zero values', () => {
  const rows = [{ time: 1, value: -2 }, { time: 2, value: 0 }, { time: 3, value: null },
    { time: 4, value: 4 }, { time: 5, value: undefined }, { time: 6, value: 6 },
    { time: 7, value: NaN }, { time: 8, value: Infinity }, { time: 9, value: null }];
  assert.deepEqual(lineSegments(rows, 'value'), [[rows[0], rows[1]], [rows[3]], [rows[5]]]);
  assert.deepEqual(lineSegments([{ time: 1, value: null }], 'value'), []);
});

test('station plot rejects missing and invalid coordinates but retains valid zero', () => {
  const rows = [{ stop_lat: null, stop_lon: null }, { stop_lat: '', stop_lon: ' ' },
    { stop_lat: 91, stop_lon: 0 }, { stop_lat: 0, stop_lon: 181 }, { stop_lat: 0, stop_lon: 0 },
    { stop_lat: '40.7', stop_lon: '-73.9' }];
  assert.deepEqual(validStationPoints(rows), [{ stop_lat: 0, stop_lon: 0 }, { stop_lat: 40.7, stop_lon: -73.9 }]);
});

test('source age increases using a monotonic clock and missing timing cannot appear fresh', () => {
  const status = { as_of: '2026-11-15T00:00:00Z', freshness_seconds: 119, stale: false, stale_after_seconds: 120 };
  assert.equal(snapshotClock(status, 1000, 1000).stale, false);
  assert.equal(snapshotClock(status, 1000, 3001).stale, true);
  for (const change of [{ as_of: null }, { freshness_seconds: null }, { stale_after_seconds: null }, { stale: true }]) {
    assert.equal(snapshotClock({ ...status, ...change }, 1000, 1000).stale, true);
  }
  assert.equal(snapshotClock(status, null, 1000).stale, true);
});

test('same query retains a snapshot through an outage without inventing a new fetch time', () => {
  const previous = { data: [{ stop_id: '101N' }], meta: { mode: 'synthetic' }, receivedAt: 10,
    fetchedAt: '2026-11-19T00:00:00Z', error: null, loading: false, path: '/routes/1/metrics', identity: 'route:synthetic' };
  const retained = requestSnapshot(previous, previous.path, previous.identity, true, false, 'Unavailable');
  assert.equal(retained.data, previous.data);
  assert.equal(retained.fetchedAt, previous.fetchedAt);
  assert.equal(retained.receivedAt, 10);
  for (const [path, scope] of [['/routes/2/metrics', previous.identity], [previous.path, 'route:live'], [null, previous.identity]]) {
    const cleared = requestSnapshot(previous, path, scope, true, true);
    assert.equal(cleared.data, null);
    assert.equal(cleared.fetchedAt, null);
  }
});

test('query builder encodes selected identifiers and omits empty filters', () => {
  assert.equal(query('/stops', { route_id: '1', stop_id: '', limit: 20 }), '/stops?route_id=1&limit=20');
  assert.equal(query('/stops', { stop_id: 'a&b' }), '/stops?stop_id=a%26b');
});

test('appearance accepts only known choices and system mode follows the device', () => {
  assert.deepEqual(displayPreferences({ scheme: 'unknown', private: 'discard' }), { scheme: 'dark' });
  assert.equal(resolvedScheme('system', false), 'light');
  assert.equal(resolvedScheme('system', true), 'dark');
  assert.equal(resolvedScheme('light', true), 'light');
});

test('reviewed text and navigation colours meet 4.5 contrast ratio in both themes', () => {
  for (const [foreground, background] of [['#EDF3FB', '#142136'], ['#ABC0D4', '#142136'],
    ['#172B3C', '#FFFFFF'], ['#496175', '#F3F6FA'], ['#304E65', '#EDF4F9'], ['#BDCFDD', '#071321']]) {
    assert.ok(contrastRatio(foreground, background) >= 4.5, `${foreground} on ${background}`);
  }
});
