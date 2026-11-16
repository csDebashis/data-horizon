import test from 'node:test';
import assert from 'node:assert/strict';
import { filterPlatformCoverage } from '../lib/coverage.mjs';

const rows = [
  { stop_id: 'A1N', stop_name: 'Active platform', reading_status: 'fresh' },
  { stop_id: 'Q1N', stop_name: 'Quiet platform', parent_station: 'P1', reading_status: 'no_reading', headway_seconds: null },
  { stop_id: 'O1N', stop_name: 'Old platform', reading_status: 'stale', delay_mean_seconds: null },
];
test('coverage retains quiet and old platforms with missing metrics', () => {
  assert.deepEqual(filterPlatformCoverage(rows), rows);
  assert.equal(filterPlatformCoverage(rows, '', 'no_reading')[0], rows[1]);
  assert.equal(filterPlatformCoverage(rows, '', 'stale')[0], rows[2]);
  assert.equal(rows[1].headway_seconds, null);
});
test('coverage searches names, IDs and parent stations without changing the input', () => {
  assert.deepEqual(filterPlatformCoverage(rows, '  QUIET '), [rows[1]]);
  assert.deepEqual(filterPlatformCoverage(rows, 'q1n'), [rows[1]]);
  assert.deepEqual(filterPlatformCoverage(rows, 'P1'), [rows[1]]);
  assert.equal(rows.length, 3);
});
test('status and search must both match and empty matches stay empty', () => {
  assert.deepEqual(filterPlatformCoverage(rows, 'Quiet', 'fresh'), []);
  assert.deepEqual(filterPlatformCoverage(rows, 'not in catalogue'), []);
});
