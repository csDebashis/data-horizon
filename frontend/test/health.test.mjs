import test from 'node:test';
import assert from 'node:assert/strict';
import { healthPresentation } from '../lib/health.mjs';

const source = '2026-11-19T06:25:03Z';
const sourceMs = Date.parse(source);
const retained = (changes = {}) => ({ health_score: null, last_valid_health: {
  score: 73, level: 'watch', source_as_of: source, age_seconds: 299,
  retention_seconds: 300, reason: 'Waiting for current inputs.', ...changes,
} });
const current = (score = 66, changes = {}) => ({ health_score: score, health_level: 'watch',
  health_source_as_of: source, health_age_seconds: 2,
  health_freshness_seconds: 120, health_retention_seconds: 300,
  health_retention_eligible: true, ...changes });

test('valid current health always wins and zero is a valid score', () => {
  for (const score of [0, 65, 100]) {
    const view = healthPresentation({ ...retained(), ...current(score) }, 1000, 1000, sourceMs + 2000);
    assert.equal(view.status, 'current');
    assert.equal(view.score, score);
  }
});

test('an original current reading ages into last valid and expires during a polling outage', () => {
  for (const [elapsed, expected] of [[118000, 'current'], [119000, 'last_valid'], [298000, 'last_valid'], [299000, 'unavailable']]) {
    const view = healthPresentation(current(), 1000, 1000 + elapsed, sourceMs + 2000 + elapsed);
    assert.equal(view.status, expected);
  }
  assert.equal(healthPresentation({ health_score: 66 }, 0, 0, sourceMs).status, 'unavailable');
  assert.equal(healthPresentation(current(66, { health_retention_eligible: false }), 0, 119000, sourceMs + 121000).status, 'unavailable');
});

test('last valid health is bounded at299/300/301 seconds using monotonic elapsed time', () => {
  for (const [elapsed, expected] of [[0, 'last_valid'], [1000, 'last_valid'], [2000, 'unavailable']]) {
    const view = healthPresentation(retained(), 1000, 1000 + elapsed, sourceMs + 299000 + elapsed);
    assert.equal(view.status, expected);
    if (expected === 'last_valid') assert.equal(view.ageSeconds, 299 + elapsed / 1000);
  }
});

test('a successful fetch cannot renew the original source age or extend the300second cap', () => {
  assert.equal(healthPresentation(retained({ age_seconds: 301 }), 2000, 2000, sourceMs + 301000).status, 'unavailable');
  assert.equal(healthPresentation(retained({ age_seconds: 301, retention_seconds: 900 }), 0, 0, sourceMs + 301000).status, 'unavailable');
  // Inconsistent young age cannot override the original timestamp and server-derived now.
  assert.equal(healthPresentation(retained({ age_seconds: 1 }), 1000, 1000, sourceMs + 301000).status, 'unavailable');
});

test('absent, invalid, future or unknown-time health is never made up', () => {
  for (const changes of [{ score: null }, { score: '73' }, { score: NaN }, { score: -1 }, { score: 101 },
    { source_as_of: null }, { source_as_of: 'not-a-date' }, { source_as_of: '2026-02-30T06:25:03Z' },
    { source_as_of: '2026-11-19T06:25:03' }, { age_seconds: -1 }, { age_seconds: null },
    { retention_seconds: 0 }, { retention_seconds: null }]) {
    assert.equal(healthPresentation(retained(changes), 1000, 1000, sourceMs + 299000).status, 'unavailable');
  }
  assert.equal(healthPresentation(retained(), null, 1000, sourceMs + 299000).status, 'unavailable');
  assert.equal(healthPresentation(retained(), 1000, 999, sourceMs + 299000).status, 'unavailable');
  assert.equal(healthPresentation(retained(), 1000, 1000, sourceMs - 1000).status, 'unavailable');
  assert.equal(healthPresentation({ health_score: null }, 1000, 1000, sourceMs).status, 'unavailable');
});
