import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { proxy, boundedBody, settings, publicKey, analyticsKey } from '../lib/server.mjs';

const env = { API_BASE_URL: 'http://api:8000', DASHBOARD_PUBLIC_URL: 'http://127.0.0.1:8501',
  PUBLIC_API_TOKEN: 'test-public-service-key', API_TOKEN: 'test-analytics-service-key' };
const request = (path, options = {}) => new Request(env.DASHBOARD_PUBLIC_URL + '/bff/api/v1/' + path, options);
const calculation = (changes = {}) => ({ method: 'POST', headers: { Origin: env.DASHBOARD_PUBLIC_URL,
  'Content-Type': 'application/json' }, body: JSON.stringify({ route_id: '1', stop_id: '101N' }), ...changes });

test('public and analytical reads use the correct server key and fixed upstream', async () => {
  for (const [path, token] of [['status', env.PUBLIC_API_TOKEN], ['routes', env.PUBLIC_API_TOKEN],
    ['network/health', env.PUBLIC_API_TOKEN], ['stops', env.PUBLIC_API_TOKEN],
    ['alerts', env.PUBLIC_API_TOKEN], ['anomalies', env.API_TOKEN],
    ['pipeline/status', env.API_TOKEN], ['routes/1/metrics', env.API_TOKEN], ['routes/1/coverage', env.API_TOKEN]]) {
    let seen;
    const response = await proxy(request(path + '?limit=10'), path.split('/'), { env,
      fetch: async (url, options) => { seen = { url, options }; return Response.json({ data: [] }); } });
    assert.equal(response.status, 200, path);
    assert.equal(seen.url, 'http://api:8000/api/v1/' + path + '?limit=10');
    assert.equal(seen.options.headers.Authorization, 'Bearer ' + token);
    assert.equal(seen.options.redirect, 'manual');
    assert.equal(seen.options.cache, 'no-store');
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.ok(!(await response.text()).includes(token));
  }
});

test('what-if uses analytics key without forwarding cookies or browser-controlled headers', async () => {
  let sent;
  const response = await proxy(request('what-if', calculation({ headers: { Origin: env.DASHBOARD_PUBLIC_URL,
    'Content-Type': 'application/json', Cookie: 'legacy_session=do-not-forward', 'X-Forwarded-For': '1.2.3.4' } })),
    ['what-if'], { env, fetch: async (_, options) => { sent = options; return Response.json({ data: {} }); } });
  assert.equal(response.status, 200);
  assert.equal(sent.headers.Authorization, 'Bearer ' + env.API_TOKEN);
  assert.equal(sent.headers.Cookie, undefined);
  assert.equal(sent.headers['X-Forwarded-For'], undefined);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(sent.body)), { route_id: '1', stop_id: '101N' });
});

test('retired account and product endpoints have no proxy route', async () => {
  for (const path of ['model/status', 'predictions', 'auth/login', 'auth/me', 'auth/register', 'me/preferences', 'notifications', 'subscriptions',
    'admin/accounts', 'analysis/incidents', 'analysis/scenarios', 'operations/diagnostics',
    'commute', 'tracking', 'arrivals', 'journey-options', 'station-details']) {
    let called = false;
    const response = await proxy(request(path), path.split('/'), { env,
      fetch: async () => { called = true; return Response.json({}); } });
    assert.equal(response.status, 404, path);
    assert.equal(called, false);
  }
});

test('unknown methods and traversal fail before any upstream call', async () => {
  for (const [path, segments, method, expected] of [
    ['routes', ['routes'], 'DELETE', 405], ['what-if', ['what-if'], 'GET', 405],
    ['routes', ['..', 'routes'], 'GET', 404], ['routes', ['routes', 'a/b', 'metrics'], 'GET', 404],
    ['routes', ['routes', 'x'.repeat(65), 'metrics'], 'GET', 404],
  ]) {
    let called = false;
    const result = await proxy(request(path, { method }), segments, { env,
      fetch: async () => { called = true; return Response.json({}); } });
    assert.equal(result.status, expected);
    assert.equal(called, false);
  }
});

test('forged origin, browser authorization and non-JSON calculations are rejected', async () => {
  for (const [options, expected] of [
    [calculation({ headers: { 'Content-Type': 'application/json' } }), 403],
    [calculation({ headers: { Origin: 'https://other.invalid', 'Content-Type': 'application/json' } }), 403],
    [calculation({ headers: { Origin: env.DASHBOARD_PUBLIC_URL, 'Content-Type': 'text/plain' } }), 415],
    [{ headers: { Authorization: 'Bearer attacker-value' } }, 400],
  ]) {
    let called = false;
    const method = options.method || 'GET';
    const path = method === 'POST' ? 'what-if' : 'routes';
    const result = await proxy(request(path, options), [path], { env,
      fetch: async () => { called = true; return Response.json({}); } });
    assert.equal(result.status, expected);
    assert.equal(called, false);
  }
});

test('streamed body and query bounds are checked before forwarding', async () => {
  let called = false;
  const options = { env, fetch: async () => { called = true; return Response.json({}); } };
  const body = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(16385)); controller.close(); } });
  const result = await proxy(request('what-if', calculation({ body, duplex: 'half' })), ['what-if'], options);
  assert.equal(result.status, 413);
  assert.equal((await proxy(request('routes?q=' + 'x'.repeat(2049)), ['routes'], options)).status, 414);
  assert.equal(called, false);
});

test('redirects, oversized or invalid JSON upstream responses fail safely', async () => {
  for (const upstream of [new Response('', { status: 302, headers: { Location: 'https://other.invalid' } }),
    new Response('not json'), new Response('x'.repeat(8 * 1024 * 1024 + 1))]) {
    const result = await proxy(request('routes'), ['routes'], { env, fetch: async () => upstream });
    assert.equal(result.status, 502);
    assert.ok(!(await result.text()).includes('other.invalid'));
  }
});

test('upstream errors hide sensitive details while preserving unavailable status', async () => {
  for (const code of [401, 403, 404, 422, 429, 500, 503]) {
    const result = await proxy(request('anomalies'), ['anomalies'], { env,
      fetch: async () => Response.json({ detail: 'sensitive-database-location' }, { status: code }) });
    assert.equal(result.status, code);
    assert.ok(!(await result.text()).includes('sensitive-database-location'));
  }
});

test('unavailable config or network gives safe errors, never a fabricated data result', async () => {
  for (const extra of [{ env: { ...env, API_TOKEN: '' } }, { fetch: async () => { throw new Error('sensitive-network-detail'); } }]) {
    const result = await proxy(request('anomalies'), ['anomalies'], { env, ...extra });
    assert.equal(result.status, 503);
    assert.ok(!(await result.text()).includes('sensitive-network-detail'));
  }
});

test('URL settings reject credentials, query strings and unsupported targets', () => {
  for (const value of ['file:///etc/passwd', 'https://user:password@api.invalid', 'https://api.invalid/path',
    'https://api.invalid/?next=elsewhere', 'https://api.invalid/#fragment']) {
    assert.throws(() => settings({ ...env, API_BASE_URL: value }));
  }
  assert.throws(() => settings({ ...env, APP_ENV: 'production' }));
  assert.equal(settings({ ...env, APP_ENV: 'production', DASHBOARD_PUBLIC_URL: 'https://horizon.example' }).origin, 'https://horizon.example');
});

test('runtime key files take precedence; missing files cannot silently fall back', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'horizon-ui-test-'));
  const file = join(folder, 'service-key');
  try {
    await writeFile(file, 'test-file-service-key\n');
    assert.equal(await publicKey({ ...env, PUBLIC_API_TOKEN_FILE: file }), 'test-file-service-key');
    assert.equal(await analyticsKey({ ...env, API_TOKEN_FILE: file }), 'test-file-service-key');
    await assert.rejects(publicKey({ ...env, PUBLIC_API_TOKEN_FILE: join(folder, 'missing') }));
    await assert.rejects(analyticsKey({ ...env, APP_ENV: 'production' }));
    await assert.rejects(publicKey({ ...env, PUBLIC_API_TOKEN: 'bad\nheader' }));
  } finally { await rm(folder, { recursive: true, force: true }); }
});

test('bounded stream utility preserves bytes and rejects overflow', async () => {
  const stream = value => new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(value)); controller.close(); } });
  assert.equal(new TextDecoder().decode(await boundedBody(stream('hello'), 5)), 'hello');
  await assert.rejects(boundedBody(stream('hello!'), 5), RangeError);
});
