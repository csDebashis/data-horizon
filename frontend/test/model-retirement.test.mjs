import test from 'node:test';
import assert from 'node:assert/strict';
import { proxy } from '../lib/server.mjs';
import { workspacePath } from '../lib/workspace-navigation.mjs';

const origin = 'http://127.0.0.1:8501';
const env = { API_BASE_URL: 'http://api:8000', DASHBOARD_PUBLIC_URL: origin,
  PUBLIC_API_TOKEN: 'test-public-service-key', API_TOKEN: 'test-analytics-service-key' };

test('retired model reads return 404 without an upstream request', async () => {
  for (const path of ['model/status', 'predictions']) {
    let forwarded = false;
    const result = await proxy(new Request(origin + '/bff/api/v1/' + path), path.split('/'), {
      env, fetch: async () => { forwarded = true; return Response.json({ data: {} }); },
    });
    assert.equal(result.status, 404, path);
    assert.equal(forwarded, false, path);
  }
});

test('model is no longer a navigable workspace', () => {
  assert.equal(workspacePath('/?view=model', origin), null);
  for (const view of ['network', 'route', 'scenario', 'pipeline', 'about']) {
    assert.equal(workspacePath('/?view=' + view, origin), '/?view=' + view);
  }
});
