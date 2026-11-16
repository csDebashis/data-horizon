import test from 'node:test';
import assert from 'node:assert/strict';
import { workspacePath, navigateWorkspace, interceptWorkspaceLink } from '../lib/workspace-navigation.mjs';
const origin = 'https://horizon.example';

test('five known page names and bounded route filters are allowed', () => {
  for (const view of ['network', 'route', 'scenario', 'pipeline', 'about']) {
    assert.equal(workspacePath('/?view=' + view + '&route=1', origin), '/?view=' + view + '&route=1');
  }
});

test('retired pages, external URLs, duplicate filters and private keys are rejected', () => {
  for (const value of ['/?view=model', '/?view=commute', '/?view=admin', '/?view=notifications', '/?view=investigation',
    'https://other.example/', '//horizon.example/', '/bff/api/v1/routes', '/?view=network&token=value',
    '/?view=route&route=1&route=2', '/?view=route&route=..', '/?view=route&route=' + 'x'.repeat(65),
    '/?view=about#section', '/?view=route&stop=101N']) assert.equal(workspacePath(value, origin), null, value);
});

test('internal links change browser history without reloading the document', () => {
  const previous = globalThis.window;
  const calls = [];
  const location = new URL('/', origin);
  globalThis.window = { location, history: {
    pushState(_, unused, path) { calls.push(path); location.href = new URL(path, origin).href; },
  }, PopStateEvent: class extends Event {}, dispatchEvent() {} };
  try {
    assert.equal(navigateWorkspace('/?view=route&route=2'), true);
    assert.deepEqual(calls, ['/?view=route&route=2']);
    assert.equal(navigateWorkspace('/?view=route&route=2'), true);
    assert.equal(calls.length, 1);
    assert.equal(navigateWorkspace('https://other.example/'), false);
    const attributes = { href: '/?view=about' };
    const anchor = { getAttribute: name => attributes[name] ?? null, hasAttribute: name => name in attributes };
    const event = { button: 0, defaultPrevented: false, target: { closest: () => anchor }, preventDefault() { this.defaultPrevented = true; } };
    assert.equal(interceptWorkspaceLink(event), true);
    assert.equal(event.defaultPrevented, true);
    assert.equal(calls.at(-1), '/?view=about');
  } finally { if (previous === undefined) delete globalThis.window; else globalThis.window = previous; }
});
