/** Public filters belong to one browser entry, route and source; never store fetched rows or keys. */
const KEY = 'dataHorizonRouteV1';
const STATUSES = new Set(['all', 'fresh', 'stale', 'no_reading']);
const identifier = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,64}$/.test(value) && !['.', '..'].includes(value);
const defaults = () => ({ platform: '', search: '', status: 'all' });

function boundedFilters(value) {
  return {
    platform: identifier(value?.platform) ? value.platform : '',
    search: typeof value?.search === 'string' && value.search.length <= 100 && !/[\u0000-\u001f\u007f]/.test(value.search) ? value.search : '',
    status: STATUSES.has(value?.status) ? value.status : 'all',
  };
}

export function routeFiltersFromHistory(state, route, scope) {
  const saved = state?.[KEY];
  if (!saved || saved.route !== route || saved.scope !== scope) return defaults();
  return boundedFilters(saved);
}

export function routeHistoryState(previous, route, scope, filters) {
  // Retain Next.js history metadata and unrelated entry state without reading or exporting it.
  const base = previous && typeof previous === 'object' && !Array.isArray(previous) ? previous : {};
  return { ...base, [KEY]: { route, scope, ...boundedFilters(filters) } };
}
