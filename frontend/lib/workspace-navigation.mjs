// Navigation carries public workspace filters only, never credentials or return URLs.
const views = new Set(['network', 'route', 'scenario', 'pipeline', 'about']);
const identifiers = new Set(['route']);
const identifier = value => /^[A-Za-z0-9_.:-]{1,64}$/.test(value) && value !== '.' && value !== '..';
/** Return a bounded, same-origin workspace path, or null for ordinary browser links. */
export function workspacePath(value, origin) {
    if (typeof value !== 'string' || !value || value.length > 1024 || /[\s\\\u0000-\u001f\u007f]/.test(value) || typeof origin !== 'string')
        return null;
    // Protocol-relative input is intentionally rejected even when its host matches.
    if (value.startsWith('//'))
        return null;
    let url;
    try {
        url = new URL(value, origin);
    }
    catch {
        return null;
    }
    if (url.origin !== origin || url.username || url.password || url.pathname !== '/' || url.hash)
        return null;
    // Relative path normalisation must not turn another URL into a workspace route.
    const relative = value.startsWith(origin + '/') ? value.slice(origin.length) : value;
    if (relative !== '/' && !relative.startsWith('/?'))
        return null;
    const seen = new Set();
    for (const [key, item] of url.searchParams) {
        if (seen.has(key))
            return null;
        seen.add(key);
        if (key === 'view') {
            if (!views.has(item))
                return null;
        }
        else if (identifiers.has(key)) {
            if (!identifier(item))
                return null;
        }
        else
            return null;
    }
    const query = url.searchParams.toString();
    return '/' + (query ? '?' + query : '');
}
/** Change the client workspace without reloading its document or trusting a redirect URL. */
export function navigateWorkspace(path, { replace = false } = {}) {
    if (typeof window === 'undefined')
        return false;
    const destination = workspacePath(path, window.location.origin);
    if (destination === null)
        return false;
    if (destination === window.location.pathname + window.location.search)
        return true;
    try {
        window.history[replace ? 'replaceState' : 'pushState'](null, '', destination);
        window.dispatchEvent(new window.PopStateEvent('popstate', { state: null }));
        return true;
    }
    catch {
        return false;
    }
}
/** Use onClick bubbling, so existing workspace handlers can preventDefault first. */
export function interceptWorkspaceLink(event) {
    if (typeof window === 'undefined' || event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.altKey || event.shiftKey)
        return false;
    const anchor = event.target?.closest?.('a[href]') || event.target?.parentElement?.closest?.('a[href]');
    if (!anchor || anchor.hasAttribute('download') || anchor.getAttribute('target') && anchor.getAttribute('target').toLowerCase() !== '_self')
        return false;
    const destination = workspacePath(anchor.getAttribute('href'), window.location.origin);
    if (destination === null || !navigateWorkspace(destination))
        return false;
    event.preventDefault();
    return true;
}
