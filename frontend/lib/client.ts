'use client';
import { useEffect, useState } from 'react';
import { usePageScope, usePageFetchRegistration, usePageRevision } from './page-refresh-context.mjs';
export class APIError extends Error {
    status: number;
    constructor(status: number, message: string) { super(message); this.status = status; }
}
async function requestJSON(path: string, method = 'GET', body?: unknown, signal?: AbortSignal) {
    const response = await fetch('/bff/api/v1' + path, {
        method, credentials: 'omit', cache: 'no-store', signal,
        headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    const payload = await response.json().catch(() => { throw new APIError(502, 'The API returned an unreadable response.'); });
    if (!response.ok)
        throw new APIError(response.status, typeof payload?.detail === 'string' ? payload.detail : 'Could not load this data. Please try again.');
    return { payload, receivedAt: performance.now(), fetchedAt: new Date().toISOString() };
}
export async function api(path: string, method = 'GET', body?: unknown, signal?: AbortSignal): Promise<any> {
    return (await requestJSON(path, method, body, signal)).payload;
}
export function query(path: string, values: Record<string, string | number | null | undefined>) {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(values)) {
        if (value !== null && value !== undefined && value !== '')
            params.set(key, String(value));
    }
    return path + (params.size ? '?' + params : '');
}
export type ApiSnapshot<T> = {
    data: T | null;
    meta: Record<string, any> | null;
    receivedAt: number | null;
    fetchedAt?: string | null;
    error: string | null;
    loading: boolean;
    path: string | null;
    identity: string | null;
};
/** Retain a good result only within the same page, source mode and query. */
export function requestSnapshot<T>(previous: ApiSnapshot<T>, path: string | null, identity: string | null, retain: boolean, loading: boolean, error: string | null = null): ApiSnapshot<T> {
    const permitted = !!(retain && path && identity && previous.path === path && previous.identity === identity &&
        previous.data !== null && previous.receivedAt !== null && Number.isFinite(previous.receivedAt) && previous.receivedAt >= 0);
    return { data: permitted ? previous.data : null, meta: permitted ? previous.meta : null,
        receivedAt: permitted ? previous.receivedAt : null, fetchedAt: permitted ? previous.fetchedAt ?? null : null,
        error, loading, path, identity };
}
export function useApi<T = any>(path: string | null, refresh = 0) {
    const identity = usePageScope();
    const pageRevision = usePageRevision();
    const [state, setState] = useState<ApiSnapshot<T>>({
        data: null, meta: null, receivedAt: null, fetchedAt: null, error: null, loading: !!path, path, identity,
    });
    useEffect(() => {
        let active = true;
        const controller = new AbortController();
        setState(previous => requestSnapshot(previous, path, identity, true, !!path));
        if (path)
            requestJSON(path, 'GET', undefined, controller.signal).then(({ payload, receivedAt, fetchedAt }) => {
                if (active)
                    setState({ data: payload.data ?? payload, meta: payload.meta ?? null,
                        receivedAt, fetchedAt, error: null, loading: false, path, identity });
            }).catch(error => {
                if (!active || error.name === 'AbortError')
                    return;
                const transient = !(error instanceof APIError) || error.status >= 500 || [408, 429].includes(error.status);
                setState(previous => requestSnapshot(previous, path, identity, transient, false, error.message || 'The API is unavailable.'));
            });
        return () => { active = false; controller.abort(); };
    }, [path, refresh, pageRevision, identity]);
    // A changed query cannot show old rows during the render before its effect.
    const visible = state.path === path && state.identity === identity ? state : requestSnapshot(state, path, identity, false, !!path);
    usePageFetchRegistration({ path, pending: visible.loading, fetchedAt: visible.fetchedAt ?? null, error: visible.error });
    return { ...visible, pending: visible.loading, loading: visible.loading && visible.data === null,
        refreshing: visible.loading && visible.data !== null };
}
