import { readFile } from 'node:fs/promises';
const MAX_BODY = 16384;
const MAX_RESPONSE = 8 * 1024 * 1024;
const IDENTIFIER = /^[A-Za-z0-9_.:-]{1,64}$/;
const PUBLIC = new Set(['status', 'routes', 'stops', 'alerts', 'network/health']);
const READS = new Set([...PUBLIC, 'anomalies', 'pipeline/status']);
export function settings(env = process.env) {
    function origin(value) {
        const url = new URL(value);
        if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password ||
            url.search || url.hash || url.pathname !== '/')
            throw new Error('Invalid service URL');
        return url;
    }
    const base = origin(env.API_BASE_URL || 'http://127.0.0.1:8000');
    const dashboard = origin(env.DASHBOARD_PUBLIC_URL || 'http://127.0.0.1:8501');
    if (env.APP_ENV === 'production' && dashboard.protocol !== 'https:')
        throw new Error('Production requires HTTPS');
    return { base: base.origin, origin: dashboard.origin };
}
async function serviceKey(env, prefix) {
    // Keys stay on the server. Never return them or place them in NEXT_PUBLIC settings.
    const filename = env[prefix + '_FILE'];
    const token = filename ? (await readFile(filename, 'utf8')).trim() : (env[prefix] || '').trim();
    if (!token || token.length > 4096 || /[\r\n]/.test(token) ||
        env.APP_ENV === 'production' && token.length < 32)
        throw new Error('Service key unavailable');
    return token;
}
export const publicKey = (env = process.env) => serviceKey(env, 'PUBLIC_API_TOKEN');
export const analyticsKey = (env = process.env) => serviceKey(env, 'API_TOKEN');
export async function boundedBody(stream, maximum = MAX_BODY) {
    if (!stream)
        return new Uint8Array();
    const reader = stream.getReader();
    const chunks = [];
    let size = 0;
    try {
        while (true) {
            const item = await reader.read();
            if (item.done)
                break;
            size += item.value.byteLength;
            if (size > maximum) {
                await reader.cancel();
                throw new RangeError('Body exceeds its bound');
            }
            chunks.push(item.value);
        }
    }
    finally {
        reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return bytes;
}
function response(payload, status) {
    return Response.json(payload, { status, headers: {
            'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
        } });
}
/** Only known analytics calls can reach the configured API. Browser credentials are not forwarded. */
export async function proxy(request, segments, options = {}) {
    const env = options.env || process.env;
    const fetcher = options.fetch || fetch;
    const fail = (status, detail) => response({ detail }, status);
    try {
        const config = settings(env);
        if (!Array.isArray(segments) || !segments.length ||
            segments.some(part => !IDENTIFIER.test(part) || part === '.' || part === '..')) {
            return fail(404, 'This page data is not available.');
        }
        const path = segments.join('/');
        const method = request.method.toUpperCase();
        const knownRead = READS.has(path) || /^routes\/[A-Za-z0-9_.:-]+\/(metrics|coverage)$/.test(path);
        if (!knownRead && path !== 'what-if')
            return fail(404, 'This page data is not available.');
        if (!(knownRead && method === 'GET' || path === 'what-if' && method === 'POST')) {
            return fail(405, 'This action is not available.');
        }
        if (request.headers.has('authorization'))
            return fail(400, 'Browser credentials are not accepted.');
        const url = new URL(request.url);
        if (url.search.length > 2048)
            return fail(414, 'The request is too large.');
        let body;
        if (method === 'POST') {
            if (request.headers.get('origin') !== config.origin)
                return fail(403, 'Open this action from the application.');
            if (!request.headers.get('content-type')?.toLowerCase().startsWith('application/json'))
                return fail(415, 'Use a JSON request.');
            try {
                body = await boundedBody(request.body);
            }
            catch (error) {
                if (error instanceof RangeError)
                    return fail(413, 'The request is too large.');
                throw error;
            }
        }
        const token = await (PUBLIC.has(path) ? publicKey(env) : analyticsKey(env));
        const upstream = await fetcher(`${config.base}/api/v1/${path}${url.search}`, {
            method, headers: { Accept: 'application/json', Authorization: `Bearer ${token}`,
                ...(method === 'POST' ? { 'Content-Type': 'application/json' } : {}) },
            body: body?.byteLength ? body : undefined, cache: 'no-store', redirect: 'manual',
            signal: AbortSignal.timeout(12000),
        });
        if (upstream.status >= 300 && upstream.status < 400)
            return fail(502, 'The API returned an unsupported response.');
        if (!upstream.ok) {
            const messages = {
                401: 'The API service key needs attention. Please check the server settings.',
                403: 'The API service key cannot read this data.',
                404: 'No data was found for this selection.',
                422: 'Please check the selected values and their permitted limits.',
                429: 'Too many requests. Please try again shortly.',
                503: 'This data is unavailable at present. Check Pipeline for details.',
            };
            return fail(upstream.status, messages[upstream.status] || 'The API could not complete this request.');
        }
        try {
            const payload = JSON.parse(new TextDecoder().decode(await boundedBody(upstream.body, MAX_RESPONSE)));
            return response(payload, upstream.status);
        }
        catch {
            return fail(502, 'The API returned an unsupported response.');
        }
    }
    catch {
        return fail(503, 'Could not reach the API. Check that the Docker services are running.');
    }
}
