export function number(value, suffix = '', digits = 0) { if (value === null || value === undefined || value === '')
    return 'Unavailable'; const n = Number(value); return Number.isFinite(n) ? n.toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits }) + suffix : 'Unavailable'; }
export function viewerZone() { const zone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'; return zone === 'Asia/Calcutta' ? 'Asia/Kolkata' : zone; }
function dateValue(value) { if (value === null || value === undefined || value === '')
    return null; const date = new Date(value); return Number.isFinite(date.getTime()) ? date : null; }
function localParts(value, zone) { const date = dateValue(value); if (!date)
    return null; try {
    const parts = new Intl.DateTimeFormat('en-GB', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }).formatToParts(date);
    const fields = Object.fromEntries(parts.map(p => [p.type, p.value]));
    const name = new Intl.DateTimeFormat('en-US', { timeZone: zone, timeZoneName: 'longOffset' }).formatToParts(date).find(p => p.type === 'timeZoneName')?.value || 'GMT';
    const offset = name.replace(/^(GMT|UTC)/, '') || '+00:00';
    return { date: `${fields.year}-${fields.month}-${fields.day}`, clock: `${fields.hour}:${fields.minute}:${fields.second}`, offset };
}
catch {
    return null;
} }
export function localTime(value, zone = viewerZone()) { const p = localParts(value, zone); return p ? `${p.date} ${p.clock} ${p.offset}` : 'Unavailable'; }
export function localStamp(value, zone = viewerZone()) { const p = localParts(value, zone); if (!p)
    return 'Unavailable'; const original = typeof value === 'string' ? value.match(/T\d{2}:\d{2}:\d{2}(\.\d+)(?:Z|[+-]\d{2}:\d{2})$/) : null; const date = dateValue(value); const fraction = original ? original[1] : date && date.getMilliseconds() ? '.' + String(date.getMilliseconds()).padStart(3, '0') : ''; return `${p.date} ${p.clock}${fraction} ${p.offset} [${zone}]`; }
export function localClock(value, seconds = false, zone = viewerZone()) { const p = localParts(value, zone); return p ? (seconds ? p.clock : p.clock.slice(0, 5)) : 'Unavailable'; }
const ISO_INSTANT = /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\b/g;
export function localText(value, zone = viewerZone()) { return String(value ?? '').replace(ISO_INSTANT, instant => localStamp(instant, zone)); }
export function localise(value, zone = viewerZone(), depth = 0) { if (depth > 20)
    return '[Further nested details]'; if (typeof value === 'string')
    return localText(value, zone); if (Array.isArray(value))
    return value.map(item => localise(item, zone, depth + 1)); if (value && typeof value === 'object')
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key.replace(/_utc$/i, '_local'), /timezone|time_zone/i.test(key) && typeof item === 'string' ? zone : localise(item, zone, depth + 1)])); return value; }
export function csv(rows, columns, zone = viewerZone()) { const cell = value => { let prepared = localise(value, zone); let text = prepared === null || prepared === undefined ? '' : typeof prepared === 'object' ? JSON.stringify(prepared) : String(prepared); if (typeof prepared === 'string' && /^[=+\-@\t\r]/.test(text))
    text = "'" + text; return '"' + text.replaceAll('"', '""') + '"'; }; return [columns.map(cell).join(','), ...rows.map(row => columns.map(key => cell(row[key])).join(','))].join('\r\n'); }
/** Historical snapshots have equal weight; event counts are not matched-delay counts. */
export function readingAverage(rows, key) {
    const values = rows.map(row => row[key]).filter(value => value !== null && value !== undefined &&
        value !== '' && Number.isFinite(Number(value))).map(Number);
    return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
}
/** A missing measurement ends a line instead of implying a continuous observation. */
export function lineSegments(rows, key) {
    const segments = [];
    let current = [];
    for (const row of rows) {
        if (typeof row[key] === 'number' && Number.isFinite(row[key])) {
            current.push(row);
        } else if (current.length) {
            segments.push(current);
            current = [];
        }
    }
    if (current.length) segments.push(current);
    return segments;
}
export function series(rows, fields) { const times = new Map(); for (const row of rows) {
    const t = Date.parse(row.window_start);
    if (!Number.isFinite(t))
        continue;
    const values = times.get(t) || {};
    for (const key of fields) {
        const v = row[key];
        if (v !== null && v !== undefined && v !== '' && Number.isFinite(Number(v))) {
            const list = values[key] || [];
            list.push(Number(v));
            values[key] = list;
        }
    }
    times.set(t, values);
} return [...times].sort((a, b) => a[0] - b[0]).map(([time, values]) => ({ time, ...Object.fromEntries(fields.map(key => [key, values[key]?.length ? values[key].reduce((a, b) => a + b, 0) / values[key].length : null])) })); }
export function validStationPoints(rows) { const numeric = value => value !== null && value !== undefined && !(typeof value === 'string' && value.trim() === '') && Number.isFinite(Number(value)); return rows.filter(row => numeric(row.stop_lat) && numeric(row.stop_lon) && Math.abs(Number(row.stop_lat)) <= 90 && Math.abs(Number(row.stop_lon)) <= 180).map(row => ({ ...row, stop_lat: Number(row.stop_lat), stop_lon: Number(row.stop_lon) })); }
