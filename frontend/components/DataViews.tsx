'use client';
import { useTheme } from '@mui/material/styles';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Paper from '@mui/material/Paper';
import Stack from '@mui/material/Stack';
import Button from '@mui/material/Button';
import Alert from '@mui/material/Alert';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import DownloadOutlined from '@mui/icons-material/DownloadOutlined';
import { csv, number, localTime, localStamp, localClock, localText, localise, viewerZone, series, validStationPoints } from '../lib/format.mjs';
export type Row = Record<string, any>;
const LABELS: Record<string, string> = { reading_status: 'Reading status', last_reading_at: 'Last reading (local)', source_as_of: 'Source updated (local)', freshness_seconds: 'Source age (seconds)', parent_station: 'Parent station', scheduled_headway_seconds: 'Scheduled headway (seconds)', route_id: 'Route', route_ids: 'Routes', route_long_name: 'Route name', stop_id: 'Stop ID', stop_name: 'Station', station: 'Station', direction: 'Direction', direction_id: 'Direction ID', window_start: 'Window start (local)', delay_mean_seconds: 'Mean delay (seconds)', headway_seconds: 'Headway (seconds)', health_score: 'Health score (/100)', health_level: 'Health level', time_basis: 'Time basis', event_count: 'Events', estimated_arrival: 'Feed estimate (local)', minutes_until: 'Minutes away', match_confidence: 'Match confidence', travel_seconds: 'Travel time (seconds)', estimated_delay_seconds: 'Estimated delay (seconds)', header: 'Notice', description: 'Alert', details: 'Details', start_time: 'Valid from (local)', end_time: 'Valid until (local)', severity: 'Severity', source: 'Source', status: 'Status', last_success: 'Last success (local)', latitude: 'Latitude', longitude: 'Longitude' };
function display(value: any, key: string) {
    if (value === null || value === undefined || value === '')
        return 'Unavailable';
    if (['window_start', 'estimated_arrival', 'last_success', 'start_time', 'end_time', 'created_at', 'last_reading_at', 'source_as_of'].includes(key))
        return localTime(value);
    if (key === 'reading_status')
        return ({ fresh: 'Fresh', stale: 'Old', no_reading: 'No reading' } as Record<string, string>)[value] || 'Unknown';
    if (Array.isArray(value))
        return value.join(', ');
    if (typeof value === 'object')
        return JSON.stringify(localise(value));
    if (typeof value === 'boolean')
        return value ? 'Yes' : 'No';
    if (typeof value === 'number')
        return number(value, '', Number.isInteger(value) ? 0 : 1);
    return localText(value);
}
export function DataTable({ rows, columns, exportColumns, title, empty = 'No records are available for this selection.', download = true }: {
    rows: Row[];
    columns?: string[];
    exportColumns?: string[];
    title: string;
    empty?: string;
    download?: boolean;
}) {
    const fields = (columns || Object.keys(rows[0] || {})).filter(key => rows.some(row => key in row));
    function exportRows() { const url = URL.createObjectURL(new Blob([csv(rows, exportColumns || fields)], { type: 'text/csv;charset=utf-8' })); const a = document.createElement('a'); a.href = url; a.download = title.toLowerCase().replace(/[^a-z0-9]+/g, '-') + '.csv'; a.click(); setTimeout(() => URL.revokeObjectURL(url), 0); }
    if (!rows.length)
        return <Alert severity="info">{empty}</Alert>;
    return <Box><Stack direction="row" sx={[{ mb: 1 }, { justifyContent: "flex-end" }]}>{download && <Button size="small" startIcon={<DownloadOutlined />} onClick={exportRows} aria-label={`Download ${title} CSV`}>Download CSV</Button>}</Stack><TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 420, overflow: 'auto' }}><Table stickyHeader size="small" aria-label={title}><TableHead><TableRow>{fields.map(key => <TableCell key={key}>{LABELS[key] || key.replaceAll('_', ' ')}</TableCell>)}</TableRow></TableHead><TableBody>{rows.map((row, i) => <TableRow key={String(row.id || row.stop_id || i) + '-' + i} hover>{fields.map(key => <TableCell key={key} sx={{ minWidth: ['description', 'header', 'details'].includes(key) ? 260 : undefined, verticalAlign: 'top', whiteSpace: ['window_start', 'estimated_arrival'].includes(key) ? 'nowrap' : undefined }}>{display(row[key], key)}</TableCell>)}</TableRow>)}</TableBody></Table></TableContainer></Box>;
}
export function Panel({ title, subtitle, children, icon, action }: {
    title: string;
    subtitle?: string;
    children: React.ReactNode;
    icon?: React.ReactNode;
    action?: React.ReactNode;
}) { return <Paper variant="outlined" sx={{ p: { xs: 2, md: 2.5 }, height: '100%', minWidth: 0 }}><Stack direction="row" sx={[{ mb: subtitle ? 1 : 2 }, { alignItems: "center", justifyContent: "space-between", gap: 1 }]}><Stack direction="row" sx={{ gap: 1, alignItems: "center" }}>{icon && <Box sx={{ color: 'primary.main', display: 'flex' }}>{icon}</Box>}<Typography component="h3" variant="h3">{title}</Typography></Stack>{action}</Stack>{subtitle && <Typography variant="caption" color="text.secondary" component="p" sx={{ mb: 2 }}>{subtitle}</Typography>}{children}</Paper>; }
export function Metrics({ items }: {
    items: {
        label: string;
        value: string;
        caption?: React.ReactNode;
        tone?: 'warning';
    }[];
}) { return <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: 'repeat(2,minmax(0,1fr))', lg: `repeat(${Math.min(items.length, 5)},minmax(0,1fr))` }, gap: 2 }}>{items.map((item, i) => <Paper variant="outlined" key={item.label} data-testid="metric-card" sx={{ p: 2.4, minWidth: 0, borderTop: `2px solid ${['#3FDFCC', '#719EFF', '#AA91FF', '#5BD9B6'][i % 4]}`, minHeight: 132, position: 'relative', overflow: 'hidden', background: theme => theme.palette.mode === 'dark' ? 'radial-gradient(ellipse at top right,#3C5B9529,transparent 72%),linear-gradient(145deg,#152840,#0C1A2F)' : 'linear-gradient(145deg,#FFFFFF,#EAF3F8)', boxShadow: '0 12px 36px #0002' }}><Typography variant="caption" color="text.secondary" component="div">{item.label}</Typography><Typography data-testid="metric-value" sx={{ fontSize: { xs: 25, lg: 29 }, fontWeight: 750, letterSpacing: '-1px', color: item.tone === 'warning' ? 'warning.main' : i === 0 ? 'primary.main' : 'text.primary', mt: .6, overflowWrap: 'anywhere' }}>{item.value}</Typography>{item.caption}</Paper>)}</Box>; }
export function TimeChart({ rows, fields, units }: {
    rows: Row[];
    fields: Record<string, string>;
    units: string;
}) {
    const chartTheme = useTheme();
    const axis = chartTheme.palette.text.secondary;
    const grid = chartTheme.palette.divider;
    const zone = viewerZone();
    const data = series(rows, Object.keys(fields)) as Row[];
    const values = data.flatMap(row => Object.keys(fields).map(k => row[k]).filter(x => x !== null)) as number[];
    if (!data.length || !values.length)
        return <Alert severity="info">Values are unavailable. Missing measurements have not been replaced with zero.</Alert>;
    const width = 560, height = 270, left = 58, right = 20, top = 20, bottom = 58;
    const t0 = data[0].time, t1 = data.at(-1)!.time;
    const min = Math.min(0, ...values), max = Math.max(1, ...values);
    const x = (t: number) => left + (t - t0) / (t1 - t0 || 1) * (width - left - right);
    const y = (v: number) => top + (max - v) / (max - min || 1) * (height - top - bottom);
    const keys = Object.keys(fields);
    const colors = chartTheme.palette.mode === 'dark' ? ['#44E5D1', '#AA98FA'] : ['#006B62', '#6450A5'];
    return <Box className="material-chart"><svg role="img" aria-label={`${units} over Time (${zone})`} viewBox={`0 0 ${width} ${height}`} style={{ width: '100%', height: 270, display: 'block' }}>{[0, 1, 2, 3, 4].map(i => { const value = min + (max - min) * i / 4; return <g key={i}><line x1={left} y1={y(value)} x2={width - right} y2={y(value)} stroke={grid}/><text x={left - 10} y={y(value) + 4} textAnchor="end" fill={axis} fontSize="11">{number(value, '', Math.abs(value) < 10 ? 1 : 0)}</text></g>; })}{[0, 1, 2, 3].map(i => { const t = t0 + (t1 - t0) * i / 3; const label = localClock(t, t1 - t0 < 300000, zone); return <text key={i} x={x(t)} y={height - 29} textAnchor="middle" fill={axis} fontSize="11">{label}</text>; })}{keys.map((key, index) => <g key={key}><polyline fill="none" stroke={colors[index % 2]} strokeWidth="2.5" strokeDasharray={index ? '6 4' : undefined} points={data.filter(row => row[key] !== null).map(row => `${x(row.time)},${y(row[key])}`).join(' ')}/>{data.filter(row => row[key] !== null).map((row, i) => <circle key={i} cx={x(row.time)} cy={y(row[key])} r="3" fill={colors[index % 2]}><title>{`${localStamp(row.time, zone)} · ${fields[key]}: ${number(row[key], '', 1)}`}</title></circle>)}</g>)}<text x={width / 2} y={height - 5} textAnchor="middle" fill={axis} fontSize="12">Time ({zone})</text><text transform={`translate(13 ${height / 2}) rotate(-90)`} textAnchor="middle" fill={axis} fontSize="11">{units}</text></svg><Stack direction="row" sx={[{ mt: 1 }, { gap: 2, justifyContent: "center", flexWrap: "wrap" }]}>{keys.map((key, i) => <Stack direction="row" key={key} sx={{ gap: .7, alignItems: "center" }}><Box sx={{ width: 18, borderTop: `3px ${i ? 'dashed' : 'solid'} ${colors[i % 2]}` }}/><Typography variant="caption" color="text.secondary">{fields[key]}</Typography></Stack>)}</Stack></Box>;
}
export function StationPlot({ points }: {
    points: Row[];
}) {
    const theme = useTheme();
    const axis = theme.palette.text.secondary;
    const grid = theme.palette.divider;
    const data = validStationPoints(points) as Row[];
    if (!data.length)
        return <Alert severity="info">Station coordinates are not available yet.</Alert>;
    const lng = data.map(p => Number(p.stop_lon)), lat = data.map(p => Number(p.stop_lat));
    const minX = Math.min(...lng), maxX = Math.max(...lng), minY = Math.min(...lat), maxY = Math.max(...lat);
    const x = (v: number) => 62 + (v - minX) / (maxX - minX || 1) * 460, y = (v: number) => 324 - (v - minY) / (maxY - minY || 1) * 290;
    return <svg role="img" aria-label="Offline station coordinates. These are station locations, not train GPS positions." viewBox="0 0 560 380" style={{ width: '100%', height: 360, display: 'block' }}>{[0, 1, 2, 3, 4].map(i => <g key={i}><line x1="62" x2="522" y1={34 + i * 72.5} y2={34 + i * 72.5} stroke={grid}/><line x1={62 + i * 115} x2={62 + i * 115} y1="34" y2="324" stroke={grid}/><text x="54" y={y(minY + (maxY - minY) * i / 4) + 4} textAnchor="end" fill={axis} fontSize="10">{number(minY + (maxY - minY) * i / 4, '', 2)}</text><text x={62 + i * 115} y="345" textAnchor="middle" fill={axis} fontSize="10">{number(minX + (maxX - minX) * i / 4, '', 2)}</text></g>)}{data.map(p => <circle key={p.stop_id} cx={x(Number(p.stop_lon))} cy={y(Number(p.stop_lat))} r="4" fill={theme.palette.primary.main} opacity=".8" tabIndex={0}><title>{`${p.stop_name} (${p.stop_id}) · ${p.stop_lat}, ${p.stop_lon}`}</title></circle>)}<text x="290" y="373" textAnchor="middle" fill={axis} fontSize="11">Longitude</text><text transform="translate(13 175) rotate(-90)" fill={axis} fontSize="11">Latitude</text></svg>;
}
