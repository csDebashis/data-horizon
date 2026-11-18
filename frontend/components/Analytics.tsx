'use client';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Box, Button, Chip, Stack, TextField, Typography } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import { api, query, useApi } from '../lib/client';
import { number, localTime, readingAverage } from '../lib/format.mjs';
import { useSnapshotClock } from '../lib/useSnapshotClock';
import { Choice, Heading, Loading, Problem, StationChoice } from './Controls';
import { DataTable, Metrics, Panel, Row, StationPlot, TimeChart } from './DataViews';
import { filterPlatformCoverage } from '../lib/coverage.mjs';
import { usePageScope } from '../lib/page-refresh';
import { routeFiltersFromHistory, routeHistoryState } from '../lib/route-state.mjs';
import { healthPresentation } from '../lib/health.mjs';
const twoColumns = { display: 'grid', gridTemplateColumns: { xs: 'minmax(0,1fr)', lg: 'repeat(2,minmax(0,1fr))' }, gap: 2.5 };
function LastValidHealth({ reading }: { reading: Row }) {
    if (reading.status !== 'last_valid') return null;
    return <Stack sx={{ mt: .75, gap: .25, color: 'warning.main', overflowWrap: 'anywhere' }} data-testid="last-valid-health">
      <Typography variant="caption" component="div" sx={{ fontWeight: 700 }}>Last valid · {number(Math.floor(reading.ageSeconds), ' seconds old')}</Typography>
      <Typography variant="caption" component="div">Source: {localTime(reading.asOf)}</Typography>
      <Typography variant="caption" component="div" color="text.secondary">{reading.reason}</Typography>
    </Stack>;
}
export function Network({ routes, refresh }: {
    routes: Row[];
    refresh: number;
}) {
    const health = useApi<Row>('/network/health', refresh);
    const alerts = useApi<Row[]>('/alerts', refresh);
    const anomalies = useApi<Row[]>('/anomalies', refresh);
    const [showStations, setShowStations] = useState(false);
    const stops = useApi<Row[]>(showStations ? '/stops?limit=2000' : null, refresh);
    const alertClock = useSnapshotClock(alerts.meta, alerts.receivedAt);
    const healthClock = useSnapshotClock(health.meta, health.receivedAt);
    const anomalyClock = useSnapshotClock(anomalies.meta, anomalies.receivedAt);
    const reading = healthPresentation(health.data, health.receivedAt, healthClock.monotonicNow, healthClock.now_ms);
    if (health.loading)
        return <Loading label="Loading network information…"/>;
    return <Stack sx={{ gap: 2.5 }}>
    <Heading title="Network overview" subtitle="See route health, unusual delays and public service alerts."/>
    <Problem message={health.error || alerts.error || anomalies.error}/>
    <Metrics items={[
            { label: 'Network health', value: number(reading.score, ' / 100'),
              tone: reading.status === 'last_valid' ? 'warning' : undefined,
              caption: <LastValidHealth reading={reading}/> },
            { label: 'Routes covered', value: String(routes.length) },
            { label: 'Active service alerts', value: alertClock.stale ? 'Unavailable' : String((alerts.data || []).length) },
            { label: 'Unusual route-stop readings', value: healthClock.stale ? 'Unavailable' : number(health.data?.anomaly_count) },
        ]}/>
    <Typography variant="caption" color="text.secondary">The health score is calculated from delay and train intervals. If inputs pause, an available last valid score is kept for up to five minutes with its original source time. No previous valid score means Unavailable. It is not an official MTA rating.</Typography>
    <Box sx={twoColumns}>
      <Panel title="Route summary" subtitle="Choose a route to view its delay and headway charts.">
        <Stack sx={{ gap: 1 }}>{(health.data?.routes || routes).map((row: Row) => {
          const routeReading = healthPresentation(row, health.receivedAt, healthClock.monotonicNow, healthClock.now_ms);
          return <Box component="a" href={'/?' + new URLSearchParams({ view: 'route', route: String(row.route_id) })} key={row.route_id} aria-label={`Analyse Route ${row.route_id}`} sx={{ display: 'flex', alignItems: 'center', gap: 1.5, p: 1.5, borderRadius: 2,
                border: '1px solid', borderColor: 'divider', textDecoration: 'none', '&:hover': { bgcolor: 'action.hover' } }}>
            <Chip size="small" label={row.route_id} color="primary"/>
            <Typography variant="body2" sx={{ flex: 1 }}>{row.route_long_name || `Route ${row.route_id}`}</Typography>
            <Box sx={{ textAlign: 'right', minWidth: 0, maxWidth: { xs: 180, sm: 270 } }}><Typography variant="body2" sx={{ fontWeight: 700, color: routeReading.status === 'last_valid' ? 'warning.main' : 'text.primary' }}>{number(routeReading.score, ' / 100')}</Typography>
              <Typography variant="caption" color="text.secondary">{routeReading.status === 'unavailable' ? 'No recent health score' : routeReading.level}</Typography>
              <LastValidHealth reading={routeReading}/></Box>
          </Box>;
        })}</Stack>
        {!routes.length && <Alert severity="info">Route information will appear after collection and processing.</Alert>}
      </Panel>
      <Panel title="Unusual readings" subtitle="Up to eight readings are shown here. The card above gives the full count.">
        <Stack sx={{ gap: 1.5 }}>
          {anomalyClock.stale && <Alert severity="warning">Unusual-reading data is old or unavailable. Any rows below are last known readings.</Alert>}
          <Typography variant="caption" color="text.secondary">Source updated: {localTime(anomalyClock.as_of)} · Age: {number(anomalyClock.freshness_seconds, ' seconds')}</Typography>
          <DataTable rows={(anomalies.data || []).slice(0, 8)} columns={['route_id', 'stop_id', 'delay_mean_seconds', 'health_level']} title="Unusual readings" empty={anomalyClock.stale ? 'Current unusual-reading status is unavailable.' : 'No unusual readings are available. Missing data does not prove normal service.'}/>
        </Stack>
      </Panel>
    </Box>
    <Panel title="Service alerts" subtitle="Public notices from the source feed, with up to 20 records shown.">
      <Stack sx={{ gap: 1.5 }}>
        {alertClock.stale && <Alert severity="warning">Alert data is old or unavailable. Any notices below are the last known information.</Alert>}
        <Typography variant="caption" color="text.secondary">Source updated: {localTime(alertClock.as_of)} · Age: {number(alertClock.freshness_seconds, ' seconds')}</Typography>
        <DataTable rows={(alerts.data || []).slice(0, 20)} columns={['route_ids', 'header', 'description', 'start_time', 'end_time']} title="Service alerts" empty={alertClock.stale ? 'Current service-alert status is unavailable.' : 'No active service alerts in this dataset.'}/>
      </Stack>
    </Panel>
    <Panel title="Station coverage" subtitle="An optional coordinate plot of stations. These are not live train positions.">
      <Button variant="outlined" onClick={() => setShowStations(value => !value)} aria-expanded={showStations}>
        {showStations ? 'Hide stations' : 'Show stations'}
      </Button>
      {showStations && <Stack sx={{ gap: 2, mt: 2 }}>
        <Problem message={stops.error}/>
        {stops.loading ? <Loading label="Loading station coordinates…"/> : <>
          <StationPlot points={stops.data || []}/>
          <DataTable rows={(stops.data || []).map(row => ({ ...row, latitude: row.stop_lat, longitude: row.stop_lon }))} columns={['stop_name', 'stop_id', 'latitude', 'longitude']} title="Station coverage"/>
        </>}
      </Stack>}
    </Panel>
  </Stack>;
}
export function RouteDetails({ route, refresh }: { route: string; refresh: number }) {
    const scope = usePageScope();
    const [filters, setFilters] = useState(() => routeFiltersFromHistory(
      typeof window === 'undefined' ? null : window.history.state, route, scope));
    const { platform: stop, search, status } = filters;
    const setStop = (platform: string) => setFilters(previous => ({ ...previous, platform }));
    const setSearch = (search: string) => setFilters(previous => ({ ...previous, search }));
    const setStatus = (status: string) => setFilters(previous => ({ ...previous, status }));
    useEffect(() => {
      try {
        window.history.replaceState(routeHistoryState(window.history.state, route, scope, filters), '', window.location.href);
      } catch { /* Filtering still works when the browser cannot save entry state. */ }
    }, [route, scope, filters]);
    const coverage = useApi<Row>(`/routes/${encodeURIComponent(route)}/coverage?limit=2000`, refresh);
    const platforms: Row[] = useMemo(() => (coverage.data?.stops || []).map((row: Row) => ({
      ...row, data_mode: coverage.meta?.mode || 'unavailable',
    })), [coverage.data?.stops, coverage.meta?.mode]);
    useEffect(() => {
      if (coverage.data && stop && !platforms.some(row => String(row.stop_id) === stop)) {
        setFilters(previous => ({ ...previous, platform: '' }));
      }
    }, [coverage.data, platforms, stop]);
    const counts = coverage.data?.counts || {};
    const shown = filterPlatformCoverage(platforms, search, status);
    const metrics = useApi<Row[]>(query(`/routes/${encodeURIComponent(route)}/metrics`, { stop_id: stop, limit: 1000 }), refresh);
    const rows = metrics.data || [];
    const name = stop ? platforms.find(row => String(row.stop_id) === stop)?.stop_name || stop : 'All platforms';
    return <Stack sx={{ gap: 2.5 }}>
      <Heading title="Route analysis" subtitle="See every platform in the route catalogue, then inspect recent readings. Headway means the time gap between trains."/>
      <Problem message={coverage.error}/>
      {coverage.loading ? <Loading label="Loading route platform coverage…"/> : !coverage.data ?
        <Alert severity="warning">The platform catalogue could not be loaded. Coverage is unavailable. Use Refresh data to try again.</Alert> : <Stack sx={{ gap: 2.5 }}>
        {coverage.meta?.mode === 'synthetic' && <Alert severity="info">Synthetic mode uses a small 5-platform example network, not the full NYC catalogue. Live mode uses the full configured route catalogue.</Alert>}
        {coverage.data?.truncated && <Alert severity="warning">Coverage is incomplete: {coverage.data.returned} of {counts.catalogue} platforms were returned. Check the source scope or API limit.</Alert>}
        <Metrics items={[
          { label: 'Catalogue platforms', value: number(counts.catalogue) },
          { label: 'With fresh reading', value: number(counts.fresh) },
          { label: 'With old reading', value: number(counts.stale) },
          { label: 'No reading', value: number(counts.no_reading) },
          { label: 'With exact-match delay', value: number(counts.exact_delay_available) },
        ]}/>
        <Panel title="Platform coverage" subtitle="One row per catalogue platform, including quiet platforms. Sorted by name and ID, not travel order. Counts refer to platforms, not physical stations.">
          <Stack sx={{ gap: 2 }}>
            <Box sx={twoColumns}>
              <TextField size="small" label="Search platforms" value={search} onChange={event => setSearch(event.target.value)}
                slotProps={{ htmlInput: { maxLength: 100 } }} helperText="Search by name, platform ID or parent station"/>
              <Choice id="coverage-status" label="Reading status" value={status} onChange={setStatus} options={[
                { value: 'all', label: 'All statuses' }, { value: 'fresh', label: 'Fresh reading' },
                { value: 'stale', label: 'Old reading' }, { value: 'no_reading', label: 'No reading' },
              ]}/>
            </Box>
            <Typography variant="caption" color="text.secondary" data-testid="coverage-context">Route {route} · Showing {shown.length} of {platforms.length} returned platforms · Fresh means source-data age at fetch is within {number(coverage.data?.stale_after_seconds, ' seconds')}.</Typography>
            {coverage.data?.catalogue_as_of && <Typography variant="caption" color="text.secondary">Catalogue updated: {localTime(coverage.data.catalogue_as_of)}</Typography>}
            <DataTable rows={shown} columns={['stop_name', 'stop_id', 'reading_status', 'last_reading_at',
              'delay_mean_seconds', 'headway_seconds', 'health_level']} exportColumns={[
                'data_mode', 'route_id', 'stop_name', 'stop_id', 'parent_station', 'direction_id', 'reading_status', 'last_reading_at',
                'source_as_of', 'freshness_seconds', 'delay_mean_seconds', 'headway_seconds', 'scheduled_headway_seconds',
                'health_score', 'health_level', 'time_basis',
              ]} title="Platform coverage" empty="No platforms match these filters. Clear the search or choose All statuses."/>
            <Typography variant="caption" color="text.secondary">Old and missing readings have unavailable current metrics. No reading does not mean no train service. Reliable delay requires an exact timetable match.</Typography>
          </Stack>
        </Panel>
      </Stack>}
      <Panel title="Recent reading history" subtitle="A separate, bounded history of up to 1,000 metric records. This is not the route catalogue.">
        <Stack sx={{ gap: 2.5 }}>
          <StationChoice label="Platform" id="detail-stop" value={stop} disabled={coverage.loading || !coverage.data} options={[
            { value: '', label: 'All platforms' }, ...platforms.map(row => ({ value: String(row.stop_id), label: `${row.stop_name} (${row.stop_id})` })),
          ]} onChange={setStop}/>
          <Problem message={metrics.error}/>
          {metrics.loading ? <Loading label="Loading selected platform history…"/> : <Stack sx={{ gap: 2.5 }} data-testid="route-results">
            <Typography variant="caption" color="text.secondary" data-testid="selection-context">Showing Route {route} · {name} · {rows.length} history records · local time</Typography>
            {rows.length ? <>
              <Metrics items={[
                { label: 'Average timetable difference', value: number(readingAverage(rows, 'delay_mean_seconds'), ' seconds') },
                { label: 'Average train interval', value: number(readingAverage(rows, 'headway_seconds'), ' seconds') },
                { label: 'Average recorded health', value: number(readingAverage(rows, 'health_score'), ' / 100') },
              ]}/>
              <Typography variant="caption" color="text.secondary">Each displayed historical reading has equal weight in these averages. The same train may appear in more than one reading. These are summaries of recorded platform readings, not a train-level average or current route status.</Typography>
              <Alert severity="info">Timetable difference compares the arrival time with an exact timetable match. Negative means earlier than the timetable; positive means later. Missing exact matches are Unavailable. Train intervals can still be shown. Feed predictions are estimates, not measured actual arrivals.</Alert>
              <Box sx={twoColumns}>
                <Panel title="Timetable comparison" subtitle="Average platform readings at each snapshot time. Negative is earlier; positive is later."><TimeChart rows={rows} fields={{ delay_mean_seconds: 'Average timetable difference' }} units="Timetable difference (seconds)"/></Panel>
                <Panel title="Train intervals" subtitle="Average recorded train interval compared with the scheduled interval at each snapshot time."><TimeChart rows={rows} fields={{ headway_seconds: 'Recorded interval', scheduled_headway_seconds: 'Scheduled interval' }} units="Headway (seconds)"/></Panel>
              </Box>
              <DataTable rows={rows} columns={['window_start', 'stop_id', 'direction_id', 'delay_mean_seconds', 'headway_seconds',
                'scheduled_headway_seconds', 'health_score', 'health_level', 'time_basis', 'event_count']} title="Route analytics"/>
            </> : <Alert severity="info">No recent history is available for this selection. The platform remains in the catalogue above.</Alert>}
          </Stack>}
        </Stack>
      </Panel>
    </Stack>;
}
/** Show one returned directed path so the graph result is easy to understand. */
function GraphPreview({ affected }: {
    affected: Row[];
}) {
    const theme = useTheme();
    const paths = affected.map(row => row.path).filter(Array.isArray);
    const longest = [...paths].sort((first, second) => second.length - first.length)[0] || [];
    const nodes: string[] = longest.slice(0, 8).map(String);
    if (nodes.length < 2)
        return null;
    return <Box sx={{ overflowX: 'auto' }}>
    <Typography variant="caption" color="text.secondary">One directed connection path{longest.length > 8 ? ' (first eight nodes)' : ''}. Arrows show the allowed travel direction.</Typography>
    <svg role="img" aria-label="Directed station connection path" viewBox={`0 0 ${nodes.length * 140} 100`} style={{ minWidth: nodes.length * 110, width: '100%', height: 110 }}>
      <defs><marker id="path-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
        <path d="M 0 0 L 10 5 L 0 10 z" fill={theme.palette.primary.main}/>
      </marker></defs>
      {nodes.map((node, index) => <g key={`${node}:${index}`}>
        {index < nodes.length - 1 && <path d={`M ${index * 140 + 86} 35 H ${index * 140 + 180}`} stroke={theme.palette.primary.main} strokeWidth="2" markerEnd="url(#path-arrow)"/>}
        <circle cx={index * 140 + 65} cy="35" r="18" fill={theme.palette.primary.main}/>
        <text x={index * 140 + 65} y="40" textAnchor="middle" fill={theme.palette.primary.contrastText} fontSize="12">{index + 1}</text>
        <text x={index * 140 + 65} y="78" textAnchor="middle" fill={theme.palette.text.primary} fontSize="11">{node}</text>
      </g>)}
    </svg>
  </Box>;
}
export function Scenarios({ route, refresh }: {
    route: string;
    refresh: number;
}) {
    const stops = useApi<Row[]>(query('/stops', { route_id: route, limit: 2000 }), refresh);
    const options = useMemo(() => (stops.data || []).map(row => ({ value: String(row.stop_id), label: `${row.stop_name} (${row.stop_id})` })), [stops.data]);
    const [origin, setOrigin] = useState('');
    const [delay, setDelay] = useState(10);
    const [horizon, setHorizon] = useState(30);
    const [limit, setLimit] = useState(20);
    const [result, setResult] = useState<Row | null>(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const controller = useRef<AbortController | null>(null);
    useEffect(() => () => controller.current?.abort(), []);
    const selected = origin || String(stops.data?.[0]?.stop_id || '');
    async function submit(event: React.FormEvent) {
        event.preventDefault();
        controller.current?.abort();
        const request = new AbortController();
        controller.current = request;
        setBusy(true);
        setError(null);
        setResult(null);
        try {
            const response = await api('/what-if', 'POST', { route_id: route, stop_id: selected,
                delay_seconds: delay * 60, horizon_minutes: horizon, limit }, request.signal);
            if (!request.signal.aborted)
                setResult(response.data);
        }
        catch (failure) {
            if (!request.signal.aborted)
                setError((failure as Error).message);
        }
        finally {
            if (!request.signal.aborted)
                setBusy(false);
        }
    }
    const affected = (result?.affected_stations || []).map((row: Row) => ({ ...row,
        station: stops.data?.find(stop => String(stop.stop_id) === String(row.stop_id))?.stop_name || row.stop_id }));
    return <Stack sx={{ gap: 2.5 }}>
    <Heading title="Graph and what-if" subtitle="A directed graph connects stations in the allowed travel direction. Use it to explore how a delay may spread."/>
    <Alert severity="info">This is a simplified graph calculation. It is not an official forecast or live train movement.</Alert>
    <Panel title="Try a delay scenario" subtitle="Choose the starting station and the delay to test.">
      <Box component="form" onSubmit={submit}>
        <Stack sx={{ gap: 2.5 }}>
          <Box sx={twoColumns}>
            <StationChoice label="Origin stop" id="scenario-origin" value={selected} options={options} onChange={setOrigin} disabled={stops.loading}/>
            <TextField type="number" label="Delay (minutes)" value={delay} onChange={event => setDelay(Number(event.target.value))} required slotProps={{ htmlInput: { min: 0, max: 120, step: 1 } }}/>
            <TextField type="number" label="Time horizon (minutes)" value={horizon} onChange={event => setHorizon(Number(event.target.value))} required helperText="How far ahead the graph should look" slotProps={{ htmlInput: { min: 1, max: 60, step: 1 } }}/>
            <TextField type="number" label="Maximum stations" value={limit} onChange={event => setLimit(Number(event.target.value))} required slotProps={{ htmlInput: { min: 1, max: 100, step: 1 } }}/>
          </Box>
          <Box><Button type="submit" variant="contained" disabled={busy || !selected}>{busy ? 'Calculating…' : 'Run scenario'}</Button></Box>
        </Stack>
      </Box>
    </Panel>
    <Problem message={error} retryHint="Use Run scenario to try the calculation again."/>
    <Problem message={stops.error}/>
    {result && <Panel title="Scenario results" subtitle={`Origin: Route ${result.origin?.route_id}, stop ${result.origin?.stop_id} · Submitted delay: ${number(result.delay_seconds / 60, ' minutes')} · Time horizon: ${result.horizon_minutes} minutes`}>
      <Stack sx={{ gap: 2 }}>
        <GraphPreview affected={affected}/>
        <DataTable rows={affected} columns={['route_id', 'station', 'stop_id', 'travel_seconds', 'estimated_delay_seconds', 'path']} title="Scenario results" empty="No connected stations fall within this time horizon."/>
        {(Array.isArray(result.limitations) ? result.limitations : [result.limitations]).filter(Boolean).map((text: string, index: number) => <Typography variant="caption" color="text.secondary" key={index}>{text}</Typography>)}
      </Stack>
    </Panel>}
  </Stack>;
}
export function Pipeline({ refresh }: {
    refresh: number;
}) {
    const pipeline = useApi<Row>('/pipeline/status', refresh);
    if (pipeline.loading)
        return <Loading label="Loading pipeline measurements…"/>;
    const data = pipeline.data || {};
    const counts = data.counts || {};
    const streaming = data.streaming || {};
    const sources = Object.entries(data.sources || {}).map(([source, value]) => ({ source,
        ...(typeof value === 'object' && value ? value : { status: value }) }));
    return <Stack sx={{ gap: 2.5 }}>
    <Heading title="Pipeline" subtitle="Check whether collection and processing are working, and see the measured data quality."/>
    <Problem message={pipeline.error}/>
    <Metrics items={[
            { label: 'Recent events', value: number(counts.events) }, { label: 'Metric rows', value: number(counts.metrics) },
            { label: 'Records kept aside as invalid', value: number(counts.quarantine) }, { label: 'Processed batches', value: number(counts.batches) },
        ]}/>
    <Panel title="Processing measurements" subtitle="Missing measurements are shown as unavailable, not zero.">
      <Stack sx={{ gap: 2 }}>
        <Metrics items={[
            { label: 'Input rate', value: number(streaming.input_rows_per_second, ' events/second', 1) },
            { label: 'Kafka lag', value: number(streaming.kafka_lag, ' events') },
            { label: 'Batch duration', value: number(streaming.batch_duration_ms, ' ms') },
        ]}/>
        <Typography variant="body2">Kafka lag is the number of messages still waiting to be processed.</Typography>
        <Typography variant="caption" color="text.secondary">Serving database: {data.engine || 'Unavailable'} · Last progress: {localTime(streaming.last_progress_at)} · Processing rate: {number(streaming.processed_rows_per_second, ' events/second', 1)}</Typography>
        {streaming.query_active === false && <Alert severity="warning">The streaming query is not active. Check the processor logs.</Alert>}
      </Stack>
    </Panel>
    <Panel title="Source status" subtitle="Last successful collection time for each data source.">
      <DataTable rows={sources} columns={['source', 'status', 'last_success', 'reason']} title="Source status"/>
    </Panel>
  </Stack>;
}
export function AboutDataHorizon() {
    return <Stack sx={{ gap: 2.5 }}>
    <Heading title="About Data Horizon" subtitle="A transit data platform for route analytics, network health and data processing."/>
    <Panel title="What Data Horizon does">
      <Typography variant="body2">Data Horizon collects NYC subway schedules and public feed updates. Kafka carries the events. Spark checks and processes them, builds route and station summaries, and writes results to PostgreSQL. The API provides those results to this dashboard.</Typography>
    </Panel>
    <Panel title="Data flow">
      <Stack component="ol" sx={{ m: 0, pl: 2.5, gap: 1 }}>
        {['Collector: download feeds and save the original source files.',
            'Kafka: keep different types of events in separate topics. A topic is a named stream of messages.',
            'Spark processor: validate events, calculate metrics and build the directed graph.',
            'Storage: keep the file-based data lake for replay and PostgreSQL tables for dashboard queries.',
            'FastAPI and Next.js: return bounded results and display charts and tables.'].map(text => <Typography component="li" key={text} variant="body2">{text}</Typography>)}
      </Stack>
    </Panel>
    <Panel title="How to use the pages">
      <DataTable download={false} title="Page guide" columns={['page', 'purpose']} rows={[
            { page: 'Network overview', purpose: 'Start with route health, unusual readings and service alerts.' },
            { page: 'Route analysis', purpose: 'Choose a route and station. Compare timetable differences and train intervals; download CSV records.' },
            { page: 'Graph and what-if', purpose: 'Enter a delay and see connected stations and directed paths.' },
            { page: 'Pipeline', purpose: 'Check source collection, invalid records, processing speed and Kafka lag.' },
        ]}/>
    </Panel>
    <Typography variant="body2" color="text.secondary">Data updates every 30 seconds on the visible page. Refresh data updates the data without reloading the page. All displayed times use your device timezone. Use Appearance to choose Daylight, Midnight or your device theme.</Typography>
  </Stack>;
}
