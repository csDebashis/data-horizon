'use client';
import { useEffect, useState } from 'react';
import { useTheme } from '@mui/material/styles';
import { Alert, Box, Button, Chip, Divider, Drawer, IconButton, List, ListItemButton, ListItemIcon, ListItemText, Stack, Typography } from '@mui/material';
import DashboardOutlined from '@mui/icons-material/DashboardOutlined';
import RouteOutlined from '@mui/icons-material/RouteOutlined';
import HubOutlined from '@mui/icons-material/HubOutlined';
import DnsOutlined from '@mui/icons-material/DnsOutlined';
import SchoolOutlined from '@mui/icons-material/SchoolOutlined';
import MenuRounded from '@mui/icons-material/MenuRounded';
import RefreshRounded from '@mui/icons-material/RefreshRounded';
import TrainRounded from '@mui/icons-material/TrainRounded';
import { useApi, type ApiSnapshot } from '../lib/client';
import { localTime, number } from '../lib/format.mjs';
import { useSnapshotClock } from '../lib/useSnapshotClock';
import { PageDataRefresh, PageRefreshStatus, usePageRefreshPending } from '../lib/page-refresh';
import { interceptWorkspaceLink, navigateWorkspace } from '../lib/workspace-navigation.mjs';
import { Network, Pipeline, RouteDetails, Scenarios, AboutDataHorizon } from './Analytics';
import { Loading, Problem } from './Controls';
import { Row } from './DataViews';
import { useViewerZone } from './Theme';
import { sidebarPalette } from './sidebarPalette';
import SidebarRouteFocus from './SidebarRouteFocus';
import DisplaySettings from './DisplaySettings';
type View = 'network' | 'route' | 'scenario' | 'pipeline' | 'about';
type ConfirmedStatus = Pick<ApiSnapshot<Row>, 'data' | 'meta' | 'receivedAt' | 'fetchedAt'>;
const PAGES = {
    network: { label: 'Network overview', icon: <DashboardOutlined /> },
    route: { label: 'Route analysis', icon: <RouteOutlined /> },
    scenario: { label: 'Graph and what-if', icon: <HubOutlined /> },
    pipeline: { label: 'Pipeline', icon: <DnsOutlined /> },
    about: { label: 'About', icon: <SchoolOutlined /> },
};
const VIEWS = Object.keys(PAGES) as View[];
export default function Dashboard() {
    const [view, setView] = useState<View>('network');
    const [route, setRoute] = useState('');
    const [ready, setReady] = useState(false);
    const [mode, setMode] = useState('');
    // Public source confirmation survives page remounts; keep its original
    // receipt so navigation cannot make its source age younger.
    const [confirmedStatus, setConfirmedStatus] = useState<ConfirmedStatus | null>(null);
    const [revision, setRevision] = useState(0);
    useEffect(() => {
        function restore() {
            const params = new URLSearchParams(window.location.search);
            const requested = params.get('view') as View;
            setView(VIEWS.includes(requested) ? requested : 'network');
            const selected = params.get('route');
            if (selected && /^[A-Za-z0-9_.:-]{1,64}$/.test(selected))
                setRoute(selected);
            setReady(true);
        }
        restore();
        window.addEventListener('popstate', restore);
        return () => window.removeEventListener('popstate', restore);
    }, []);
    function chooseRoute(selected: string) {
        setRoute(selected);
        navigateWorkspace('/?' + new URLSearchParams({ view, route: selected }));
    }
    return <Box onClick={interceptWorkspaceLink}>
    <a className="skip-link" href="#main-content">Skip to main content</a>
    <PageDataRefresh key={`${view}:${mode}`} scope={`${view}:${mode}`} revision={revision} onRefresh={() => setRevision(value => value + 1)}>
      <Workspace view={view} mode={mode} route={route} setRoute={setRoute} chooseRoute={chooseRoute} ready={ready} refresh={revision} refreshPage={() => setRevision(value => value + 1)} onMode={setMode} confirmedStatus={confirmedStatus} onConfirmedStatus={setConfirmedStatus}/>
    </PageDataRefresh>
  </Box>;
}
function Workspace({ view, mode, route, setRoute, chooseRoute, ready, refresh, refreshPage, onMode, confirmedStatus, onConfirmedStatus }: {
    view: View;
    mode: string;
    route: string;
    setRoute: (route: string) => void;
    chooseRoute: (route: string) => void;
    ready: boolean;
    refresh: number;
    refreshPage: () => void;
    onMode: (mode: string) => void;
    confirmedStatus: ConfirmedStatus | null;
    onConfirmedStatus: (snapshot: ConfirmedStatus) => void;
}) {
    const colours = sidebarPalette(useTheme());
    const zone = useViewerZone();
    const pending = usePageRefreshPending();
    const [drawer, setDrawer] = useState(false);
    const livePage = ready && view !== 'about';
    const status = useApi<Row>(livePage ? '/status' : null);
    // Resolve the source scope before a user can enter a form that a scope change resets.
    const sourceReady = !!mode && status.data?.mode === mode;
    const routes = useApi<Row[]>(livePage ? '/routes' : null);
    // Cache is for the header only. Forms still await this page's actual source
    // response, preserving the source-scope readiness and mode-change gates.
    const displayedStatus = status.data ? status : confirmedStatus;
    const clock = useSnapshotClock(displayedStatus?.data, displayedStatus?.receivedAt);
    const routeOptions = routes.data || [];
    useEffect(() => {
        if (status.data?.mode && ['live', 'recorded', 'synthetic'].includes(String(status.data.mode)) && status.receivedAt !== null) {
            onConfirmedStatus({ data: status.data, meta: status.meta, receivedAt: status.receivedAt, fetchedAt: status.fetchedAt });
            onMode(String(status.data.mode));
        }
    }, [status.data, status.meta, status.receivedAt, status.fetchedAt, onMode, onConfirmedStatus]);
    useEffect(() => {
        if (routeOptions.length && !routeOptions.some(item => String(item.route_id) === route)) {
            setRoute(String(routeOptions[0].route_id));
        }
    }, [routes.data, route, setRoute]); // eslint-disable-line react-hooks/exhaustive-deps
    function rail(id: string) {
        return <Stack data-testid="navigation-panel" sx={{ minHeight: '100%', p: 2.5, gap: 2,
                color: colours.text, background: colours.rail }}>
      <Stack direction="row" sx={{ gap: 1.5, alignItems: 'center', py: 1 }}>
        <Box sx={{ p: 1, bgcolor: 'primary.main', color: 'primary.contrastText', borderRadius: 2, display: 'flex' }}><TrainRounded /></Box>
        <Box><Typography sx={{ fontWeight: 750 }}>Data Horizon</Typography>
          <Typography variant="caption" sx={{ color: colours.muted }}>Transit data platform</Typography></Box>
      </Stack>
      <Divider sx={{ borderColor: colours.divider }}/>
      <List component="nav" aria-label="Main navigation" disablePadding>
        {VIEWS.map(item => <ListItemButton key={item} component="a" href={'/?' + new URLSearchParams({ view: item, ...(route ? { route } : {}) })} selected={view === item} aria-current={view === item ? 'page' : undefined} onClick={() => setDrawer(false)} sx={{ mb: .75, borderRadius: 2, color: colours.navigation,
                    '&:hover': { bgcolor: colours.hover }, '&:focus-visible': { outline: `2px solid ${colours.focus}`, outlineOffset: 2 },
                    '&.Mui-selected': { background: colours.selected, color: colours.selectedText,
                        '&:hover': { background: colours.selectedHover } } }}>
          <ListItemIcon sx={{ minWidth: 36, color: view === item ? colours.selectedIcon : colours.icon }}>{PAGES[item].icon}</ListItemIcon>
          <ListItemText primary={PAGES[item].label} slotProps={{ primary: { sx: { fontSize: 14, fontWeight: view === item ? 700 : 500 } } }}/>
        </ListItemButton>)}
      </List>
      {['route', 'scenario'].includes(view) && <SidebarRouteFocus id={id} route={route} routes={routeOptions as {
                route_id: string | number;
            }[]} onChange={chooseRoute}/>}
      <Box sx={{ flex: 1 }}/>
      <Button startIcon={<RefreshRounded />} variant="outlined" onClick={refreshPage} disabled={pending || !livePage} sx={{ color: colours.refreshText, borderColor: colours.refreshBorder,
                '&:hover': { bgcolor: colours.refreshHover }, '&.Mui-disabled': {
                    color: colours.pendingText, bgcolor: colours.pendingSurface, borderColor: colours.refreshBorder
                } }}>
        {pending ? 'Updating data…' : 'Refresh data'}
      </Button>
      <Typography variant="caption" sx={{ color: colours.muted }}>NYC Subway · Routes 1–6<br />Network analysis and service data</Typography>
    </Stack>;
    }
    return <Box sx={{ display: 'flex', minHeight: '100vh' }}>
    <Box component="aside" sx={{ width: { md: 250 }, flexShrink: { md: 0 } }}>
      <Drawer variant="permanent" open sx={{ display: { xs: 'none', md: 'block' },
            '& .MuiDrawer-paper': { width: 250, border: 0, bgcolor: colours.surface, overflowX: 'hidden' } }}>{rail('analysis-route')}</Drawer>
      <Drawer variant="temporary" open={drawer} onClose={() => setDrawer(false)} sx={{ display: { md: 'none' },
            '& .MuiDrawer-paper': { width: 270, bgcolor: colours.surface, overflowX: 'hidden' } }}>{rail('analysis-route-mobile')}</Drawer>
    </Box>
    <Box component="main" id="main-content" sx={{ flex: 1, minWidth: 0, p: { xs: 2, md: 3.5 }, maxWidth: 1600, mx: 'auto' }}>
      <Stack sx={{ gap: 2.5 }}>
        <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1.5 }}>
          <Stack direction="row" sx={{ alignItems: 'center', gap: 1 }}>
            <IconButton aria-label="Open navigation" onClick={() => setDrawer(true)} sx={{ display: { md: 'none' } }}><MenuRounded /></IconButton>
            <Box><Typography variant="h1" component="h1">Data Horizon</Typography>
              <Typography variant="caption" color="text.secondary">NYC subway data and analysis</Typography></Box>
          </Stack>
          <DisplaySettings />
        </Stack>
        {livePage && <>
          <Stack direction="row" sx={{ gap: 1, alignItems: 'center', flexWrap: 'wrap' }} data-testid="source-status">
            <Chip size="small" label={`Data: ${String(displayedStatus?.data?.mode || (status.error ? 'Unavailable' : 'Loading…'))}`} color={clock.stale || status.error ? 'warning' : 'default'}/>
            <Typography variant="caption" color="text.secondary">{displayedStatus?.data ? <>
              Source updated: {localTime(clock.as_of)} · Age: {number(clock.freshness_seconds, ' seconds')}
            </> : status.error ? 'Source status could not be confirmed.' : 'Waiting for first source confirmation…'}</Typography>
            <Chip size="small" variant="outlined" data-testid="local-zone" label={zone || 'Loading local timezone…'}/>
            {status.error && confirmedStatus && <Typography variant="caption" color="warning.main">Last confirmed source; latest status check failed.</Typography>}
          </Stack>
          {displayedStatus?.data && clock.stale && <Alert severity="warning">The source data is old or its age is unavailable. Results below may describe earlier service.</Alert>}
          <Problem message={status.error || routes.error}/>
        </>}
        <PageRefreshStatus />
        {!ready || !zone ? <Loading label="Preparing the dashboard…"/> :
            livePage && !sourceReady ? <Loading label="Loading the data source. Use Refresh data if this does not complete…"/> :
            view === 'network' ? <Network routes={routeOptions} refresh={refresh}/> :
                view === 'pipeline' ? <Pipeline refresh={refresh}/> :
                        view === 'about' ? <AboutDataHorizon /> :
                            !route ? <Loading label="Loading available routes…"/> :
                                view === 'route' ? <RouteDetails key={route} route={route} refresh={refresh}/> :
                                    <Scenarios key={route} route={route} refresh={refresh}/>}
      </Stack>
    </Box>
  </Box>;
}
