'use client';
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import Paper from '@mui/material/Paper';
import CircularProgress from '@mui/material/CircularProgress';
import SyncRounded from '@mui/icons-material/SyncRounded';
import { localTime, viewerZone } from './format.mjs';
import { PageFetchContext } from './page-refresh-context.mjs';
import { emptyPageFetchState, pageFetchSummary, shouldRefreshPage, updatePageFetchState } from './page-refresh-state.mjs';
export { usePageScope, usePageRevision, usePageFetchRegistration, usePageRefreshPending } from './page-refresh-context.mjs';
type Snapshot = {
    path: string | null;
    pending: boolean;
    fetchedAt: string | null;
    error: string | null;
};
type RequestState = {
    requests: Record<string, Snapshot>;
    lastSuccessfulAt: string | null;
};
type Summary = {
    count: number;
    pending: boolean;
    failed: boolean;
    lastSuccessfulAt: string | null;
};
const StatusContext = createContext<Summary>({ count: 0, pending: false, failed: false, lastSuccessfulAt: null });
/** One provider tracks requests on the visible page. */
export function PageDataRefresh({ children, onRefresh, scope, revision = 0, onPendingChange }: {
    children: React.ReactNode;
    onRefresh: () => void;
    scope: string;
    revision?: number;
    onPendingChange?: (pending: boolean) => void;
}) {
    const [state, setState] = useState<RequestState>(emptyPageFetchState);
    const summary = useMemo(() => pageFetchSummary(state), [state]);
    const current = useRef(summary);
    const refresh = useRef(onRefresh);
    const pendingChanged = useRef(onPendingChange);
    const requested = useRef(false);
    const track = useCallback((id: string, snapshot: Snapshot | null) => {
        setState(previous => updatePageFetchState(previous, id, snapshot));
    }, []);
    useEffect(() => { refresh.current = onRefresh; }, [onRefresh]);
    useEffect(() => { pendingChanged.current = onPendingChange; }, [onPendingChange]);
    useEffect(() => {
        current.current = summary;
        requested.current = summary.pending;
        pendingChanged.current?.(summary.pending);
    }, [summary]);
    useEffect(() => {
        const tick = () => {
            if (!shouldRefreshPage(current.current, document.visibilityState === 'visible', requested.current))
                return;
            requested.current = true;
            refresh.current();
        };
        let visible = document.visibilityState === 'visible';
        let timer: number | undefined = visible ? window.setInterval(tick, 30000) : undefined;
        const visibilityChanged = () => {
            const nextVisible = document.visibilityState === 'visible';
            if (nextVisible === visible)
                return;
            visible = nextVisible;
            window.clearInterval(timer);
            timer = undefined;
            if (visible) {
                tick();
                timer = window.setInterval(tick, 30000);
            }
        };
        document.addEventListener('visibilitychange', visibilityChanged);
        return () => {
            window.clearInterval(timer);
            document.removeEventListener('visibilitychange', visibilityChanged);
        };
    }, [scope]);
    const value = useMemo(() => ({ scope, revision, track, pending: summary.pending }), [scope, revision, track, summary.pending]);
    return <PageFetchContext.Provider value={value}><StatusContext.Provider value={summary}>{children}</StatusContext.Provider></PageFetchContext.Provider>;
}
export function PageRefreshStatus() {
    const status = useContext(StatusContext);
    return <Paper variant="outlined" data-testid="page-fetch-status" aria-live="polite" aria-atomic="true" sx={{ px: 2, py: 1.25, borderColor: status.failed ? 'warning.main' : 'divider' }}>
        <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, alignItems: { xs: 'flex-start', md: 'center' }, justifyContent: 'space-between' }}>
            <Stack direction="row" sx={{ gap: 1, alignItems: 'center', minWidth: 0 }}>
                {status.pending ? <CircularProgress size={14} aria-label="Updating page data"/> : <SyncRounded sx={{ fontSize: 17, color: status.failed ? 'warning.main' : 'text.secondary' }}/>}
                <Typography variant="caption" color={status.failed ? 'warning.main' : 'text.secondary'}>
                    {status.failed ? 'Some data could not be refreshed. Use Refresh data to try again.' : status.pending ? 'Updating data…' : status.count ? 'Page data is up to date with the last fetch.' : 'No live data to refresh on this page.'}
                </Typography>
            </Stack>
            {status.lastSuccessfulAt && <Typography variant="caption" color="text.secondary" sx={{ overflowWrap: 'anywhere' }}>
                Last successful fetch: <time data-testid="page-last-fetched" dateTime={status.lastSuccessfulAt} title={viewerZone()}>{localTime(status.lastSuccessfulAt)}</time>
            </Typography>}
        </Stack>
        {status.count > 0 && <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: .5 }}>Automatic updates every 30 seconds while this page is visible. Source-data age is shown separately.</Typography>}
    </Paper>;
}
