// Fetch completion is a browser receipt fact, separate from transit source time.
export const emptyPageFetchState = () => ({ requests: {}, lastSuccessfulAt: null });
const validInstant = value => typeof value === 'string' && Number.isFinite(Date.parse(value));
export function updatePageFetchState(state, id, snapshot) {
    const previous = state.requests[id];
    const next = snapshot?.path ? {
        path: snapshot.path, pending: snapshot.pending === true,
        fetchedAt: validInstant(snapshot.fetchedAt) ? snapshot.fetchedAt : null,
        error: typeof snapshot.error === 'string' && snapshot.error ? snapshot.error : null,
    } : null;
    if ((!previous && !next) || (previous && next && Object.keys(next).every(key => previous[key] === next[key])))
        return state;
    const requests = { ...state.requests };
    if (next)
        requests[id] = next;
    else
        delete requests[id];
    const rows = Object.values(requests);
    const complete = rows.length > 0 && rows.every(row => !row.pending && !row.error && row.fetchedAt);
    const pathChanged = !!next && (!previous || previous.path !== next.path);
    const lastSuccessfulAt = complete
        ? rows.reduce((latest, row) => !latest || Date.parse(row.fetchedAt) > Date.parse(latest) ? row.fetchedAt : latest, null)
        : !rows.length || pathChanged ? null : state.lastSuccessfulAt;
    return { requests, lastSuccessfulAt };
}
export function pageFetchSummary(state) {
    const requests = Object.values(state.requests);
    return {
        count: requests.length,
        pending: requests.some(request => request.pending),
        failed: requests.some(request => !!request.error),
        lastSuccessfulAt: state.lastSuccessfulAt,
    };
}
export function shouldRefreshPage(summary, visible, alreadyRequested = false) {
    return visible && summary.count > 0 && !summary.pending && !alreadyRequested;
}
