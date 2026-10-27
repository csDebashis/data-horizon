// A receipt is measured by performance.now(). No browser wall-clock value grants freshness.
const nonnegative = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const instant = value => typeof value === 'string' && /(?:Z|[+-]\d{2}:\d{2})$/.test(value) ? Date.parse(value) : NaN;
export function snapshotClock(snapshot, receivedAt, monotonicNow) {
    const input = snapshot || {};
    const elapsed = nonnegative(receivedAt) && nonnegative(monotonicNow) && monotonicNow >= receivedAt ? (monotonicNow - receivedAt) / 1000 : null;
    const age = nonnegative(input.freshness_seconds) && elapsed !== null ? input.freshness_seconds + elapsed : null;
    const asOf = instant(input.as_of);
    const budget = typeof input.stale_after_seconds === 'number' && Number.isFinite(input.stale_after_seconds) && input.stale_after_seconds > 0 ? input.stale_after_seconds : null;
    const valid = age !== null && Number.isFinite(asOf) && budget !== null;
    return { ...input, freshness_seconds: age, stale: input.stale !== false || !valid || age > budget, stale_after_seconds: budget, elapsed_seconds: elapsed, now_ms: valid ? asOf + age * 1000 : null };
}
