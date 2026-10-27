'use client';
import { useEffect, useState } from 'react';
import { snapshotClock } from './live.mjs';
/** Repaint ages locally; network polling remains owned by the existing data hook. */
export function useSnapshotClock(snapshot: Record<string, any> | null | undefined, receivedAt: number | null | undefined) {
    const [sample, setSample] = useState<{
        receipt: number | null | undefined;
        now: number;
    } | null>(null);
    useEffect(() => {
        const update = () => { if (document.visibilityState === 'visible')
            setSample({ receipt: receivedAt, now: performance.now() }); };
        update();
        const timer = setInterval(update, 1000);
        document.addEventListener('visibilitychange', update);
        return () => { clearInterval(timer); document.removeEventListener('visibilitychange', update); };
    }, [receivedAt]);
    // A newly received snapshot can precede the next effect by one paint.
    const monotonicNow = sample && sample.receipt === receivedAt ? sample.now : receivedAt ?? NaN;
    return { ...snapshotClock(snapshot, receivedAt, monotonicNow), monotonicNow };
}
