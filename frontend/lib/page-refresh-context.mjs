'use client';
import { createContext, useContext, useEffect, useId } from 'react';
/** @typedef {{path:string|null,pending:boolean,fetchedAt:string|null,error:string|null}} PageFetchSnapshot */
/** @typedef {{scope:string,revision:number,track:(id:string,snapshot:PageFetchSnapshot|null)=>void,pending:boolean}} PageFetchContextValue */
/** @type {import('react').Context<PageFetchContextValue|null>} */
export const PageFetchContext = createContext(null);
export function usePageScope() {
    return useContext(PageFetchContext)?.scope || 'shell';
}
export function usePageRevision() {
    return useContext(PageFetchContext)?.revision || 0;
}
/** @param {PageFetchSnapshot} snapshot */
export function usePageFetchRegistration(snapshot) {
    const context = useContext(PageFetchContext);
    const track = context?.track;
    const id = useId();
    const { path, pending, fetchedAt, error } = snapshot;
    useEffect(() => {
        track?.(id, path ? { path, pending, fetchedAt, error } : null);
    }, [track, id, path, pending, fetchedAt, error]);
    useEffect(() => () => track?.(id, null), [track, id]);
}
export function usePageRefreshPending() {
    return useContext(PageFetchContext)?.pending || false;
}
