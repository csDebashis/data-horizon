'use client';
import { useMemo } from 'react';
import FormControl from '@mui/material/FormControl';
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import Alert from '@mui/material/Alert';
import Skeleton from '@mui/material/Skeleton';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
export function Choice({ label, value, options, onChange, disabled = false, id }: {
    label: string;
    value: string;
    options: {
        value: string;
        label: string;
    }[];
    onChange: (value: string) => void;
    disabled?: boolean;
    id: string;
}) { return <FormControl fullWidth size="small" disabled={disabled}><InputLabel shrink htmlFor={id}>{label}</InputLabel><Select native notched label={label} value={value} onChange={e => onChange(e.target.value)} inputProps={{ id, 'aria-label': label }}>{options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}</Select></FormControl>; }
export function Loading({ label = 'Loading current information…' }: {
    label?: string;
}) { return <Stack data-testid="loading-state" aria-label={label} role="status" sx={{ gap: 1.5 }}><Typography variant="caption" color="text.secondary">{label}</Typography><Skeleton variant="rounded" height={90}/><Skeleton variant="rounded" height={180}/></Stack>; }
export function Problem({ message, retryHint = 'Use Refresh data to try loading this view again.' }: {
    message: string | null | undefined;
    retryHint?: string;
}) { return message ? <Alert severity="error" role="alert">{message} {retryHint}</Alert> : null; }
export function Heading({ title, subtitle }: {
    title: string;
    subtitle: string;
}) { return <Stack sx={[{ mb: .5 }, { gap: .8 }]}><Typography component="h2" variant="h2" color="text.primary">{title}</Typography><Typography variant="body2" color="text.secondary">{subtitle}</Typography></Stack>; }
export function StationChoice({ label, value, options, onChange, disabled = false, id }: {
    label: string;
    value: string;
    options: {
        value: string;
        label: string;
    }[];
    onChange: (value: string) => void;
    disabled?: boolean;
    id: string;
}) { const match = options.find(option => option.value === value); const selectedID = match?.value; const selectedLabel = match?.label || ''; const selected = useMemo(() => selectedID === undefined ? null : { value: selectedID, label: selectedLabel }, [selectedID, selectedLabel]); return <Autocomplete id={id} size="small" fullWidth disabled={disabled} options={options} value={selected} isOptionEqualToValue={(option, selected) => option.value === selected.value} getOptionLabel={option => option.label} onChange={(_, option) => onChange(option?.value || '')} autoHighlight renderInput={params => <TextField {...params} label={label} slotProps={{ ...params.slotProps, inputLabel: { ...params.slotProps.inputLabel, shrink: true } }} helperText="Search by station name or ID"/>} noOptionsText="No matching station. Try another name or ID."/>; }
