'use client';
import { Box, FormControl, InputLabel, Select, MenuItem } from '@mui/material';
import { useDisplayPreferences } from './Theme';
/** A display preference affects only this browser. */
export default function DisplaySettings() {
    const { choice, setChoice } = useDisplayPreferences();
    return <Box sx={{ minWidth: 150 }}>
    <FormControl fullWidth size="small">
      <InputLabel id="appearance-label">Appearance</InputLabel>
      <Select labelId="appearance-label" label="Appearance" value={choice.scheme} onChange={event => setChoice({ ...choice, scheme: event.target.value })}>
        <MenuItem value="light">Daylight</MenuItem>
        <MenuItem value="dark">Midnight</MenuItem>
        <MenuItem value="system">Use device theme</MenuItem>
      </Select>
    </FormControl>
  </Box>;
}
