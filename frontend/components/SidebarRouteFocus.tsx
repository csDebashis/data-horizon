'use client';
import Box from '@mui/material/Box';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import FormLabel from '@mui/material/FormLabel';
import Select from '@mui/material/Select';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { sidebarPalette } from './sidebarPalette';
type Props = {
    id: string;
    route: string;
    routes: {
        route_id: string | number;
    }[];
    onChange: (route: string) => void;
};
/** Route controls share the resolved navigation palette without changing layout. */
export default function SidebarRouteFocus({ id, route, routes, onChange }: Props) {
    const colours = sidebarPalette(useTheme());
    return <Box data-testid="sidebar-route-focus" sx={{ mt: 2, minWidth: 0 }}>
        <Divider sx={{ borderColor: colours.divider, mb: 2 }}/>
        <Typography sx={{ fontSize: 9, color: colours.muted, letterSpacing: 1.4, mb: 1.5 }}>ROUTE FOCUS</Typography>
        <FormControl fullWidth size="small" sx={{ gap: 1 }}>
            <FormLabel htmlFor={id} sx={{ position: 'static', fontSize: 12, lineHeight: 1.5, color: colours.muted, '&.Mui-focused': { color: colours.focus } }}>Route</FormLabel>
            <Select native notched={false} value={route} onChange={event => onChange(event.target.value)} inputProps={{ id, 'aria-label': 'Route' }} sx={{ backgroundColor: colours.controlSurface, color: colours.controlText, borderRadius: 1.5,
            '& .MuiOutlinedInput-notchedOutline': { borderColor: colours.controlBorder },
            '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: colours.controlHover },
            '&.Mui-focused .MuiOutlinedInput-notchedOutline': { borderColor: colours.focus },
            '& .MuiNativeSelect-icon': { color: colours.controlIcon },
            '& select:focus': { backgroundColor: colours.controlSurface },
            '& select:focus-visible': { outline: `3px solid ${colours.focus}`, outlineOffset: 3 },
            '& option': { backgroundColor: colours.optionSurface, color: colours.optionText } }}>
                {routes.map(item => <option key={item.route_id} value={String(item.route_id)}>{String(item.route_id)}</option>)}
            </Select>
        </FormControl>
    </Box>;
}
