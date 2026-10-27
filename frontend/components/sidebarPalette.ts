import type { Theme } from '@mui/material/styles';
/** One navigation palette follows the same resolved Material theme as the page. */
export function sidebarPalette(theme: Theme) {
    const daylight = theme.palette.mode === 'light';
    return {
        surface: daylight ? '#EDF4F9' : '#071321',
        rail: daylight
            ? 'radial-gradient(ellipse at top,#D9F3EE88,transparent 60%),linear-gradient(180deg,#F6FAFD,#EDF4F9)'
            : 'radial-gradient(ellipse at top,#11394833,transparent 60%),linear-gradient(180deg,#091426,#060D1C)',
        text: theme.palette.text.primary,
        muted: daylight ? theme.palette.text.secondary : '#9AB4C8',
        navigation: daylight ? '#304E65' : '#BDCFDD',
        icon: daylight ? '#46667C' : '#91AEC3',
        selected: daylight
            ? 'linear-gradient(100deg,#D5EEE9,#DFEAF8)'
            : 'linear-gradient(100deg,#174751,#1B2947)',
        selectedHover: daylight
            ? 'linear-gradient(100deg,#C5E7E0,#D1E2F3)'
            : 'linear-gradient(100deg,#1D5460,#26395A)',
        selectedText: daylight ? '#064E48' : '#FFFFFF',
        selectedIcon: daylight ? theme.palette.primary.main : '#6DE5D0',
        accent: daylight ? theme.palette.primary.main : '#22D3BA',
        hover: daylight ? '#DDECF2' : '#FFFFFF0B',
        focus: daylight ? theme.palette.primary.main : '#60EEDB',
        divider: daylight ? theme.palette.divider : '#FFFFFF18',
        badgeText: daylight ? theme.palette.primary.contrastText : '#D7FFF5',
        badgeSurface: daylight ? theme.palette.primary.main : '#126B61',
        controlSurface: daylight ? theme.palette.background.paper : '#0C1E31',
        controlText: theme.palette.text.primary,
        controlBorder: daylight ? '#809AAF' : '#49657F',
        controlHover: daylight ? '#526E85' : '#84A9C3',
        controlIcon: daylight ? '#46667C' : '#B6D1E4',
        optionSurface: daylight ? theme.palette.background.paper : '#10213A',
        optionText: theme.palette.text.primary,
        refreshText: daylight ? '#24465F' : '#D7E6F0',
        refreshBorder: daylight ? '#809AAF' : '#7892A84D',
        refreshHover: daylight ? '#DDEFEA' : '#142E41',
        pendingText: daylight ? theme.palette.text.secondary : '#A4BDCE',
        pendingSurface: daylight ? '#E2ECEF' : '#0C1E31',
    };
}
