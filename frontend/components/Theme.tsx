'use client';
import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { viewerZone } from '../lib/format.mjs';
import { DISPLAY_KEY, displayPreferences, resolvedScheme } from '../lib/display-preferences.mjs';
type DisplayChoice = {
    scheme: string;
};
const ViewerZone = createContext<string | null>(null);
const DisplayContext = createContext<{
    choice: DisplayChoice;
    setChoice: (choice: DisplayChoice) => void;
}>({
    choice: { scheme: 'dark' }, setChoice: () => { },
});
export const useViewerZone = () => useContext(ViewerZone);
export const useDisplayPreferences = () => useContext(DisplayContext);
function materialTheme(mode: 'light' | 'dark') {
    const dark = mode === 'dark';
    return createTheme({
        palette: {
            mode, primary: { main: dark ? '#59DACC' : '#006B62', contrastText: dark ? '#082E2A' : '#FFFFFF' },
            secondary: { main: dark ? '#BAA7F2' : '#6650A4' },
            background: { default: dark ? '#0B1321' : '#F3F6FA', paper: dark ? '#142136' : '#FFFFFF' },
            text: { primary: dark ? '#EDF3FB' : '#172B3C', secondary: dark ? '#ABC0D4' : '#496175' },
            divider: dark ? '#33465D' : '#CBD7E1',
            ...(dark ? { warning: { main: '#FFD18B' }, error: { main: '#FFA8AE' }, success: { main: '#74DAB2' } } : {}),
        },
        typography: {
            fontFamily: 'ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
            h1: { fontSize: '1.4rem', fontWeight: 750 }, h2: { fontSize: '1.8rem', fontWeight: 750, letterSpacing: '-.5px' },
            h3: { fontSize: '1rem', fontWeight: 700 }, body2: { fontSize: '.9rem', lineHeight: 1.75 },
            caption: { fontSize: '.78rem', lineHeight: 1.7 }, button: { textTransform: 'none', fontWeight: 650 },
        },
        shape: { borderRadius: 12 },
        components: {
            MuiPaper: { defaultProps: { elevation: 0 }, styleOverrides: { root: { backgroundImage: 'none' } } },
            MuiButton: { defaultProps: { disableElevation: true }, styleOverrides: { root: { minHeight: 44, borderRadius: 8 } } },
            MuiTableCell: { styleOverrides: { root: { fontSize: 12, verticalAlign: 'top', padding: '12px 14px' },
                    head: { background: dark ? '#1C304A' : '#EAF1F7', fontWeight: 700, whiteSpace: 'nowrap' } } },
            MuiOutlinedInput: { styleOverrides: { root: { background: dark ? '#101D2F' : '#FFFFFF', borderRadius: 8 } } },
            MuiAlert: { styleOverrides: { root: { borderRadius: 8 } } },
            MuiChip: { styleOverrides: { root: { fontWeight: 650 } } },
        },
    });
}
export default function Theme({ children }: {
    children: React.ReactNode;
}) {
    const [zone, setZone] = useState<string | null>(null);
    const [choice, setDisplay] = useState<DisplayChoice>({ scheme: 'dark' });
    const [systemDark, setSystemDark] = useState(true);
    useEffect(() => {
        const updateZone = () => setZone(viewerZone());
        updateZone();
        document.addEventListener('visibilitychange', updateZone);
        try {
            setDisplay(displayPreferences(JSON.parse(localStorage.getItem(DISPLAY_KEY) || '{}')));
        }
        catch { }
        const media = window.matchMedia('(prefers-color-scheme: dark)');
        const updateScheme = () => setSystemDark(media.matches);
        updateScheme();
        media.addEventListener('change', updateScheme);
        return () => { document.removeEventListener('visibilitychange', updateZone); media.removeEventListener('change', updateScheme); };
    }, []);
    const mode = resolvedScheme(choice.scheme, systemDark) as 'light' | 'dark';
    useEffect(() => {
        document.documentElement.dataset.colorScheme = mode;
        document.documentElement.style.colorScheme = mode;
    }, [mode]);
    function setChoice(value: DisplayChoice) {
        const safe = displayPreferences(value);
        setDisplay(safe);
        try {
            localStorage.setItem(DISPLAY_KEY, JSON.stringify(safe));
        }
        catch { }
    }
    const theme = useMemo(() => materialTheme(mode), [mode]);
    return <ThemeProvider theme={theme}><CssBaseline />
    <DisplayContext.Provider value={{ choice, setChoice }}><ViewerZone.Provider value={zone}>{children}</ViewerZone.Provider></DisplayContext.Provider>
  </ThemeProvider>;
}
