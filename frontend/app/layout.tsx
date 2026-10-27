import type { Metadata } from 'next';
import { AppRouterCacheProvider } from '@mui/material-nextjs/v16-appRouter';
import Theme from '../components/Theme';
import './styles.css';

export const metadata: Metadata = {
  title: 'Data Horizon | Transit data project',
  description: 'A project for NYC subway data processing and analysis.',
};
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return <html lang="en"><body><AppRouterCacheProvider><Theme>{children}</Theme></AppRouterCacheProvider></body></html>;
}
