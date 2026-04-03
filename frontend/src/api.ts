import type {
  TrackedTrader,
  TradeSignal,
  CopiedTrade,
  PortfolioPerformance,
  PortfolioSnapshot,
  TraderPnl,
  AppSettings,
} from './types'

const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

export const api = {
  health: () => get<{ status: string; service: string }>('/health'),

  traders: {
    list: () => get<TrackedTrader[]>('/traders/'),
    top: (limit = 10) => get<TrackedTrader[]>(`/traders/top?limit=${limit}`),
    scrape: () =>
      fetch(`${BASE}/traders/scrape`, { method: 'POST' }).then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        return r.json() as Promise<{ scraped: number; timestamp: string }>
      }),
  },

  signals: {
    list: (status?: string) =>
      get<TradeSignal[]>(`/signals/${status ? `?status=${status}` : ''}`),
    pending: () => get<TradeSignal[]>('/signals/pending'),
    execute: (id: number) =>
      fetch(`${BASE}/signals/${id}/execute`, { method: 'POST' }).then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        return r.json() as Promise<CopiedTrade>
      }),
    dismiss: (id: number) =>
      fetch(`${BASE}/signals/${id}/dismiss`, { method: 'POST' }).then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        return r.json() as Promise<TradeSignal>
      }),
  },

  portfolio: {
    trades: (limit = 50) => get<CopiedTrade[]>(`/portfolio/trades?limit=${limit}`),
    performance: () => get<PortfolioPerformance>('/portfolio/performance'),
    snapshots: (limit = 100) =>
      get<PortfolioSnapshot[]>(`/portfolio/snapshots?limit=${limit}`),
    traderPnl: () => get<TraderPnl[]>('/portfolio/traders'),
  },

  settings: {
    get: () => get<AppSettings>('/settings/'),
    patch: (data: Partial<AppSettings>) =>
      fetch(`${BASE}/settings/`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      }).then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`)
        return r.json() as Promise<AppSettings>
      }),
  },
}
