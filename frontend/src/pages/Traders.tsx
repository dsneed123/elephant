import { useEffect, useState } from 'react'
import { api } from '../api'
import type { TrackedTrader, TraderPnl } from '../types'
import { useToast } from '../contexts/ToastContext'

function timeAgo(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

const TIER_COLOR: Record<string, string> = {
  top_001: 'bg-yellow-500/20 text-yellow-300',
  top_01: 'bg-purple-500/20 text-purple-400',
  top_1: 'bg-blue-500/20 text-blue-400',
  top_10: 'bg-zinc-700 text-zinc-400',
}

type SortKey = 'elephant_score' | 'win_rate' | 'total_profit' | 'total_trades' | 'pnl'

export default function Traders() {
  const { push: toast } = useToast()
  const [traders, setTraders] = useState<TrackedTrader[]>([])
  const [pnlMap, setPnlMap] = useState<Record<string, TraderPnl>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('elephant_score')
  const [scraping, setScraping] = useState(false)
  const [togglingId, setTogglingId] = useState<number | null>(null)
  const [search, setSearch] = useState('')

  useEffect(() => {
    Promise.all([api.traders.list(), api.portfolio.traderPnl()])
      .then(([tradersData, pnlData]) => {
        setTraders(tradersData)
        const map: Record<string, TraderPnl> = {}
        for (const entry of pnlData) map[entry.kalshi_username] = entry
        setPnlMap(map)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setLoading(false)
      })
  }, [])

  const filtered = search.trim()
    ? traders.filter(
        (t) =>
          t.kalshi_username.toLowerCase().includes(search.toLowerCase()) ||
          (t.display_name ?? '').toLowerCase().includes(search.toLowerCase()),
      )
    : traders

  const sorted = [...filtered].sort((a, b) => {
    if (sortKey === 'pnl') {
      const aPnl = pnlMap[a.kalshi_username]?.total_pnl ?? -Infinity
      const bPnl = pnlMap[b.kalshi_username]?.total_pnl ?? -Infinity
      return bPnl - aPnl
    }
    return b[sortKey] - a[sortKey]
  })

  const handleToggleEnabled = (trader: import('../types').TrackedTrader) => {
    setTogglingId(trader.id)
    api.traders
      .patch(trader.id, { is_enabled: !trader.is_enabled })
      .then((updated) => {
        setTraders((prev) => prev.map((t) => (t.id === updated.id ? updated : t)))
        toast('success', `${updated.kalshi_username} ${updated.is_enabled ? 'enabled' : 'disabled'}`)
      })
      .catch((e: Error) => toast('error', e.message))
      .finally(() => setTogglingId(null))
  }

  const handleScrape = () => {
    setScraping(true)
    api.traders
      .scrape()
      .then((r) => {
        toast('success', `Scraped ${r.scraped} traders`)
        return api.traders.list()
      })
      .then(setTraders)
      .catch((e: Error) => toast('error', e.message))
      .finally(() => setScraping(false))
  }

  const SortBtn = ({ k, label }: { k: SortKey; label: string }) => (
    <button
      onClick={() => setSortKey(k)}
      className={`text-xs px-2.5 py-1 rounded-md transition-colors ${
        sortKey === k
          ? 'bg-emerald-600 text-white'
          : 'bg-zinc-800 text-zinc-400 hover:text-white'
      }`}
    >
      {label}
    </button>
  )

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Tracked Traders</h1>
        <div className="flex items-center gap-3">
          <button
            onClick={handleScrape}
            disabled={scraping}
            className="text-sm px-4 py-1.5 rounded-md bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors"
          >
            {scraping ? 'Scraping…' : 'Scrape Now'}
          </button>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-[180px] max-w-xs">
          <svg
            className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-500 pointer-events-none"
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth={2}
          >
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
          <input
            type="text"
            placeholder="Search traders…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full bg-zinc-800 border border-zinc-700 rounded-md pl-8 pr-3 py-1.5 text-sm text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-zinc-500"
          />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-zinc-500">Sort by:</span>
          <SortBtn k="elephant_score" label="Score" />
          <SortBtn k="win_rate" label="Win Rate" />
          <SortBtn k="total_profit" label="Profit" />
          <SortBtn k="total_trades" label="Trades" />
          <SortBtn k="pnl" label="P&L" />
        </div>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="px-5 py-10 text-center text-zinc-500 text-sm">Loading…</div>
        ) : error ? (
          <div className="px-5 py-10 text-center text-red-400 text-sm">{error}</div>
        ) : sorted.length === 0 ? (
          <div className="px-5 py-10 text-center text-zinc-600 text-sm">
            {search ? `No traders matching "${search}".` : 'No traders yet. Click "Scrape Now" to fetch from Kalshi.'}
          </div>
        ) : (
          <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-zinc-500 uppercase border-b border-zinc-800">
                <th className="px-5 py-3 text-left">#</th>
                <th className="px-5 py-3 text-left">Username</th>
                <th className="px-5 py-3 text-left">Tier</th>
                <th className="px-5 py-3 text-right">Score</th>
                <th className="px-5 py-3 text-right">Win Rate</th>
                <th className="px-5 py-3 text-right">Total Profit</th>
                <th className="px-5 py-3 text-right">Trades</th>
                <th className="px-5 py-3 text-right">Copied P&L</th>
                <th className="px-5 py-3 text-right">ROI</th>
                <th className="px-5 py-3 text-left">Last Active</th>
                <th className="px-5 py-3 text-center">Active</th>
                <th className="px-5 py-3 text-center">Enabled</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {sorted.map((t, i) => (
                <tr key={t.id} className="hover:bg-zinc-800/50 transition-colors">
                  <td className="px-5 py-3 text-zinc-600 text-xs">{i + 1}</td>
                  <td className="px-5 py-3">
                    <div className="font-medium text-zinc-200">{t.kalshi_username}</div>
                    {t.display_name && (
                      <div className="text-xs text-zinc-500">{t.display_name}</div>
                    )}
                  </td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-xs px-2 py-0.5 rounded-full ${
                        TIER_COLOR[t.tier] ?? 'bg-zinc-700 text-zinc-400'
                      }`}
                    >
                      {t.tier}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-emerald-400">
                    {t.elephant_score.toFixed(1)}
                  </td>
                  <td className="px-5 py-3 text-right text-zinc-300">
                    {(t.win_rate * 100).toFixed(1)}%
                  </td>
                  <td
                    className={`px-5 py-3 text-right font-mono ${
                      t.total_profit >= 0 ? 'text-emerald-400' : 'text-red-400'
                    }`}
                  >
                    {t.total_profit >= 0 ? '+' : ''}${t.total_profit.toFixed(2)}
                  </td>
                  <td className="px-5 py-3 text-right text-zinc-400">{t.total_trades}</td>
                  <td
                    className={`px-5 py-3 text-right font-mono ${
                      pnlMap[t.kalshi_username] === undefined
                        ? 'text-zinc-600'
                        : pnlMap[t.kalshi_username].total_pnl >= 0
                        ? 'text-emerald-400'
                        : 'text-red-400'
                    }`}
                  >
                    {pnlMap[t.kalshi_username] !== undefined ? (
                      <>
                        {pnlMap[t.kalshi_username].total_pnl >= 0 ? '+' : ''}
                        ${pnlMap[t.kalshi_username].total_pnl.toFixed(2)}
                      </>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-5 py-3 text-right text-zinc-400">
                    {pnlMap[t.kalshi_username] !== undefined ? (
                      <span
                        className={
                          pnlMap[t.kalshi_username].roi >= 0
                            ? 'text-emerald-400'
                            : 'text-red-400'
                        }
                      >
                        {(pnlMap[t.kalshi_username].roi * 100).toFixed(1)}%
                      </span>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-5 py-3 text-zinc-500 text-xs" title={t.last_seen ? new Date(t.last_seen).toLocaleString() : undefined}>
                    {t.last_seen ? timeAgo(t.last_seen) : '—'}
                  </td>
                  <td className="px-5 py-3 text-center">
                    <span
                      className={`inline-block w-2 h-2 rounded-full ${
                        t.is_active ? 'bg-emerald-400' : 'bg-zinc-600'
                      }`}
                    />
                  </td>
                  <td className="px-5 py-3 text-center">
                    <button
                      disabled={togglingId === t.id}
                      onClick={() => handleToggleEnabled(t)}
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none disabled:opacity-50 disabled:cursor-not-allowed ${
                        t.is_enabled ? 'bg-emerald-600' : 'bg-zinc-600'
                      }`}
                      aria-label={t.is_enabled ? 'Disable trader' : 'Enable trader'}
                    >
                      <span
                        className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform ${
                          t.is_enabled ? 'translate-x-4' : 'translate-x-1'
                        }`}
                      />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        )}
      </div>
    </div>
  )
}
