import { useCallback, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts'
import { api } from '../api'
import type { PortfolioPerformance, PortfolioSnapshot, TradeSignal } from '../types'
import { useWebSocket } from '../contexts/WebSocketContext'

function StatCard({
  label,
  value,
  sub,
  color = 'text-white',
}: {
  label: string
  value: string
  sub?: string
  color?: string
}) {
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
      <p className="text-xs text-zinc-500 uppercase tracking-wider mb-1">{label}</p>
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
      {sub && <p className="text-xs text-zinc-500 mt-1">{sub}</p>}
    </div>
  )
}

function fmt(n: number, digits = 2) {
  return n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function fmtDate(iso: string) {
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

const STATUS_COLOR: Record<string, string> = {
  pending: 'bg-amber-500/20 text-amber-400',
  copied: 'bg-emerald-500/20 text-emerald-400',
  skipped: 'bg-zinc-700 text-zinc-400',
  expired: 'bg-red-500/20 text-red-400',
}

const RefreshIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} className="w-4 h-4">
    <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
  </svg>
)

export default function Dashboard() {
  const [perf, setPerf] = useState<PortfolioPerformance | null>(null)
  const [snapshots, setSnapshots] = useState<PortfolioSnapshot[]>([])
  const [signals, setSignals] = useState<TradeSignal[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const { latestEvent } = useWebSocket()

  const load = useCallback((isRefresh = false) => {
    if (isRefresh) setRefreshing(true)
    setError(null)
    Promise.all([
      api.portfolio.performance(),
      api.portfolio.snapshots(100),
      api.signals.list(),
    ])
      .then(([p, s, sig]) => {
        setPerf(p)
        setSnapshots(s)
        setSignals(sig.slice(0, 8))
        setLastUpdated(new Date())
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => {
        setLoading(false)
        setRefreshing(false)
      })
  }, [])

  useEffect(() => {
    load()
  }, [load])

  useEffect(() => {
    if (!latestEvent) return
    if (latestEvent.type === 'signal_created') {
      const sig = latestEvent.payload as TradeSignal
      setSignals((prev) => [sig, ...prev].slice(0, 8))
    } else if (latestEvent.type === 'portfolio_snapshot') {
      const snap = latestEvent.payload as PortfolioSnapshot
      setSnapshots((prev) => [...prev, snap])
      setLastUpdated(new Date())
    }
  }, [latestEvent])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-zinc-500 text-sm">Loading…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-red-400 text-sm">Failed to load: {error}</p>
      </div>
    )
  }

  const chartData = snapshots.map((s) => ({
    time: fmtDate(s.created_at),
    value: s.total_value,
    pnl: s.total_pnl,
  }))

  const pnlColor = perf
    ? perf.total_pnl >= 0
      ? 'text-emerald-400'
      : 'text-red-400'
    : 'text-white'

  const pendingCount = signals.filter((s) => s.status === 'pending').length

  return (
    <div className="space-y-6">
      {/* Pending signals alert */}
      {pendingCount > 0 && (
        <Link
          to="/signals"
          className="flex items-center gap-3 bg-amber-500/10 border border-amber-500/30 rounded-xl px-5 py-3.5 hover:bg-amber-500/15 transition-colors group"
        >
          <span className="flex-shrink-0 w-2 h-2 rounded-full bg-amber-400 animate-pulse" />
          <span className="text-sm text-amber-300 flex-1">
            <span className="font-semibold">{pendingCount} pending signal{pendingCount > 1 ? 's' : ''}</span>
            {' '}waiting for review
          </span>
          <span className="text-xs text-amber-500 group-hover:text-amber-400 transition-colors">
            Review signals →
          </span>
        </Link>
      )}

      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold text-white">Portfolio Overview</h1>
          {perf && (
            <span
              className={`text-xs font-medium px-2.5 py-1 rounded-full ${
                perf.mode === 'paper'
                  ? 'bg-amber-500/20 text-amber-400'
                  : 'bg-emerald-500/20 text-emerald-400'
              }`}
            >
              {perf.mode === 'paper' ? 'Paper' : 'Live'}
            </span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {lastUpdated && (
            <span className="text-xs text-zinc-600">
              Updated {lastUpdated.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </span>
          )}
          <button
            onClick={() => load(true)}
            disabled={refreshing}
            className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 text-zinc-300 transition-colors"
          >
            <span className={refreshing ? 'animate-spin' : ''}>
              <RefreshIcon />
            </span>
            Refresh
          </button>
        </div>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Balance"
          value={perf ? `$${fmt(perf.balance)}` : '—'}
          sub={perf ? `Total value $${fmt(perf.total_value)}` : undefined}
        />
        <StatCard
          label="Total P&L"
          value={perf ? `${perf.total_pnl >= 0 ? '+' : ''}$${fmt(perf.total_pnl)}` : '—'}
          color={pnlColor}
        />
        <StatCard
          label="Win Rate"
          value={perf ? `${(perf.win_rate * 100).toFixed(1)}%` : '—'}
          color={
            perf
              ? perf.win_rate >= 0.65
                ? 'text-emerald-400'
                : 'text-amber-400'
              : 'text-white'
          }
        />
        <StatCard
          label="Total Trades"
          value={perf ? String(perf.total_trades) : '—'}
        />
      </div>

      {/* Chart */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <h2 className="text-sm font-medium text-zinc-300 mb-4">Portfolio Value</h2>
        {chartData.length === 0 ? (
          <div className="flex items-center justify-center h-40 text-zinc-600 text-sm">
            No snapshot data yet — snapshots are taken every 30 minutes.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="valueGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
              <XAxis
                dataKey="time"
                tick={{ fill: '#71717a', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: '#71717a', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => `$${v.toFixed(0)}`}
                width={60}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#18181b',
                  border: '1px solid #3f3f46',
                  borderRadius: 8,
                  fontSize: 12,
                }}
                formatter={(v: number) => [`$${fmt(v)}`, 'Total Value']}
              />
              <Area
                type="monotone"
                dataKey="value"
                stroke="#10b981"
                strokeWidth={2}
                fill="url(#valueGrad)"
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Recent signals */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl">
        <div className="px-5 py-4 border-b border-zinc-800 flex items-center justify-between">
          <h2 className="text-sm font-medium text-zinc-300">Recent Signals</h2>
          <Link
            to="/signals"
            className="text-xs text-zinc-500 hover:text-emerald-400 transition-colors"
          >
            View all →
          </Link>
        </div>
        {signals.length === 0 ? (
          <div className="px-5 py-8 text-center text-zinc-600 text-sm">
            No signals yet — they appear here in real time as whale activity is detected.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-zinc-500 uppercase">
                  <th className="px-5 py-3 text-left">Market</th>
                  <th className="px-5 py-3 text-left">Side</th>
                  <th className="px-5 py-3 text-left">Action</th>
                  <th className="px-5 py-3 text-left">Confidence</th>
                  <th className="px-5 py-3 text-left">Status</th>
                  <th className="px-5 py-3 text-left">Time</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {signals.map((s) => (
                  <tr key={s.id} className={`transition-colors ${s.status === 'pending' ? 'bg-amber-500/5 hover:bg-amber-500/10' : 'hover:bg-zinc-800/50'}`}>
                    <td className="px-5 py-3 text-zinc-300 font-mono text-xs truncate max-w-[180px]">
                      {s.market_ticker}
                    </td>
                    <td className="px-5 py-3">
                      <span
                        className={`text-xs font-medium ${
                          s.side === 'yes' ? 'text-emerald-400' : 'text-red-400'
                        }`}
                      >
                        {s.side.toUpperCase()}
                      </span>
                    </td>
                    <td className="px-5 py-3 text-zinc-300 uppercase text-xs">{s.action}</td>
                    <td className="px-5 py-3 text-zinc-300">{(s.confidence * 100).toFixed(0)}%</td>
                    <td className="px-5 py-3">
                      <span
                        className={`text-xs px-2 py-0.5 rounded-full ${
                          STATUS_COLOR[s.status] ?? 'bg-zinc-700 text-zinc-400'
                        }`}
                      >
                        {s.status}
                      </span>
                    </td>
                    <td className="px-5 py-3 text-zinc-500 text-xs">
                      {new Date(s.created_at).toLocaleString()}
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
