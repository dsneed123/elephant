import { useEffect, useState } from 'react'
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
import type { PortfolioPerformance, PortfolioSnapshot, CopiedTrade } from '../types'

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
  filled: 'bg-blue-500/20 text-blue-400',
  partial: 'bg-purple-500/20 text-purple-400',
  cancelled: 'bg-zinc-700 text-zinc-500',
  settled: 'bg-emerald-500/20 text-emerald-400',
  simulated: 'bg-zinc-700 text-zinc-400',
}

export default function Portfolio() {
  const [perf, setPerf] = useState<PortfolioPerformance | null>(null)
  const [snapshots, setSnapshots] = useState<PortfolioSnapshot[]>([])
  const [trades, setTrades] = useState<CopiedTrade[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([
      api.portfolio.performance(),
      api.portfolio.snapshots(100),
      api.portfolio.trades(50),
    ])
      .then(([p, s, t]) => {
        setPerf(p)
        setSnapshots(s)
        setTrades(t)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setLoading(false)
      })
  }, [])

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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Portfolio</h1>
        {perf && (
          <span
            className={`text-xs font-medium px-2.5 py-1 rounded-full ${
              perf.mode === 'paper'
                ? 'bg-amber-500/20 text-amber-400'
                : 'bg-emerald-500/20 text-emerald-400'
            }`}
          >
            {perf.mode === 'paper' ? 'Paper Trading' : 'Live Trading'}
          </span>
        )}
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
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
      </div>

      {/* Risk metrics */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
        <StatCard
          label="Sharpe Ratio"
          value={perf?.sharpe_ratio != null ? fmt(perf.sharpe_ratio) : '—'}
          sub="Annualized"
          color={
            perf?.sharpe_ratio != null
              ? perf.sharpe_ratio >= 1
                ? 'text-emerald-400'
                : perf.sharpe_ratio >= 0
                  ? 'text-amber-400'
                  : 'text-red-400'
              : 'text-white'
          }
        />
        <StatCard
          label="Sortino Ratio"
          value={perf?.sortino_ratio != null ? fmt(perf.sortino_ratio) : '—'}
          sub="Annualized"
          color={
            perf?.sortino_ratio != null
              ? perf.sortino_ratio >= 1
                ? 'text-emerald-400'
                : perf.sortino_ratio >= 0
                  ? 'text-amber-400'
                  : 'text-red-400'
              : 'text-white'
          }
        />
        <StatCard
          label="Max Drawdown"
          value={perf?.max_drawdown != null ? `${(perf.max_drawdown * 100).toFixed(2)}%` : '—'}
          color={
            perf?.max_drawdown != null
              ? perf.max_drawdown <= 0.05
                ? 'text-emerald-400'
                : perf.max_drawdown <= 0.15
                  ? 'text-amber-400'
                  : 'text-red-400'
              : 'text-white'
          }
        />
      </div>

      {/* Portfolio value chart */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <h2 className="text-sm font-medium text-zinc-300 mb-4">Portfolio Value Over Time</h2>
        {chartData.length === 0 ? (
          <div className="flex items-center justify-center h-40 text-zinc-600 text-sm">
            No snapshot data yet — snapshots are taken every 30 minutes.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="portfolioValueGrad" x1="0" y1="0" x2="0" y2="1">
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
                fill="url(#portfolioValueGrad)"
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Recent trades table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        <div className="px-5 py-4 border-b border-zinc-800">
          <h2 className="text-sm font-medium text-zinc-300">Recent Trades</h2>
        </div>
        {trades.length === 0 ? (
          <div className="px-5 py-8 text-center text-zinc-600 text-sm">No trades yet.</div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-zinc-500 uppercase border-b border-zinc-800">
                <th className="px-5 py-3 text-left">Market</th>
                <th className="px-5 py-3 text-left">Side</th>
                <th className="px-5 py-3 text-left">Action</th>
                <th className="px-5 py-3 text-right">Contracts</th>
                <th className="px-5 py-3 text-right">Price</th>
                <th className="px-5 py-3 text-right">Cost</th>
                <th className="px-5 py-3 text-right">P&L</th>
                <th className="px-5 py-3 text-left">Status</th>
                <th className="px-5 py-3 text-left">Date</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {trades.map((t) => (
                <tr key={t.id} className="hover:bg-zinc-800/50 transition-colors">
                  <td className="px-5 py-3 font-mono text-xs text-zinc-300">
                    {t.market_ticker}
                  </td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-xs font-semibold ${
                        t.side === 'yes' ? 'text-emerald-400' : 'text-red-400'
                      }`}
                    >
                      {t.side.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-zinc-300 uppercase text-xs font-medium">
                    {t.action}
                  </td>
                  <td className="px-5 py-3 text-right text-zinc-300">{t.contracts}</td>
                  <td className="px-5 py-3 text-right font-mono text-zinc-300 text-xs">
                    ${t.price.toFixed(2)}
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-zinc-400 text-xs">
                    ${t.cost.toFixed(2)}
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-xs">
                    {t.pnl !== null ? (
                      <span className={t.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}>
                        {t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(2)}
                      </span>
                    ) : (
                      <span className="text-zinc-600">—</span>
                    )}
                  </td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-xs px-2 py-0.5 rounded-full ${
                        STATUS_COLOR[t.status] ?? 'bg-zinc-700 text-zinc-400'
                      }`}
                    >
                      {t.status}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-zinc-500 text-xs">
                    {new Date(t.created_at).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
