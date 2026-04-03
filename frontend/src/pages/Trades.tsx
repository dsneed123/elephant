import { useEffect, useState } from 'react'
import { api } from '../api'
import type { CopiedTrade } from '../types'

const STATUS_COLOR: Record<string, string> = {
  pending: 'bg-amber-500/20 text-amber-400',
  filled: 'bg-blue-500/20 text-blue-400',
  partial: 'bg-purple-500/20 text-purple-400',
  cancelled: 'bg-zinc-700 text-zinc-500',
  settled: 'bg-emerald-500/20 text-emerald-400',
  simulated: 'bg-zinc-700 text-zinc-400',
}

export default function Trades() {
  const [trades, setTrades] = useState<CopiedTrade[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.portfolio
      .trades(100)
      .then((data) => {
        setTrades(data)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setLoading(false)
      })
  }, [])

  const totalPnl = trades.reduce((sum, t) => sum + (t.pnl ?? 0), 0)
  const settled = trades.filter((t) => t.status === 'settled')
  const wins = settled.filter((t) => (t.pnl ?? 0) > 0).length

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Copied Trades</h1>
        <div className="flex gap-4 text-xs text-zinc-500">
          <span>
            Total P&L:{' '}
            <span className={totalPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}>
              {totalPnl >= 0 ? '+' : ''}${totalPnl.toFixed(2)}
            </span>
          </span>
          {settled.length > 0 && (
            <span>
              Win Rate:{' '}
              <span className="text-zinc-300">
                {((wins / settled.length) * 100).toFixed(1)}%
              </span>
            </span>
          )}
        </div>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="px-5 py-10 text-center text-zinc-500 text-sm">Loading…</div>
        ) : error ? (
          <div className="px-5 py-10 text-center text-red-400 text-sm">{error}</div>
        ) : trades.length === 0 ? (
          <div className="px-5 py-10 text-center text-zinc-600 text-sm">
            No trades yet. Trades are executed when signals are generated and confidence
            exceeds the auto-execute threshold.
          </div>
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
                <th className="px-5 py-3 text-center">Sim</th>
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
                  <td className="px-5 py-3 text-center">
                    <span
                      className={`inline-block w-2 h-2 rounded-full ${
                        t.is_simulated ? 'bg-amber-400' : 'bg-emerald-400'
                      }`}
                      title={t.is_simulated ? 'Simulated' : 'Live'}
                    />
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
