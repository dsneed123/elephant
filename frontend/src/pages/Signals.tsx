import { useEffect, useState } from 'react'
import { api } from '../api'
import type { TradeSignal } from '../types'

type StatusFilter = 'all' | 'pending' | 'copied' | 'skipped' | 'expired'

const STATUS_COLOR: Record<string, string> = {
  pending: 'bg-amber-500/20 text-amber-400',
  copied: 'bg-emerald-500/20 text-emerald-400',
  skipped: 'bg-zinc-700 text-zinc-400',
  expired: 'bg-red-500/20 text-red-400',
}

export default function Signals() {
  const [signals, setSignals] = useState<TradeSignal[]>([])
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = (f: StatusFilter) => {
    setLoading(true)
    setError(null)
    const req = f === 'all' ? api.signals.list() : api.signals.list(f)
    req
      .then((data) => {
        setSignals(data)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setLoading(false)
      })
  }

  useEffect(() => {
    load(filter)
    // Poll every 15 seconds for new signals
    const interval = setInterval(() => load(filter), 15_000)
    return () => clearInterval(interval)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter])

  const tabs: StatusFilter[] = ['all', 'pending', 'copied', 'skipped', 'expired']

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Trade Signals</h1>
        <span className="text-xs text-zinc-500">Auto-refreshes every 15s</span>
      </div>

      {/* Status filter tabs */}
      <div className="flex gap-2">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setFilter(t)}
            className={`text-xs px-3 py-1.5 rounded-md capitalize transition-colors ${
              filter === t
                ? 'bg-emerald-600 text-white'
                : 'bg-zinc-800 text-zinc-400 hover:text-white'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {loading ? (
          <div className="px-5 py-10 text-center text-zinc-500 text-sm">Loading…</div>
        ) : error ? (
          <div className="px-5 py-10 text-center text-red-400 text-sm">{error}</div>
        ) : signals.length === 0 ? (
          <div className="px-5 py-10 text-center text-zinc-600 text-sm">
            No signals found.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-zinc-500 uppercase border-b border-zinc-800">
                <th className="px-5 py-3 text-left">Market</th>
                <th className="px-5 py-3 text-left">Side</th>
                <th className="px-5 py-3 text-left">Action</th>
                <th className="px-5 py-3 text-right">Price</th>
                <th className="px-5 py-3 text-right">Volume</th>
                <th className="px-5 py-3 text-right">Confidence</th>
                <th className="px-5 py-3 text-left">Status</th>
                <th className="px-5 py-3 text-left">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {signals.map((s) => (
                <tr key={s.id} className="hover:bg-zinc-800/50 transition-colors">
                  <td className="px-5 py-3">
                    <div className="font-mono text-xs text-zinc-300">{s.market_ticker}</div>
                    {s.market_title && (
                      <div className="text-xs text-zinc-500 truncate max-w-[200px]">
                        {s.market_title}
                      </div>
                    )}
                  </td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-xs font-semibold ${
                        s.side === 'yes' ? 'text-emerald-400' : 'text-red-400'
                      }`}
                    >
                      {s.side.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-zinc-300 uppercase text-xs font-medium">
                    {s.action}
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-zinc-300 text-xs">
                    ${s.detected_price.toFixed(2)}
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-zinc-400 text-xs">
                    {s.detected_volume.toLocaleString()}
                  </td>
                  <td className="px-5 py-3 text-right">
                    <span
                      className={
                        s.confidence >= 0.85
                          ? 'text-emerald-400'
                          : s.confidence >= 0.7
                          ? 'text-amber-400'
                          : 'text-zinc-400'
                      }
                    >
                      {(s.confidence * 100).toFixed(0)}%
                    </span>
                  </td>
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
        )}
      </div>
    </div>
  )
}
