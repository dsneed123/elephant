import { useEffect, useRef, useState } from 'react'
import { api } from '../api'
import type { TradeSignal } from '../types'
import { useWebSocket } from '../contexts/WebSocketContext'
import { useToast } from '../contexts/ToastContext'

type StatusFilter = 'all' | 'pending' | 'copied' | 'skipped' | 'expired' | 'dismissed'

const STATUS_COLOR: Record<string, string> = {
  pending: 'bg-amber-500/20 text-amber-400',
  copied: 'bg-emerald-500/20 text-emerald-400',
  skipped: 'bg-zinc-700 text-zinc-400',
  expired: 'bg-red-500/20 text-red-400',
  dismissed: 'bg-zinc-700 text-zinc-500',
}

function timeAgo(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

export default function Signals() {
  const [signals, setSignals] = useState<TradeSignal[]>([])
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionLoading, setActionLoading] = useState<Record<number, 'execute' | 'dismiss'>>({})
  const [executingAll, setExecutingAll] = useState(false)
  const [pendingCount, setPendingCount] = useState(0)
  const { latestEvent } = useWebSocket()
  const { push: toast } = useToast()
  const filterRef = useRef(filter)
  filterRef.current = filter

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

  const refreshPendingCount = () => {
    api.signals.pending().then((sigs) => setPendingCount(sigs.length)).catch(() => {})
  }

  useEffect(() => {
    load(filter)
    refreshPendingCount()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter])

  useEffect(() => {
    if (!latestEvent) return
    if (latestEvent.type === 'signal_created') {
      const sig = latestEvent.payload as TradeSignal
      setSignals((prev) => {
        const f = filterRef.current
        if (f !== 'all' && f !== sig.status) return prev
        if (prev.some((s) => s.id === sig.id)) return prev
        return [sig, ...prev]
      })
      if (sig.status === 'pending') setPendingCount((n) => n + 1)
    } else if (latestEvent.type === 'trade_updated') {
      load(filterRef.current)
      refreshPendingCount()
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestEvent])

  const handleExecute = (id: number) => {
    setActionLoading((prev) => ({ ...prev, [id]: 'execute' }))
    api.signals.execute(id)
      .then(() => {
        toast('success', 'Signal executed')
        load(filter)
        refreshPendingCount()
      })
      .catch((e: Error) => toast('error', `Execution failed: ${e.message}`))
      .finally(() => setActionLoading((prev) => { const next = { ...prev }; delete next[id]; return next }))
  }

  const handleDismiss = (id: number) => {
    setActionLoading((prev) => ({ ...prev, [id]: 'dismiss' }))
    api.signals.dismiss(id)
      .then(() => {
        toast('info', 'Signal dismissed')
        load(filter)
        refreshPendingCount()
      })
      .catch((e: Error) => toast('error', `Dismiss failed: ${e.message}`))
      .finally(() => setActionLoading((prev) => { const next = { ...prev }; delete next[id]; return next }))
  }

  const handleExecuteAll = async () => {
    const pending = signals.filter((s) => s.status === 'pending')
    if (pending.length === 0) return
    setExecutingAll(true)
    let successCount = 0
    let errorCount = 0
    for (const sig of pending) {
      try {
        await api.signals.execute(sig.id)
        successCount++
      } catch {
        errorCount++
      }
    }
    if (successCount > 0) toast('success', `Executed ${successCount} signal${successCount > 1 ? 's' : ''}`)
    if (errorCount > 0) toast('error', `${errorCount} signal${errorCount > 1 ? 's' : ''} failed to execute`)
    load(filter)
    refreshPendingCount()
    setExecutingAll(false)
  }

  const tabs: StatusFilter[] = ['all', 'pending', 'copied', 'skipped', 'expired', 'dismissed']

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Trade Signals</h1>
        <div className="flex items-center gap-3">
          <button
            onClick={() => { load(filter); refreshPendingCount() }}
            disabled={loading}
            className="text-sm px-4 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 disabled:cursor-not-allowed text-zinc-300 transition-colors"
          >
            {loading ? 'Refreshing…' : 'Refresh'}
          </button>
          <span className="text-xs text-zinc-500">Live updates via WebSocket</span>
        </div>
      </div>

      {/* Status filter tabs + Execute All */}
      <div className="flex items-center gap-2 flex-wrap">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setFilter(t)}
            className={`flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md capitalize transition-colors ${
              filter === t
                ? 'bg-emerald-600 text-white'
                : 'bg-zinc-800 text-zinc-400 hover:text-white'
            }`}
          >
            {t}
            {t === 'pending' && pendingCount > 0 && (
              <span
                className={`text-xs font-bold rounded-full min-w-[1.1rem] h-4 flex items-center justify-center px-1 leading-none ${
                  filter === 'pending'
                    ? 'bg-white/20 text-white'
                    : 'bg-amber-500 text-zinc-900'
                }`}
              >
                {pendingCount > 99 ? '99+' : pendingCount}
              </span>
            )}
          </button>
        ))}

        {pendingCount > 0 && (
          <button
            onClick={handleExecuteAll}
            disabled={executingAll}
            className="ml-auto flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors font-medium"
          >
            {executingAll ? (
              <>
                <svg className="animate-spin w-3 h-3" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
                </svg>
                Executing…
              </>
            ) : (
              `Execute All (${pendingCount})`
            )}
          </button>
        )}
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
          <div className="overflow-x-auto">
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
                <th className="px-5 py-3 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800">
              {signals.map((s) => (
                <tr
                  key={s.id}
                  className={`transition-colors ${
                    s.status === 'pending'
                      ? 'bg-amber-500/5 hover:bg-amber-500/10'
                      : 'hover:bg-zinc-800/50'
                  }`}
                >
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
                    <div className="flex flex-col items-end gap-1">
                      <span
                        className={`text-xs font-medium ${
                          s.confidence >= 0.85
                            ? 'text-emerald-400'
                            : s.confidence >= 0.7
                            ? 'text-amber-400'
                            : 'text-zinc-400'
                        }`}
                      >
                        {(s.confidence * 100).toFixed(0)}%
                      </span>
                      <div className="w-16 h-1 bg-zinc-700 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full ${
                            s.confidence >= 0.85
                              ? 'bg-emerald-500'
                              : s.confidence >= 0.7
                              ? 'bg-amber-500'
                              : 'bg-zinc-500'
                          }`}
                          style={{ width: `${(s.confidence * 100).toFixed(0)}%` }}
                        />
                      </div>
                    </div>
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
                  <td className="px-5 py-3 text-zinc-500 text-xs" title={new Date(s.created_at).toLocaleString()}>
                    {timeAgo(s.created_at)}
                  </td>
                  <td className="px-5 py-3">
                    {s.status === 'pending' && (
                      <div className="flex gap-2 items-center">
                        <button
                          onClick={() => handleExecute(s.id)}
                          disabled={!!actionLoading[s.id] || executingAll}
                          className="text-xs px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-white transition-colors"
                        >
                          {actionLoading[s.id] === 'execute' ? '…' : 'Execute'}
                        </button>
                        <button
                          onClick={() => handleDismiss(s.id)}
                          disabled={!!actionLoading[s.id] || executingAll}
                          className="text-xs px-2.5 py-1 rounded bg-zinc-700 hover:bg-zinc-600 disabled:opacity-50 text-zinc-300 transition-colors"
                        >
                          {actionLoading[s.id] === 'dismiss' ? '…' : 'Dismiss'}
                        </button>
                      </div>
                    )}
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
