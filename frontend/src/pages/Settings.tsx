import { useEffect, useState } from 'react'
import { api } from '../api'
import type { AppSettings } from '../types'

interface FieldProps {
  label: string
  description: string
  value: number
  onChange: (v: string) => void
  step?: string
}

function NumberField({ label, description, value, onChange, step = '0.01' }: FieldProps) {
  return (
    <div>
      <label className="block text-xs font-medium text-zinc-400 mb-1">{label}</label>
      <input
        type="number"
        step={step}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full bg-zinc-800 border border-zinc-700 rounded-md px-3 py-2 text-sm text-white font-mono focus:outline-none focus:border-emerald-500"
      />
      <p className="text-xs text-zinc-600 mt-1">{description}</p>
    </div>
  )
}

export default function Settings() {
  const [form, setForm] = useState<AppSettings | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    api.settings
      .get()
      .then((data) => {
        setForm(data)
        setLoading(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setLoading(false)
      })
  }, [])

  const setNum = (key: keyof AppSettings) => (raw: string) => {
    if (!form) return
    setSaved(false)
    setForm({ ...form, [key]: parseFloat(raw) })
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!form) return
    setSaving(true)
    setError(null)
    setSaved(false)
    api.settings
      .patch(form)
      .then((data) => {
        setForm(data)
        setSaved(true)
        setSaving(false)
      })
      .catch((e: Error) => {
        setError(e.message)
        setSaving(false)
      })
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-40 text-zinc-500 text-sm">
        Loading…
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-xl">
      <h1 className="text-xl font-semibold text-white">Settings</h1>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-4 py-3 text-sm text-red-400">
          {error}
        </div>
      )}
      {saved && (
        <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-lg px-4 py-3 text-sm text-emerald-400">
          Settings saved.
        </div>
      )}

      {form && (
        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Risk Limits */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
            <h2 className="text-sm font-semibold text-zinc-300">Risk Limits</h2>
            <NumberField
              label="Max Exposure"
              description="Maximum fraction of portfolio in open trades (e.g. 0.30 = 30%)"
              value={form.max_exposure_pct}
              onChange={setNum('max_exposure_pct')}
            />
            <NumberField
              label="Max Daily Loss"
              description="Stop trading if daily realized loss exceeds this fraction (e.g. 0.10 = 10%)"
              value={form.max_daily_loss_pct}
              onChange={setNum('max_daily_loss_pct')}
            />
            <NumberField
              label="Max Per-Trader Exposure"
              description="Maximum fraction of portfolio in open trades for a single trader (e.g. 0.15 = 15%)"
              value={form.max_per_trader_exposure_pct}
              onChange={setNum('max_per_trader_exposure_pct')}
            />
            <NumberField
              label="Stop Loss"
              description="Close a trade if unrealized loss exceeds this fraction of entry cost (e.g. 0.20 = 20%)"
              value={form.stop_loss_pct}
              onChange={setNum('stop_loss_pct')}
            />
            <NumberField
              label="Max Trades Per Market"
              description="Maximum number of concurrent open trades for a single market ticker (e.g. 3)"
              value={form.max_trades_per_market}
              onChange={setNum('max_trades_per_market')}
              step="1"
            />
          </div>

          {/* Signal Settings */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
            <h2 className="text-sm font-semibold text-zinc-300">Signal Settings</h2>
            <NumberField
              label="Min Confidence Threshold"
              description="Minimum signal confidence to emit (0–1, e.g. 0.70)"
              value={form.min_confidence_threshold}
              onChange={setNum('min_confidence_threshold')}
            />
            <NumberField
              label="Whale Order Threshold ($)"
              description="Minimum USD order size to classify as a whale event"
              value={form.whale_order_threshold}
              onChange={setNum('whale_order_threshold')}
              step="1"
            />
          </div>

          {/* Paper Trading */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
            <h2 className="text-sm font-semibold text-zinc-300">Paper Trading</h2>
            <div>
              <label className="flex items-center gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={form.paper_trading_mode}
                  onChange={(e) => {
                    setSaved(false)
                    setForm({ ...form, paper_trading_mode: e.target.checked })
                  }}
                  className="w-4 h-4 rounded accent-emerald-500"
                />
                <span className="text-sm text-zinc-300">Paper trading mode</span>
              </label>
              <p className="text-xs text-zinc-600 mt-1 ml-7">
                When enabled, orders are simulated — no real trades are placed.
              </p>
            </div>
            <NumberField
              label="Paper Balance ($)"
              description="Starting paper balance in dollars for dry-run mode"
              value={form.paper_balance}
              onChange={setNum('paper_balance')}
              step="1"
            />
          </div>

          <button
            type="submit"
            disabled={saving}
            className="px-4 py-2 rounded-md bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-white text-sm font-medium transition-colors"
          >
            {saving ? 'Saving…' : 'Save Settings'}
          </button>
        </form>
      )}
    </div>
  )
}
