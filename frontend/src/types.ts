export interface TrackedTrader {
  id: number
  kalshi_username: string
  display_name: string | null
  total_profit: number
  win_rate: number
  total_trades: number
  avg_position_size: number
  market_diversity: number
  consistency_score: number
  elephant_score: number
  tier: string
  is_active: boolean
  last_seen: string | null
  created_at: string
}

export interface TradeSignal {
  id: number
  trader_id: number
  market_ticker: string
  market_title: string
  side: 'yes' | 'no'
  action: 'buy' | 'sell'
  detected_price: number
  detected_volume: number
  confidence: number
  status: 'pending' | 'copied' | 'skipped' | 'expired' | 'dismissed'
  created_at: string
}

export interface CopiedTrade {
  id: number
  signal_id: number | null
  market_ticker: string
  side: 'yes' | 'no'
  action: 'buy' | 'sell'
  contracts: number
  price: number
  cost: number
  kalshi_order_id: string | null
  status: 'pending' | 'filled' | 'partial' | 'cancelled' | 'settled' | 'simulated'
  is_simulated: boolean
  pnl: number | null
  created_at: string
  settled_at: string | null
}

export interface PortfolioPerformance {
  mode: 'paper' | 'live'
  balance: number
  total_value: number
  total_pnl: number
  total_trades: number
  win_rate: number
  sharpe_ratio: number | null
  sortino_ratio: number | null
  max_drawdown: number | null
}

export interface AppSettings {
  max_exposure_pct: number
  max_daily_loss_pct: number
  stop_loss_pct: number
  min_confidence_threshold: number
  whale_order_threshold: number
  paper_trading_mode: boolean
  paper_balance: number
}

export interface TraderPnl {
  kalshi_username: string
  display_name: string | null
  elephant_score: number
  tier: string
  total_pnl: number
  win_rate: number
  trade_count: number
  total_cost: number
  roi: number
}

export interface PortfolioSnapshot {
  id: number
  balance: number
  positions_value: number
  total_value: number
  total_pnl: number
  win_rate: number
  created_at: string
}
