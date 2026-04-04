"""App configuration via environment variables."""

from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kalshi_api_key: str = ""
    kalshi_private_key_path: str = ""
    kalshi_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    kalshi_ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    database_url: str = "sqlite:///./elephant.db"

    # Copy-trading settings
    max_position_pct: float = 0.05  # Max 5% of portfolio per trade
    min_trader_score: float = 80.0  # Minimum score to copy
    min_win_rate: float = 0.65  # 65% minimum win rate
    min_trades: int = 20  # Minimum trades to qualify

    # Signal generation settings
    min_signal_confidence: float = 0.50  # Minimum confidence to emit a signal
    min_elephant_score: float = 30.0  # Minimum elephant_score for signal candidates
    whale_order_threshold: float = 100.0  # Minimum USD order size to classify as whale
    signal_ttl_minutes: int = 30  # Pending signals older than this are expired
    auto_execute_threshold: float = 0.85  # Confidence threshold for automatic trade execution

    # Portfolio risk limits
    max_daily_loss_pct: float = 0.10  # Abort if today's realized loss exceeds 10% of portfolio
    max_total_exposure_pct: float = 0.30  # Abort if open trade costs exceed 30% of portfolio
    max_per_trader_exposure_pct: float = 0.15  # Abort if open costs for a single trader exceed 15% of portfolio
    max_drawdown_pct: float = 0.25  # Block new trades if portfolio has dropped more than 25% from its 30-day peak
    stop_loss_pct: float = 0.20  # Close trade if unrealized loss exceeds 20% of entry cost
    max_trades_per_market: int = 3  # Max concurrent open trades for a single market ticker

    # Paper trading / dry-run settings
    dry_run: bool = True  # When True, simulate orders instead of calling Kalshi API
    paper_balance_initial: float = 1000.0  # Starting paper balance in dollars for dry-run mode

    # Webhook notifications
    webhook_url: str = ""  # Discord-compatible webhook URL; empty disables notifications
    webhook_enabled: bool = False  # Set True to enable webhook notifications

    # API key authentication (ELEPHANT_API_KEY); None disables auth (dev mode)
    api_key: Optional[str] = None

    class Config:
        env_file = ".env"


settings = Settings()
