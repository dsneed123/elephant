"""App configuration via environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kalshi_api_key: str = ""
    kalshi_private_key_path: str = ""
    kalshi_base_url: str = "https://api.kalshi.com/trade-api/v2"
    kalshi_ws_url: str = "wss://api.kalshi.com/trade-api/ws/v2"
    database_url: str = "sqlite:///./elephant.db"

    # Copy-trading settings
    max_position_pct: float = 0.05  # Max 5% of portfolio per trade
    min_trader_score: float = 80.0  # Minimum score to copy
    min_win_rate: float = 0.65  # 65% minimum win rate
    min_trades: int = 20  # Minimum trades to qualify

    # Signal generation settings
    min_signal_confidence: float = 0.7  # Minimum confidence to emit a signal
    min_elephant_score: float = 80.0  # Minimum elephant_score for signal candidates
    whale_order_threshold: float = 1000.0  # Minimum USD order size to classify as whale
    signal_ttl_minutes: int = 30  # Pending signals older than this are expired
    auto_execute_threshold: float = 0.85  # Confidence threshold for automatic trade execution

    # Paper trading / dry-run settings
    dry_run: bool = True  # When True, simulate orders instead of calling Kalshi API
    paper_balance_initial: float = 1000.0  # Starting paper balance in dollars for dry-run mode

    class Config:
        env_file = ".env"


settings = Settings()
