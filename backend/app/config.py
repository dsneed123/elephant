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

    class Config:
        env_file = ".env"


settings = Settings()
