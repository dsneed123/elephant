"""Database models."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from app.db import Base


class TrackedTrader(Base):
    """A trader being tracked from Kalshi leaderboard."""
    __tablename__ = "tracked_traders"

    id = Column(Integer, primary_key=True)
    kalshi_username = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String)
    total_profit = Column(Float, default=0.0)
    win_rate = Column(Float, default=0.0)
    total_trades = Column(Integer, default=0)
    avg_position_size = Column(Float, default=0.0)
    market_diversity = Column(Integer, default=0)
    consistency_score = Column(Float, default=0.0)
    elephant_score = Column(Float, default=0.0)  # Our composite ranking
    tier = Column(String, default="unranked")  # top_001, top_01, top_1, etc.
    is_active = Column(Boolean, default=True)
    last_seen = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    signals = relationship("TradeSignal", back_populates="trader")


class TradeSignal(Base):
    """A detected trade signal from a tracked trader."""
    __tablename__ = "trade_signals"

    id = Column(Integer, primary_key=True)
    trader_id = Column(Integer, ForeignKey("tracked_traders.id"), nullable=False)
    market_ticker = Column(String, nullable=False, index=True)
    market_title = Column(String)
    side = Column(String)  # yes/no
    action = Column(String)  # buy/sell
    detected_price = Column(Float)
    detected_volume = Column(Float)
    confidence = Column(Float, default=0.0)  # How confident we are this is from a top trader
    status = Column(String, default="pending")  # pending, copied, skipped, expired
    created_at = Column(DateTime, default=datetime.utcnow)

    trader = relationship("TrackedTrader", back_populates="signals")


class CopiedTrade(Base):
    """A trade we executed by copying a signal."""
    __tablename__ = "copied_trades"

    id = Column(Integer, primary_key=True)
    signal_id = Column(Integer, ForeignKey("trade_signals.id"))
    market_ticker = Column(String, nullable=False)
    side = Column(String, nullable=False)
    action = Column(String, nullable=False)
    contracts = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    cost = Column(Float, nullable=False)
    kalshi_order_id = Column(String)
    status = Column(String, default="pending")  # pending, filled, partial, cancelled, settled
    pnl = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    settled_at = Column(DateTime, nullable=True)


class PortfolioSnapshot(Base):
    """Periodic snapshot of portfolio value."""
    __tablename__ = "portfolio_snapshots"

    id = Column(Integer, primary_key=True)
    balance = Column(Float, nullable=False)
    positions_value = Column(Float, nullable=False)
    total_value = Column(Float, nullable=False)
    total_pnl = Column(Float, nullable=False)
    win_rate = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)
