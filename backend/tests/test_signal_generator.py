"""Tests for signal_generator._trader_tracks_market and process_whale_event."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import TrackedTrader, TradeSignal
from app.services.signal_generator import (
    WhaleEvent,
    _trader_tracks_market,
    expire_stale_signals,
    process_whale_event,
)


class _FakeTrader:
    """Minimal stand-in for TrackedTrader with only top_markets set."""

    def __init__(self, top_markets=None):
        self.top_markets = top_markets


class TestTraderTracksMarket:
    def test_none_top_markets_returns_true(self):
        """None means no filter yet — trader is candidate for all markets."""
        trader = _FakeTrader(top_markets=None)
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_empty_string_returns_true(self):
        trader = _FakeTrader(top_markets="")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_empty_json_list_returns_true(self):
        trader = _FakeTrader(top_markets="[]")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_matching_ticker_returns_true(self):
        trader = _FakeTrader(top_markets='["NASDAQ-CLOSE-24DEC31", "FED-RATE-24DEC18"]')
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_non_matching_ticker_returns_false(self):
        trader = _FakeTrader(top_markets='["NASDAQ-CLOSE-24DEC31"]')
        assert _trader_tracks_market(trader, "FED-RATE-24DEC18") is False

    def test_malformed_json_returns_true(self):
        """Malformed JSON should not exclude a trader."""
        trader = _FakeTrader(top_markets="{not valid json}")
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is True

    def test_case_sensitive_match(self):
        trader = _FakeTrader(top_markets='["nasdaq-close-24dec31"]')
        assert _trader_tracks_market(trader, "NASDAQ-CLOSE-24DEC31") is False

    def test_multiple_markets_no_match(self):
        trader = _FakeTrader(top_markets='["A", "B", "C"]')
        assert _trader_tracks_market(trader, "D") is False


# ---------------------------------------------------------------------------
# process_whale_event — auto-execution scheduling via APScheduler
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def _make_active_trader(db, elephant_score: float = 90.0, top_markets=None) -> TrackedTrader:
    trader = TrackedTrader(
        kalshi_username="whale_trader",
        elephant_score=elephant_score,
        win_rate=0.75,
        total_trades=50,
        is_active=True,
        top_markets=top_markets,
    )
    db.add(trader)
    db.commit()
    db.refresh(trader)
    return trader


class TestProcessWhaleEventAutoExecution:
    def _whale_event(self, order_size: float = 9000.0) -> WhaleEvent:
        return WhaleEvent(
            market_ticker="NASDAQ-24DEC31",
            side="yes",
            action="buy",
            order_size=order_size,
            price=45.0,
        )

    def test_high_confidence_schedules_job(self, db):
        """High-confidence signal should be scheduled via scheduler.add_job, not asyncio."""
        _make_active_trader(db, elephant_score=90.0)
        event = self._whale_event(order_size=9000.0)

        mock_scheduler = MagicMock()
        with patch("app.main.scheduler", mock_scheduler), \
             patch("app.services.execution_service.execute_signal") as mock_exec:
            signals = process_whale_event(event, db)

        assert len(signals) == 1
        mock_scheduler.add_job.assert_called_once()
        call_kwargs = mock_scheduler.add_job.call_args
        assert call_kwargs.kwargs.get("trigger") == "date" or call_kwargs.args[1] == "date"
        assert signals[0].id in (call_kwargs.kwargs.get("args") or call_kwargs.args[2] or [])

    def test_low_confidence_does_not_schedule(self, db):
        """Low-confidence signal should not trigger scheduler.add_job."""
        # elephant_score=10 + order_size=100 → confidence ~0.502, below auto_execute_threshold
        _make_active_trader(db, elephant_score=10.0)
        event = self._whale_event(order_size=100.0)

        mock_scheduler = MagicMock()
        with patch("app.main.scheduler", mock_scheduler):
            signals = process_whale_event(event, db)

        mock_scheduler.add_job.assert_not_called()


# ---------------------------------------------------------------------------
# process_whale_event — core signal creation behaviour
# ---------------------------------------------------------------------------


class TestProcessWhaleEventCore:
    def _whale_event(self, market_ticker="NASDAQ-24DEC31", order_size=5000.0) -> WhaleEvent:
        return WhaleEvent(
            market_ticker=market_ticker,
            side="yes",
            action="buy",
            order_size=order_size,
            price=45.0,
        )

    def test_no_active_traders_creates_no_signal(self, db):
        """process_whale_event returns an empty list when no eligible traders exist."""
        event = self._whale_event()
        signals = process_whale_event(event, db)
        assert signals == []

    def test_matching_trader_creates_signal_with_correct_confidence(self, db):
        """A matching trader produces a signal whose confidence matches the formula."""
        _make_active_trader(db, elephant_score=80.0)
        event = self._whale_event(order_size=5000.0)
        # confidence = 0.5 + (80/100)*0.3 + (5000/10000)*0.2 = 0.84
        expected_confidence = 0.5 + (80.0 / 100) * 0.3 + (5000.0 / 10_000) * 0.2

        signals = process_whale_event(event, db)

        assert len(signals) == 1
        assert signals[0].confidence == pytest.approx(expected_confidence)
        assert signals[0].status == "pending"
        assert signals[0].market_ticker == "NASDAQ-24DEC31"
        assert signals[0].side == "yes"

    def test_confidence_capped_at_0_95(self, db):
        """Confidence is capped at 0.95 regardless of score and order size."""
        _make_active_trader(db, elephant_score=100.0)
        event = self._whale_event(order_size=100_000.0)
        # raw = 0.5 + 1.0*0.3 + 10.0*0.2 = 2.8 → capped at 0.95

        mock_scheduler = MagicMock()
        with patch("app.main.scheduler", mock_scheduler):
            signals = process_whale_event(event, db)

        assert len(signals) == 1
        assert signals[0].confidence == pytest.approx(0.95)

    def test_below_score_threshold_creates_no_signal(self, db):
        """A trader whose elephant_score is below min_elephant_score is excluded."""
        # min_elephant_score default is 30.0; use a trader just below threshold
        _make_active_trader(db, elephant_score=5.0)
        event = self._whale_event(order_size=9000.0)

        signals = process_whale_event(event, db)

        assert signals == []

    def test_inactive_trader_creates_no_signal(self, db):
        """is_active=False traders are excluded from signal generation."""
        trader = TrackedTrader(
            kalshi_username="inactive_whale",
            elephant_score=90.0,
            win_rate=0.75,
            total_trades=50,
            is_active=False,
        )
        db.add(trader)
        db.commit()

        event = self._whale_event()
        signals = process_whale_event(event, db)

        assert signals == []


# ---------------------------------------------------------------------------
# expire_stale_signals
# ---------------------------------------------------------------------------


class TestExpireStaleSignals:
    def test_expire_stale_signals_marks_old_pending_as_expired(self, db):
        """Pending signals older than signal_ttl_minutes are updated to 'expired'."""
        trader = _make_active_trader(db)
        old_signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="STALE-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.70,
            status="pending",
            created_at=datetime.utcnow() - timedelta(hours=2),
        )
        db.add(old_signal)
        db.commit()
        db.refresh(old_signal)

        count = expire_stale_signals(db)

        db.refresh(old_signal)
        assert count == 1
        assert old_signal.status == "expired"

    def test_fresh_pending_signal_is_not_expired(self, db):
        """Pending signals within the TTL window are left unchanged."""
        trader = _make_active_trader(db)
        fresh_signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="FRESH-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.70,
            status="pending",
        )
        db.add(fresh_signal)
        db.commit()
        db.refresh(fresh_signal)

        count = expire_stale_signals(db)

        db.refresh(fresh_signal)
        assert count == 0
        assert fresh_signal.status == "pending"

    def test_non_pending_signal_is_not_expired(self, db):
        """Only 'pending' signals are targeted; copied/skipped signals are unchanged."""
        trader = _make_active_trader(db)
        old_copied = TradeSignal(
            trader_id=trader.id,
            market_ticker="COPIED-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.70,
            status="copied",
            created_at=datetime.utcnow() - timedelta(hours=2),
        )
        db.add(old_copied)
        db.commit()
        db.refresh(old_copied)

        count = expire_stale_signals(db)

        db.refresh(old_copied)
        assert count == 0
        assert old_copied.status == "copied"
