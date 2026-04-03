"""Tests for signal_generator._trader_tracks_market and process_whale_event."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import TrackedTrader
from app.services.signal_generator import WhaleEvent, _trader_tracks_market, process_whale_event


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
