"""End-to-end integration tests for the signal-to-settlement pipeline.

Tests use a real in-memory SQLite database (StaticPool so every SessionLocal()
call within the same test shares the same connection) and mock only the Kalshi
HTTP client.  Service classes are imported and invoked directly so that
cross-service wiring bugs surface here rather than in isolated unit tests.

Scenarios covered:
  1. High-confidence whale event creates a TradeSignal and schedules
     execute_signal; running it in live mode stores a CopiedTrade with
     status='pending'.
  2. poll_open_orders transitions a pending trade to 'filled' when Kalshi
     reports the order as filled.
  3. settle_open_trades computes correct PnL for both YES and NO sides after
     market resolution, and also handles simulated trades.
  4. check_stop_losses marks a trade 'stopped_out' when the unrealized loss
     exceeds stop_loss_pct.
  5. Dry-run mode creates simulated trades (status='simulated',
     is_simulated=True) without calling any Kalshi API endpoints.
"""

import asyncio
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import CopiedTrade, TradeSignal, TrackedTrader
from app.services.signal_generator import WhaleEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


@contextmanager
def _override_settings(**kwargs):
    """Temporarily set settings attributes, bypassing Pydantic validation."""
    from app.config import settings

    original = {k: getattr(settings, k) for k in kwargs}
    try:
        for k, v in kwargs.items():
            object.__setattr__(settings, k, v)
        yield
    finally:
        for k, v in original.items():
            object.__setattr__(settings, k, v)


def _make_kalshi_client(**method_returns) -> MagicMock:
    """Return a MagicMock KalshiClient with sensible AsyncMock defaults."""
    defaults = {
        "get_portfolio_balance": 1000.0,
        "place_order": {"order_id": "order-mock001"},
        "get_order": {"status": "resting"},
        "get_market": {"result": None},
        "cancel_order": {},
    }
    client = MagicMock()
    for name, default in defaults.items():
        setattr(client, name, AsyncMock(return_value=method_returns.get(name, default)))
    return client


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    """
    In-memory SQLite session shared across all SessionLocal() calls within
    one test via StaticPool.  Also patches app.db.SessionLocal so that
    service functions that open their own session use the same engine.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    _Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = _Session()
    with patch("app.db.SessionLocal", _Session):
        yield session
    session.close()
    engine.dispose()


@pytest.fixture(autouse=True)
def _no_side_effects():
    """Suppress WebSocket broadcasts and outbound notifications for every test."""
    with (
        patch("app.services.execution_service.broadcast_event"),
        patch("app.services.signal_generator.broadcast_event"),
        patch("app.services.notification_service.notify_high_confidence_signal"),
        patch("app.services.notification_service.notify_trade_executed"),
        patch("app.services.notification_service.notify_stop_loss"),
        patch("app.services.notification_service.notify_daily_loss_warning"),
    ):
        yield


# ---------------------------------------------------------------------------
# Object-creation helpers
# ---------------------------------------------------------------------------


def _make_trader(
    db, *, username="whale_alice", elephant_score=90.0, win_rate=0.75
) -> TrackedTrader:
    trader = TrackedTrader(
        kalshi_username=username,
        elephant_score=elephant_score,
        win_rate=win_rate,
        total_trades=50,
        is_active=True,
    )
    db.add(trader)
    db.commit()
    db.refresh(trader)
    return trader


def _make_signal(
    db,
    trader: TrackedTrader,
    *,
    market_ticker="NASDAQ-24DEC31",
    side="yes",
    price: float = 40.0,
    confidence: float = 0.88,
    status: str = "pending",
) -> TradeSignal:
    signal = TradeSignal(
        trader_id=trader.id,
        market_ticker=market_ticker,
        side=side,
        action="buy",
        detected_price=price,
        detected_volume=5000.0,
        confidence=confidence,
        status=status,
    )
    db.add(signal)
    db.commit()
    db.refresh(signal)
    return signal


def _make_trade(
    db,
    signal: TradeSignal,
    *,
    contracts: int = 10,
    price_dollars: float = 0.40,
    side: str = "yes",
    status: str = "pending",
    order_id: str = "order-abc123",
    is_simulated: bool = False,
) -> CopiedTrade:
    trade = CopiedTrade(
        signal_id=signal.id,
        market_ticker=signal.market_ticker,
        side=side,
        action="buy",
        contracts=contracts,
        price=price_dollars,
        cost=contracts * price_dollars,
        kalshi_order_id=order_id,
        status=status,
        is_simulated=is_simulated,
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    return trade


# ---------------------------------------------------------------------------
# Scenario 1 — Whale event → signal creation → auto-execution → pending trade
# ---------------------------------------------------------------------------


class TestScenario1_WhaleToAutoExecution:
    """
    A high-confidence whale event produces a TradeSignal and schedules
    execute_signal.  Running execute_signal in live mode (dry_run=False)
    calls Kalshi's place_order and stores a CopiedTrade with status='pending'.
    """

    def test_signal_created_from_whale_event(self, db):
        from app.services.signal_generator import process_whale_event

        _make_trader(db, elephant_score=90.0)

        event = WhaleEvent(
            market_ticker="NASDAQ-24DEC31",
            side="yes",
            action="buy",
            order_size=5000.0,
            price=40.0,
        )

        with patch("app.main.scheduler", MagicMock()):
            signals = process_whale_event(event, db)

        assert len(signals) == 1
        sig = signals[0]
        assert sig.status == "pending"
        assert sig.market_ticker == "NASDAQ-24DEC31"
        assert sig.side == "yes"

    def test_high_confidence_schedules_auto_execution(self, db):
        """
        elephant_score=90, order_size=30_000, win_rate=0.75 →
          confidence = 0.75*0.40 + 0.90*0.35 + (log10(30000)/log10(50000))*0.25 ≈ 0.853 ≥ 0.85 threshold
        """
        from app.services.signal_generator import process_whale_event

        _make_trader(db, elephant_score=90.0)

        event = WhaleEvent(
            market_ticker="NASDAQ-24DEC31",
            side="yes",
            action="buy",
            order_size=30_000.0,
            price=40.0,
        )

        mock_scheduler = MagicMock()
        with patch("app.main.scheduler", mock_scheduler):
            signals = process_whale_event(event, db)

        assert len(signals) == 1
        assert signals[0].confidence >= 0.85
        mock_scheduler.add_job.assert_called_once()
        _args = mock_scheduler.add_job.call_args[1]["args"]
        assert _args == [signals[0].id]

    def test_execute_signal_live_mode_creates_pending_trade(self, db):
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        client = _make_kalshi_client(
            get_portfolio_balance=1000.0,
            place_order={"order_id": "order-live001"},
        )

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=False),
        ):
            _run(execute_signal(signal.id))

        db.expire_all()
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        assert trade.status == "pending"
        assert trade.is_simulated is False
        assert trade.kalshi_order_id == "order-live001"

        sig = db.query(TradeSignal).filter_by(id=signal.id).first()
        assert sig.status == "copied"

    def test_execute_signal_calls_place_order_with_correct_ticker(self, db):
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, market_ticker="TRUMP-WIN-24", price=55.0)

        client = _make_kalshi_client(place_order={"order_id": "order-trump001"})

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=False),
        ):
            _run(execute_signal(signal.id))

        client.place_order.assert_called_once()
        call_kwargs = client.place_order.call_args[1]
        assert call_kwargs["ticker"] == "TRUMP-WIN-24"
        assert call_kwargs["side"] == "yes"
        assert call_kwargs["price"] == 55


# ---------------------------------------------------------------------------
# Scenario 2 — Order fill polling: pending → filled / partial / cancelled
# ---------------------------------------------------------------------------


class TestScenario2_PollOrderFill:
    """poll_open_orders fetches each pending order and transitions local status."""

    def test_pending_transitions_to_filled(self, db):
        from app.services.settlement_service import poll_open_orders

        trader = _make_trader(db)
        signal = _make_signal(db, trader)
        _make_trade(db, signal, order_id="order-fill001")

        client = _make_kalshi_client(get_order={"status": "filled"})
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            updated = _run(poll_open_orders(db))

        assert updated == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-fill001").first()
        assert t.status == "filled"

    def test_resting_order_stays_pending(self, db):
        from app.services.settlement_service import poll_open_orders

        trader = _make_trader(db)
        signal = _make_signal(db, trader)
        _make_trade(db, signal, order_id="order-rest001")

        client = _make_kalshi_client(get_order={"status": "resting"})
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            updated = _run(poll_open_orders(db))

        assert updated == 0
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-rest001").first()
        assert t.status == "pending"

    def test_partial_fill_updates_contracts_and_cost(self, db):
        from app.services.settlement_service import poll_open_orders

        trader = _make_trader(db)
        signal = _make_signal(db, trader)
        _make_trade(db, signal, contracts=10, price_dollars=0.40, order_id="order-partial001")

        client = _make_kalshi_client(
            get_order={"status": "partially_filled", "filled_count": 3}
        )
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            updated = _run(poll_open_orders(db))

        assert updated == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-partial001").first()
        assert t.status == "partial"
        assert t.contracts == 3
        assert abs(t.cost - 3 * 0.40) < 1e-6

    def test_cancelled_order_transitions_to_cancelled(self, db):
        from app.services.settlement_service import poll_open_orders

        trader = _make_trader(db)
        signal = _make_signal(db, trader)
        _make_trade(db, signal, order_id="order-cancel001")

        client = _make_kalshi_client(get_order={"status": "cancelled"})
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            updated = _run(poll_open_orders(db))

        assert updated == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-cancel001").first()
        assert t.status == "cancelled"


# ---------------------------------------------------------------------------
# Scenario 3 — Settlement with correct PnL for YES/NO sides
# ---------------------------------------------------------------------------


class TestScenario3_Settlement:
    """
    settle_open_trades computes realized PnL using:
      YES:  pnl = (close_price - fill_price) * contracts / 100
      NO:   pnl = ((100 - close_price) - fill_price) * contracts / 100
    """

    def test_yes_side_win(self, db):
        """YES buyer wins: close_price=100, fill_price=40¢, 10 contracts → pnl=6.0"""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=40.0)
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.40, side="yes",
            status="filled", order_id="order-y-win",
        )

        client = _make_kalshi_client(get_order={
            "status": "filled", "close_price": 100,
            "yes_price": 40, "filled_count": 10,
        })
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-y-win").first()
        assert t.status == "settled"
        assert abs(t.pnl - 6.0) < 1e-6

    def test_yes_side_loss(self, db):
        """YES buyer loses: close_price=0, fill_price=70¢, 5 contracts → pnl=-3.5"""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=70.0)
        _make_trade(
            db, signal,
            contracts=5, price_dollars=0.70, side="yes",
            status="filled", order_id="order-y-loss",
        )

        client = _make_kalshi_client(get_order={
            "status": "filled", "close_price": 0,
            "yes_price": 70, "filled_count": 5,
        })
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-y-loss").first()
        assert t.status == "settled"
        assert abs(t.pnl - (-3.5)) < 1e-6

    def test_no_side_win(self, db):
        """NO buyer wins: close_price=0, fill_price=35¢, 8 contracts → pnl=5.2"""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="no", price=35.0)
        _make_trade(
            db, signal,
            contracts=8, price_dollars=0.35, side="no",
            status="filled", order_id="order-n-win",
        )

        client = _make_kalshi_client(get_order={
            "status": "filled", "close_price": 0,
            "no_price": 35, "filled_count": 8,
        })
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-n-win").first()
        assert t.status == "settled"
        # pnl = ((100 - 0) - 35) * 8 / 100 = 5.2
        assert abs(t.pnl - 5.2) < 1e-6

    def test_no_side_loss(self, db):
        """NO buyer loses: close_price=100, fill_price=30¢, 6 contracts → pnl=-1.8"""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="no", price=30.0)
        _make_trade(
            db, signal,
            contracts=6, price_dollars=0.30, side="no",
            status="filled", order_id="order-n-loss",
        )

        client = _make_kalshi_client(get_order={
            "status": "filled", "close_price": 100,
            "no_price": 30, "filled_count": 6,
        })
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-n-loss").first()
        assert t.status == "settled"
        # pnl = ((100 - 100) - 30) * 6 / 100 = -1.8
        assert abs(t.pnl - (-1.8)) < 1e-6

    def test_simulated_trade_settlement_via_get_market(self, db):
        """Simulated trades settle through _settle_simulated → get_market() path."""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=40.0)
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.40, side="yes",
            status="simulated", order_id="sim-settle001", is_simulated=True,
        )

        # Market resolves YES → pnl = (100 - 40) * 10 / 100 = 6.0
        client = _make_kalshi_client(get_market={"result": "yes"})
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 1
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="sim-settle001").first()
        assert t.status == "settled"
        assert abs(t.pnl - 6.0) < 1e-6

    def test_unresolved_market_is_skipped(self, db):
        """Trades with no close_price / result are left unsettled for the next run."""
        from app.services.settlement_service import settle_open_trades

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=50.0)
        _make_trade(
            db, signal,
            contracts=5, price_dollars=0.50, side="yes",
            status="filled", order_id="order-unresolved",
        )

        # No close_price in the response → market not yet resolved
        client = _make_kalshi_client(get_order={"status": "filled", "close_price": None})
        with patch("app.services.kalshi_client.get_kalshi_client", return_value=client):
            settled = _run(settle_open_trades(db))

        assert settled == 0
        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-unresolved").first()
        assert t.status == "filled"  # unchanged
        assert t.pnl is None


# ---------------------------------------------------------------------------
# Scenario 4 — Stop-loss closes trades that exceed stop_loss_pct
# ---------------------------------------------------------------------------


class TestScenario4_StopLoss:
    """
    check_stop_losses marks a trade 'stopped_out' when the unrealized loss
    as a fraction of entry cost (−pnl / cost) exceeds settings.stop_loss_pct.
    """

    def test_stop_loss_triggers_in_dry_run(self, db):
        """
        Bought YES @ 50¢, 10 contracts (cost=5.00).
        Current price = 25¢ → loss = 2.50 → loss_ratio = 0.50 > 0.20 threshold.
        """
        from app.services.execution_service import check_stop_losses

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=50.0)
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.50, side="yes",
            status="simulated", order_id="sim-sl001", is_simulated=True,
        )

        client = _make_kalshi_client(get_market={"yes_bid": 25, "last_price": 25})
        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True, stop_loss_pct=0.20),
        ):
            _run(check_stop_losses(db))

        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="sim-sl001").first()
        assert t.status == "stopped_out"
        assert t.pnl is not None and t.pnl < 0
        assert t.settled_at is not None

    def test_stop_loss_does_not_trigger_below_threshold(self, db):
        """
        Bought YES @ 50¢, 10 contracts (cost=5.00).
        Current price = 45¢ → loss = 0.50 → loss_ratio = 0.10 < 0.20 threshold.
        """
        from app.services.execution_service import check_stop_losses

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=50.0)
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.50, side="yes",
            status="simulated", order_id="sim-sl002", is_simulated=True,
        )

        client = _make_kalshi_client(get_market={"yes_bid": 45, "last_price": 45})
        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True, stop_loss_pct=0.20),
        ):
            _run(check_stop_losses(db))

        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="sim-sl002").first()
        assert t.status == "simulated"  # unchanged
        assert t.pnl is None

    def test_stop_loss_correct_pnl_value(self, db):
        """Verify the PnL stored on stop-out equals unrealized_pnl at exit price."""
        from app.services.execution_service import check_stop_losses

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=60.0)
        # cost = 10 * 0.60 = 6.00; exit at 30¢ → unrealized = (0.30 - 0.60)*10 = -3.0
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.60, side="yes",
            status="pending", order_id="order-sl003", is_simulated=False,
        )

        client = _make_kalshi_client(get_market={"yes_bid": 30, "last_price": 30})
        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True, stop_loss_pct=0.20),
        ):
            _run(check_stop_losses(db))

        db.expire_all()
        t = db.query(CopiedTrade).filter_by(kalshi_order_id="order-sl003").first()
        assert t.status == "stopped_out"
        assert abs(t.pnl - (-3.0)) < 1e-6

    def test_stop_loss_skips_already_settled_trades(self, db):
        """Already-settled and stopped-out trades must not be re-evaluated."""
        from app.services.execution_service import check_stop_losses

        trader = _make_trader(db)
        signal = _make_signal(db, trader, side="yes", price=50.0)
        _make_trade(
            db, signal,
            contracts=10, price_dollars=0.50, side="yes",
            status="settled", order_id="order-sl-settled",
        )

        client = _make_kalshi_client(get_market={"yes_bid": 5, "last_price": 5})
        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True, stop_loss_pct=0.20),
        ):
            _run(check_stop_losses(db))

        # get_market should never have been called for a settled trade
        client.get_market.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 5 — Dry-run mode: simulated trades, no Kalshi API calls
# ---------------------------------------------------------------------------


class TestScenario5_DryRun:
    """
    In dry-run mode execute_signal simulates the order locally:
    - CopiedTrade is stored with is_simulated=True and status='simulated'
    - place_order and get_portfolio_balance are never called
    """

    def test_dry_run_creates_simulated_trade(self, db):
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)
        client = _make_kalshi_client()

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True),
        ):
            _run(execute_signal(signal.id))

        db.expire_all()
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        assert trade.is_simulated is True
        assert trade.status == "simulated"
        assert trade.kalshi_order_id.startswith("sim-")

    def test_dry_run_marks_signal_copied(self, db):
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)
        client = _make_kalshi_client()

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True),
        ):
            _run(execute_signal(signal.id))

        db.expire_all()
        sig = db.query(TradeSignal).filter_by(id=signal.id).first()
        assert sig.status == "copied"

    def test_dry_run_does_not_call_kalshi_api(self, db):
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)
        client = _make_kalshi_client()

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True),
        ):
            _run(execute_signal(signal.id))

        client.place_order.assert_not_called()
        client.get_portfolio_balance.assert_not_called()

    def test_dry_run_position_size_bounded_by_paper_balance(self, db):
        """Trade cost must never exceed the available paper balance."""
        from app.services.execution_service import execute_signal

        trader = _make_trader(db, win_rate=0.75)
        signal = _make_signal(db, trader, price=40.0)
        client = _make_kalshi_client()

        paper_balance = 500.0
        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(
                dry_run=True,
                paper_balance_initial=paper_balance,
                max_position_pct=0.05,
            ),
        ):
            _run(execute_signal(signal.id))

        db.expire_all()
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        assert trade.cost <= paper_balance

    def test_dry_run_skips_non_pending_signal(self, db):
        """execute_signal must be idempotent: already-copied signals are ignored."""
        from app.services.execution_service import execute_signal

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0, status="copied")
        client = _make_kalshi_client()

        with (
            patch("app.services.kalshi_client.get_kalshi_client", return_value=client),
            _override_settings(dry_run=True),
        ):
            _run(execute_signal(signal.id))

        # No trade should have been created for a non-pending signal
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is None
