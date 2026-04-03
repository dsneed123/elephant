"""Tests for execution_service dry-run (paper trading) mode."""

import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import CopiedTrade, TradeSignal, TrackedTrader


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """In-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def _make_trader(db) -> TrackedTrader:
    trader = TrackedTrader(
        kalshi_username="whale_tester",
        elephant_score=90.0,
        win_rate=0.75,
        total_trades=50,
    )
    db.add(trader)
    db.commit()
    db.refresh(trader)
    return trader


def _make_signal(db, trader: TrackedTrader, price: float = 40.0) -> TradeSignal:
    signal = TradeSignal(
        trader_id=trader.id,
        market_ticker="NASDAQ-24DEC31",
        side="yes",
        action="buy",
        detected_price=price,
        confidence=0.88,
        status="pending",
    )
    db.add(signal)
    db.commit()
    db.refresh(signal)
    return signal


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# _execute_simulated
# ---------------------------------------------------------------------------

class TestExecuteSimulated:
    def test_creates_simulated_copied_trade(self, db):
        from app.services.execution_service import _execute_simulated

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        _run(_execute_simulated(db, signal, price_cents=40))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        assert trade.is_simulated is True
        assert trade.status == "simulated"
        assert trade.market_ticker == "NASDAQ-24DEC31"
        assert trade.side == "yes"
        assert trade.price == pytest.approx(0.40)

    def test_order_id_has_sim_prefix(self, db):
        from app.services.execution_service import _execute_simulated

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=60.0)

        _run(_execute_simulated(db, signal, price_cents=60))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade.kalshi_order_id.startswith("sim-")

    def test_signal_status_set_to_copied(self, db):
        from app.services.execution_service import _execute_simulated

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=50.0)

        _run(_execute_simulated(db, signal, price_cents=50))

        db.refresh(signal)
        assert signal.status == "copied"

    def test_cost_equals_contracts_times_price(self, db):
        from app.services.execution_service import _execute_simulated

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=50.0)

        _run(_execute_simulated(db, signal, price_cents=50))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        expected_cost = trade.contracts * (50 / 100)
        assert trade.cost == pytest.approx(expected_cost)

    def test_paper_balance_reduces_open_cost(self, db):
        """Second trade uses remaining balance after first open trade locks in cost."""
        from app.services.execution_service import _execute_simulated
        from app.config import settings

        trader = _make_trader(db)

        # First trade — creates an open simulated trade that locks up cost
        signal1 = _make_signal(db, trader, price=50.0)
        _run(_execute_simulated(db, signal1, price_cents=50))

        trade1 = db.query(CopiedTrade).filter_by(signal_id=signal1.id).first()
        first_cost = trade1.cost

        # Second trade with fresh signal
        signal2 = _make_signal(db, trader, price=50.0)
        _run(_execute_simulated(db, signal2, price_cents=50))

        trade2 = db.query(CopiedTrade).filter_by(signal_id=signal2.id).first()
        # The second trade's available balance was reduced by the first trade's cost.
        # Both trades should have been placed (no skip), but second may have fewer contracts.
        assert trade2 is not None
        # Available balance for trade2 = initial - first_cost; max_spend = available * max_position_pct
        available = settings.paper_balance_initial - first_cost
        max_spend2 = available * settings.max_position_pct
        import math
        expected_contracts = max(1, math.floor(max_spend2 / (50 / 100)))
        assert trade2.contracts == expected_contracts

    def test_settled_pnl_increases_paper_balance(self, db):
        """Settled P&L is added back to the paper balance for subsequent trades."""
        from app.services.execution_service import _execute_simulated
        from app.config import settings
        import math

        trader = _make_trader(db)

        # Seed a settled simulated trade with positive PnL
        settled = CopiedTrade(
            signal_id=None,
            market_ticker="OLD-MARKET",
            side="yes",
            action="buy",
            contracts=10,
            price=0.50,
            cost=5.0,
            kalshi_order_id="sim-settled",
            status="settled",
            is_simulated=True,
            pnl=50.0,  # $50 profit
        )
        db.add(settled)
        db.commit()

        signal = _make_signal(db, trader, price=50.0)
        _run(_execute_simulated(db, signal, price_cents=50))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        # Balance = initial + settled_pnl - open_costs (no open costs yet after settlement)
        balance = settings.paper_balance_initial + 50.0
        max_spend = balance * settings.max_position_pct
        expected_contracts = max(1, math.floor(max_spend / (50 / 100)))
        assert trade.contracts == expected_contracts


# ---------------------------------------------------------------------------
# execute_signal — routing between dry-run and real
# ---------------------------------------------------------------------------

class TestExecuteSignal:
    """Test execute_signal routing by patching the inner helpers directly."""

    def _make_db_factory(self, db):
        """Return a mock factory whose return value wraps db but no-ops close()."""
        mock_db = MagicMock(wraps=db)
        mock_db.close = MagicMock()  # prevent the finally-block from closing our session
        factory = MagicMock(return_value=mock_db)
        return factory, mock_db

    def test_dry_run_calls_execute_simulated(self, db):
        """When dry_run=True, execute_signal routes to _execute_simulated."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            mock_settings.dry_run = True
            mock_settings.max_position_pct = 0.05
            mock_settings.paper_balance_initial = 1000.0

            _run(execution_service.execute_signal(signal.id))

        sim.assert_awaited_once()
        call_args = sim.call_args
        assert call_args.args[1].id == signal.id  # second arg is the signal

    def test_real_mode_calls_execute_real(self, db):
        """When dry_run=False, execute_signal routes to _execute_real."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        factory, mock_db = self._make_db_factory(db)
        real = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_real", real):
            mock_settings.dry_run = False
            mock_settings.max_position_pct = 0.05
            mock_settings.paper_balance_initial = 1000.0

            _run(execution_service.execute_signal(signal.id))

        real.assert_awaited_once()

    def test_skips_non_pending_signal(self, db):
        """execute_signal exits early if signal is not in pending status."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)
        signal.status = "copied"
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()
        real = AsyncMock()

        with patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim), \
             patch.object(execution_service, "_execute_real", real):
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        real.assert_not_awaited()

    def test_skips_signal_with_invalid_price(self, db):
        """execute_signal skips when detected_price is out of [1, 99] range."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=0.0)  # invalid: 0 is out of [1,99]

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            mock_settings.dry_run = True
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()

    def test_skips_missing_signal(self, db):
        """execute_signal exits cleanly when signal_id does not exist."""
        from app.services import execution_service

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            _run(execution_service.execute_signal(9999))

        sim.assert_not_awaited()


# ---------------------------------------------------------------------------
# _kelly_position_pct unit tests
# ---------------------------------------------------------------------------

class TestKellyPositionPct:
    def test_positive_edge_returns_half_kelly(self):
        """High win-rate at moderate price yields a positive half-Kelly fraction."""
        from app.services.execution_service import _kelly_position_pct

        # win_rate=0.75, price=0.50 → b=1.0, kelly_f=0.5, half_kelly=0.25
        result = _kelly_position_pct(win_rate=0.75, price=0.50, max_pct=1.0)
        assert result == pytest.approx(0.25)

    def test_caps_at_max_pct(self):
        """Half-Kelly above max_pct is capped at max_pct."""
        from app.services.execution_service import _kelly_position_pct

        # win_rate=0.75, price=0.50 → half_kelly=0.25, cap at 0.05
        result = _kelly_position_pct(win_rate=0.75, price=0.50, max_pct=0.05)
        assert result == pytest.approx(0.05)

    def test_negative_edge_returns_none(self):
        """Low win-rate at high price gives negative Kelly → returns None."""
        from app.services.execution_service import _kelly_position_pct

        # win_rate=0.40, price=0.60 → b=0.6667, kelly_f=-0.5, half_kelly=-0.25
        result = _kelly_position_pct(win_rate=0.40, price=0.60, max_pct=0.05)
        assert result is None

    def test_zero_edge_returns_none(self):
        """Breakeven edge (expected value = 0) → returns None."""
        from app.services.execution_service import _kelly_position_pct

        # win_rate=0.50, price=0.50 → kelly_f=0, half_kelly=0
        result = _kelly_position_pct(win_rate=0.50, price=0.50, max_pct=0.05)
        assert result is None

    def test_marginal_positive_edge_below_cap(self):
        """Thin edge yields a Kelly fraction below the cap."""
        from app.services.execution_service import _kelly_position_pct

        # win_rate=0.52, price=0.50 → b=1.0, kelly_f=0.04, half_kelly=0.02
        result = _kelly_position_pct(win_rate=0.52, price=0.50, max_pct=0.05)
        assert result == pytest.approx(0.02)


# ---------------------------------------------------------------------------
# Kelly integration tests via _execute_simulated
# ---------------------------------------------------------------------------

class TestKellyInExecuteSimulated:
    def test_kelly_scales_position_below_max(self, db):
        """Thin-edge trader gets a smaller position than max_position_pct."""
        from app.services.execution_service import _execute_simulated
        from app.config import settings
        import math

        # win_rate=0.52, price=50¢ → half_kelly=0.02 < max_position_pct(0.05)
        trader = TrackedTrader(
            kalshi_username="thin_edge_trader",
            elephant_score=85.0,
            win_rate=0.52,
            total_trades=30,
        )
        db.add(trader)
        db.commit()
        db.refresh(trader)

        signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="TEST-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.90,
            status="pending",
        )
        db.add(signal)
        db.commit()
        db.refresh(signal)

        _run(_execute_simulated(db, signal, price_cents=50))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        # position_pct = 0.02 (half-Kelly), max_spend = 1000 * 0.02 = 20
        # contracts = floor(20 / 0.50) = 40
        expected_contracts = max(1, math.floor(settings.paper_balance_initial * 0.02 / 0.50))
        assert trade.contracts == expected_contracts
        # Should be fewer contracts than with flat max_position_pct (= 100)
        flat_contracts = max(1, math.floor(settings.paper_balance_initial * settings.max_position_pct / 0.50))
        assert trade.contracts < flat_contracts

    def test_negative_kelly_skips_signal(self, db):
        """Signal with no positive edge is marked skipped, no trade created."""
        from app.services.execution_service import _execute_simulated

        # win_rate=0.40 at price=60¢ → negative Kelly
        trader = TrackedTrader(
            kalshi_username="bad_edge_trader",
            elephant_score=82.0,
            win_rate=0.40,
            total_trades=25,
        )
        db.add(trader)
        db.commit()
        db.refresh(trader)

        signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="TEST-MARKET",
            side="yes",
            action="buy",
            detected_price=60.0,
            confidence=0.88,
            status="pending",
        )
        db.add(signal)
        db.commit()
        db.refresh(signal)

        _run(_execute_simulated(db, signal, price_cents=60))

        db.refresh(signal)
        assert signal.status == "skipped"
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is None
