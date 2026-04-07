"""Tests for execution_service dry-run (paper trading) mode."""

import asyncio
import re
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import CopiedTrade, PortfolioSnapshot, TradeSignal, TrackedTrader


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

    def test_stopped_out_pnl_updates_paper_balance(self, db):
        """Stopped-out trades are treated as closed: only the realised loss reduces balance."""
        from app.services.execution_service import _execute_simulated
        from app.config import settings
        import math
        from datetime import datetime, timezone

        trader = _make_trader(db)

        # A stopped_out trade: opened for $50, closed with a $10 loss.
        # The position is no longer open so the $50 cost must NOT be deducted a second
        # time; only the realised -$10 pnl should affect the balance.
        stopped = CopiedTrade(
            signal_id=None,
            market_ticker="OLD-MARKET",
            side="yes",
            action="buy",
            contracts=100,
            price=0.50,
            cost=50.0,
            kalshi_order_id="sim-stopped",
            status="stopped_out",
            is_simulated=True,
            pnl=-10.0,
            settled_at=datetime.now(timezone.utc),
        )
        db.add(stopped)
        db.commit()

        signal = _make_signal(db, trader, price=50.0)
        _run(_execute_simulated(db, signal, price_cents=50))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None, "Trade should be created (position is closed)"
        # Balance = initial + (-10) - 0 open costs = 990, not initial - 50 = 950
        balance = settings.paper_balance_initial + (-10.0)
        max_spend = balance * settings.max_position_pct
        expected_contracts = max(1, math.floor(max_spend / 0.50))
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
            mock_settings.max_total_exposure_pct = 0.30
            mock_settings.max_daily_loss_pct = 0.10
            mock_settings.max_per_trader_exposure_pct = 0.15
            mock_settings.max_trades_per_market = 3

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
            mock_settings.max_total_exposure_pct = 0.30
            mock_settings.max_daily_loss_pct = 0.10
            mock_settings.max_per_trader_exposure_pct = 0.15
            mock_settings.max_trades_per_market = 3

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


# ---------------------------------------------------------------------------
# Portfolio-level risk limit guards in execute_signal
# ---------------------------------------------------------------------------

class TestRiskLimits:
    """Tests for max_total_exposure_pct and max_daily_loss_pct guards."""

    def _make_db_factory(self, db):
        mock_db = MagicMock(wraps=db)
        mock_db.close = MagicMock()
        factory = MagicMock(return_value=mock_db)
        return factory, mock_db

    def _patch_settings(self, mock_settings):
        mock_settings.dry_run = True
        mock_settings.max_position_pct = 0.05
        mock_settings.paper_balance_initial = 1000.0
        mock_settings.max_total_exposure_pct = 0.30
        mock_settings.max_daily_loss_pct = 0.10
        mock_settings.max_per_trader_exposure_pct = 0.15
        mock_settings.max_drawdown_pct = 0.25
        mock_settings.max_trades_per_market = 3

    def test_max_total_exposure_skips_signal(self, db):
        """Signal is skipped when open trade costs breach max_total_exposure_pct."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # 4 open trades × $80 = $320 > 30% of $1000 ($300)
        for _ in range(4):
            db.add(CopiedTrade(
                signal_id=None,
                market_ticker="OPEN-MARKET",
                side="yes",
                action="buy",
                contracts=100,
                price=0.80,
                cost=80.0,
                kalshi_order_id="open-order",
                status="pending",
                is_simulated=True,
            ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        db.refresh(signal)
        assert signal.status == "skipped"

    def test_max_daily_loss_skips_signal(self, db):
        """Signal is skipped when today's realized loss breaches max_daily_loss_pct."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=900.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # Loss of $110 > 10% of $1000 ($100), settled today
        db.add(CopiedTrade(
            signal_id=None,
            market_ticker="LOSS-MARKET",
            side="yes",
            action="buy",
            contracts=100,
            price=0.50,
            cost=50.0,
            kalshi_order_id="loss-order",
            status="settled",
            is_simulated=True,
            pnl=-110.0,
            settled_at=datetime.now(timezone.utc),
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        db.refresh(signal)
        assert signal.status == "skipped"

    def test_within_limits_executes_normally(self, db):
        """Signal routes to execution when both risk limits are satisfied."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # Small open exposure: $50 < 30% of $1000
        db.add(CopiedTrade(
            signal_id=None,
            market_ticker="SMALL-MARKET",
            side="yes",
            action="buy",
            contracts=10,
            price=0.50,
            cost=5.0,
            kalshi_order_id="small-order",
            status="pending",
            is_simulated=True,
        ))
        # Small settled loss today: $5 < 10% of $1000
        db.add(CopiedTrade(
            signal_id=None,
            market_ticker="SMALL-LOSS",
            side="yes",
            action="buy",
            contracts=10,
            price=0.50,
            cost=5.0,
            kalshi_order_id="small-loss",
            status="settled",
            is_simulated=True,
            pnl=-5.0,
            settled_at=datetime.now(timezone.utc),
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_awaited_once()

    def test_per_trader_exposure_breach_skips_signal(self, db):
        """Signal is skipped when open costs for its trader breach max_per_trader_exposure_pct."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # Two prior signals from the same trader, each with an open copied trade
        # Total: $90 + $90 = $180 > 15% of $1000 ($150)
        for _ in range(2):
            prior_signal = TradeSignal(
                trader_id=trader.id,
                market_ticker="PRIOR-MARKET",
                side="yes",
                action="buy",
                detected_price=50.0,
                confidence=0.88,
                status="copied",
            )
            db.add(prior_signal)
            db.flush()
            db.add(CopiedTrade(
                signal_id=prior_signal.id,
                market_ticker="PRIOR-MARKET",
                side="yes",
                action="buy",
                contracts=100,
                price=0.90,
                cost=90.0,
                kalshi_order_id=f"prior-order-{prior_signal.id}",
                status="pending",
                is_simulated=True,
            ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        db.refresh(signal)
        assert signal.status == "skipped"

    def test_per_trader_exposure_pass_executes_signal(self, db):
        """Signal executes when per-trader open costs are below max_per_trader_exposure_pct."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # One prior open trade for this trader: $50 < 15% of $1000 ($150)
        prior_signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="PRIOR-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.88,
            status="copied",
        )
        db.add(prior_signal)
        db.flush()
        db.add(CopiedTrade(
            signal_id=prior_signal.id,
            market_ticker="PRIOR-MARKET",
            side="yes",
            action="buy",
            contracts=100,
            price=0.50,
            cost=50.0,
            kalshi_order_id="prior-order",
            status="pending",
            is_simulated=True,
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_awaited_once()

    def test_per_trader_exposure_only_counts_same_trader(self, db):
        """Open costs from a different trader do not count toward per-trader limit."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        # A second trader with heavy open exposure
        other_trader = TrackedTrader(
            kalshi_username="other_whale",
            elephant_score=85.0,
            win_rate=0.70,
            total_trades=30,
        )
        db.add(other_trader)
        db.flush()

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # Other trader has $200 open — would breach if counted for our trader
        other_signal = TradeSignal(
            trader_id=other_trader.id,
            market_ticker="OTHER-MARKET",
            side="yes",
            action="buy",
            detected_price=50.0,
            confidence=0.88,
            status="copied",
        )
        db.add(other_signal)
        db.flush()
        db.add(CopiedTrade(
            signal_id=other_signal.id,
            market_ticker="OTHER-MARKET",
            side="yes",
            action="buy",
            contracts=200,
            price=1.00,
            cost=200.0,
            kalshi_order_id="other-order",
            status="pending",
            is_simulated=True,
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        # Our trader has $0 open exposure → should execute
        sim.assert_awaited_once()

    def test_max_drawdown_skips_signal(self, db):
        """Signal is skipped when portfolio has dropped more than max_drawdown_pct from its 30-day peak."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot
        from datetime import timedelta

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        # Peak snapshot 15 days ago: $1000
        peak_time = datetime.now(timezone.utc) - timedelta(days=15)
        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
            created_at=peak_time,
        ))
        # Current snapshot: $700 → drawdown = 30% > 25% limit
        db.add(PortfolioSnapshot(
            balance=700.0, positions_value=0.0, total_value=700.0, total_pnl=-300.0,
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            mock_settings.max_drawdown_pct = 0.25
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        db.refresh(signal)
        assert signal.status == "skipped"

    def test_max_drawdown_within_limit_executes(self, db):
        """Signal executes when drawdown from 30-day peak is below max_drawdown_pct."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot
        from datetime import timedelta

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        # Peak snapshot 15 days ago: $1000
        peak_time = datetime.now(timezone.utc) - timedelta(days=15)
        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
            created_at=peak_time,
        ))
        # Current snapshot: $850 → drawdown = 15% < 25% limit
        db.add(PortfolioSnapshot(
            balance=850.0, positions_value=0.0, total_value=850.0, total_pnl=-150.0,
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            mock_settings.max_drawdown_pct = 0.25
            _run(execution_service.execute_signal(signal.id))

        sim.assert_awaited_once()

    def test_max_drawdown_ignores_snapshots_older_than_30_days(self, db):
        """Peak snapshots older than 30 days are excluded from the drawdown calculation."""
        from app.services import execution_service
        from app.models import PortfolioSnapshot
        from datetime import timedelta

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        # Old peak 40 days ago: $10000 (outside the 30-day window, ignored)
        old_peak_time = datetime.now(timezone.utc) - timedelta(days=40)
        db.add(PortfolioSnapshot(
            balance=10000.0, positions_value=0.0, total_value=10000.0, total_pnl=0.0,
            created_at=old_peak_time,
        ))
        # Current snapshot within 30-day window: $850 (only snapshot in window)
        db.add(PortfolioSnapshot(
            balance=850.0, positions_value=0.0, total_value=850.0, total_pnl=0.0,
        ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            mock_settings.max_drawdown_pct = 0.25
            _run(execution_service.execute_signal(signal.id))

        # Old peak is excluded; peak within window = current = 0% drawdown → executes
        sim.assert_awaited_once()


# ---------------------------------------------------------------------------
# Stop-loss checks
# ---------------------------------------------------------------------------

class TestCheckStopLosses:
    """Tests for check_stop_losses() and its helpers."""

    def _make_open_trade(self, db, *, market_ticker="TEST-MARKET", side="yes",
                         price=0.50, contracts=10, status="simulated") -> CopiedTrade:
        trade = CopiedTrade(
            signal_id=None,
            market_ticker=market_ticker,
            side=side,
            action="buy",
            contracts=contracts,
            price=price,
            cost=contracts * price,
            kalshi_order_id=f"sim-{uuid.uuid4().hex[:8]}",
            status=status,
            is_simulated=True,
        )
        db.add(trade)
        db.commit()
        db.refresh(trade)
        return trade

    def test_stop_loss_triggered_dry_run(self, db):
        """Trade is marked stopped_out when loss ratio exceeds stop_loss_pct in dry-run."""
        from app.services.execution_service import check_stop_losses

        # Entry 50¢, current bid 30¢ → loss = (0.50-0.30)*10 / 5.00 = 40% > 20%
        trade = self._make_open_trade(db, price=0.50, contracts=10)
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"yes_bid": 30})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = True
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "stopped_out"
        assert trade.pnl == pytest.approx(-2.0)  # 10 * (0.30 - 0.50)
        assert trade.settled_at is not None

    def test_stop_loss_not_triggered_below_threshold(self, db):
        """Trade is left open when loss is below stop_loss_pct."""
        from app.services.execution_service import check_stop_losses

        # Entry 50¢, current bid 45¢ → loss = (0.50-0.45)*10 / 5.00 = 10% < 20%
        trade = self._make_open_trade(db, price=0.50, contracts=10)
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"yes_bid": 45})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = True
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "simulated"  # unchanged
        assert trade.pnl is None

    def test_already_closed_trades_are_skipped(self, db):
        """Settled and cancelled trades are not re-evaluated."""
        from app.services.execution_service import check_stop_losses

        settled = self._make_open_trade(db, status="settled")
        cancelled = self._make_open_trade(db, status="cancelled")
        stopped = self._make_open_trade(db, status="stopped_out")
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"yes_bid": 1})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = True
            _run(check_stop_losses(db))

        # get_market should never be called since no eligible open trades
        mock_client.get_market.assert_not_awaited()

    def test_market_fetch_failure_skips_trade(self, db):
        """If get_market() raises, the trade is left unchanged (will retry next run)."""
        from app.services.execution_service import check_stop_losses

        trade = self._make_open_trade(db, price=0.50, contracts=10)
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(side_effect=Exception("network error"))

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = True
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "simulated"  # unchanged

    def test_no_price_in_market_data_skips_trade(self, db):
        """When market data contains no bid/last_price, trade is left unchanged."""
        from app.services.execution_service import check_stop_losses

        trade = self._make_open_trade(db, price=0.50, contracts=10)
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={})  # no price fields

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = True
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "simulated"

    def test_stop_loss_live_cancel_success(self, db):
        """In live mode a successful cancel_order marks the trade stopped_out."""
        from app.services.execution_service import check_stop_losses

        trade = self._make_open_trade(db, price=0.50, contracts=10, status="pending")
        trade.is_simulated = False
        db.commit()

        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"yes_bid": 30})
        mock_client.cancel_order = AsyncMock(return_value={"order_id": trade.kalshi_order_id})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = False
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "stopped_out"
        mock_client.cancel_order.assert_awaited_once_with(trade.kalshi_order_id)

    def test_stop_loss_live_cancel_fails_places_sell(self, db):
        """In live mode when cancel_order fails, a closing sell order is placed."""
        from app.services.execution_service import check_stop_losses

        trade = self._make_open_trade(db, price=0.50, contracts=10, status="filled")
        trade.is_simulated = False
        db.commit()

        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"yes_bid": 30})
        mock_client.cancel_order = AsyncMock(side_effect=Exception("already filled"))
        mock_client.place_order = AsyncMock(return_value={"order_id": "close-order-id"})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client), \
             patch("app.services.execution_service.settings") as mock_settings:
            mock_settings.stop_loss_pct = 0.20
            mock_settings.dry_run = False
            _run(check_stop_losses(db))

        db.refresh(trade)
        assert trade.status == "stopped_out"
        mock_client.place_order.assert_awaited_once_with(
            ticker=trade.market_ticker,
            side=trade.side,
            count=trade.contracts,
            price=30,
            action="sell",
        )

    def test_get_exit_price_uses_no_bid_for_no_side(self, db):
        """_get_exit_price_cents returns no_bid for NO-side trades."""
        from app.services.execution_service import _get_exit_price_cents

        market = {"yes_bid": 40, "no_bid": 58, "last_price": 42}
        assert _get_exit_price_cents(market, "no") == 58
        assert _get_exit_price_cents(market, "yes") == 40

    def test_get_exit_price_falls_back_to_last_price(self, db):
        """_get_exit_price_cents falls back to last_price when bid is absent."""
        from app.services.execution_service import _get_exit_price_cents

        market = {"last_price": 35}
        assert _get_exit_price_cents(market, "yes") == 35
        assert _get_exit_price_cents(market, "no") == 35

    def test_get_exit_price_returns_none_when_no_data(self, db):
        """_get_exit_price_cents returns None when no price fields are present."""
        from app.services.execution_service import _get_exit_price_cents

        assert _get_exit_price_cents({}, "yes") is None


# ---------------------------------------------------------------------------
# _settle_real partial-fill handling
# ---------------------------------------------------------------------------

class TestSettleReal:
    """Tests for settlement_service._settle_real partial/zero fill branches."""

    def _make_live_trade(self, db, *, contracts=10, price=0.50, side="yes") -> CopiedTrade:
        trade = CopiedTrade(
            signal_id=None,
            market_ticker="TEST-MARKET",
            side=side,
            action="buy",
            contracts=contracts,
            price=price,
            cost=contracts * price,
            kalshi_order_id=f"order-{uuid.uuid4().hex[:8]}",
            status="pending",
            is_simulated=False,
        )
        db.add(trade)
        db.commit()
        db.refresh(trade)
        return trade

    def test_partial_fill_updates_contracts_and_pnl(self, db):
        """When only 6 of 10 contracts fill, trade.contracts==6 and PnL uses 6."""
        from app.services import settlement_service

        trade = self._make_live_trade(db, contracts=10, price=0.50, side="yes")

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(return_value={
            "status": "filled",
            "close_price": 100,
            "yes_price": 50,
            "filled_count": 6,
        })

        _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert trade.contracts == 6
        assert trade.status == "partial"
        assert trade.pnl == pytest.approx((100 - 50) * 6 / 100)
        assert trade.settled_at is not None

    def test_zero_fill_marks_cancelled(self, db):
        """When filled_count==0, trade is marked cancelled and returns 0."""
        from app.services import settlement_service

        trade = self._make_live_trade(db, contracts=10, price=0.50)

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(return_value={
            "status": "filled",
            "close_price": 100,
            "yes_price": 50,
            "filled_count": 0,
        })

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "cancelled"
        assert trade.pnl is None

    def test_full_fill_marks_settled_with_correct_contracts(self, db):
        """When filled_count equals requested contracts, status is settled."""
        from app.services import settlement_service

        trade = self._make_live_trade(db, contracts=10, price=0.50)

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(return_value={
            "status": "filled",
            "close_price": 100,
            "yes_price": 50,
            "filled_count": 10,
        })

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.contracts == 10
        assert trade.status == "settled"
        assert trade.pnl == pytest.approx((100 - 50) * 10 / 100)

    def test_no_fill_count_assumes_full_fill(self, db):
        """When filled_count is absent, falls back to trade.contracts."""
        from app.services import settlement_service

        trade = self._make_live_trade(db, contracts=10, price=0.50)

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(return_value={
            "status": "filled",
            "close_price": 100,
            "yes_price": 50,
            # no filled_count key
        })

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.contracts == 10


# ---------------------------------------------------------------------------
# execute_signal integration tests
# ---------------------------------------------------------------------------


class TestExecuteSignalIntegration:
    """End-to-end integration tests for execute_signal covering key business paths."""

    def _make_db_factory(self, db):
        mock_db = MagicMock(wraps=db)
        mock_db.close = MagicMock()
        factory = MagicMock(return_value=mock_db)
        return factory, mock_db

    def _patch_settings(self, mock_settings):
        mock_settings.dry_run = True
        mock_settings.max_position_pct = 0.05
        mock_settings.paper_balance_initial = 1000.0
        mock_settings.max_total_exposure_pct = 0.30
        mock_settings.max_daily_loss_pct = 0.10
        mock_settings.max_per_trader_exposure_pct = 0.15
        mock_settings.max_drawdown_pct = 0.25
        mock_settings.max_trades_per_market = 3

    def test_null_detected_price_is_skipped(self, db):
        """execute_signal returns early when detected_price is None; no trade is created."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = TradeSignal(
            trader_id=trader.id,
            market_ticker="NASDAQ-24DEC31",
            side="yes",
            action="buy",
            detected_price=None,
            confidence=0.88,
            status="pending",
        )
        db.add(signal)
        db.commit()
        db.refresh(signal)

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is None
        db.refresh(signal)
        assert signal.status == "pending"  # status unchanged

    def test_valid_price_sufficient_balance_creates_trade_and_marks_copied(self, db):
        """Signal with valid price and available balance produces a CopiedTrade (status=copied)."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        factory, mock_db = self._make_db_factory(db)

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        trade = db.query(CopiedTrade).filter_by(signal_id=signal.id).first()
        assert trade is not None
        assert trade.is_simulated is True
        assert trade.status == "simulated"
        db.refresh(signal)
        assert signal.status == "copied"

    def test_insufficient_balance_skips_execution(self, db):
        """Signal is skipped when open trade costs have exhausted the total-exposure budget."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        db.add(PortfolioSnapshot(
            balance=1000.0, positions_value=0.0, total_value=1000.0, total_pnl=0.0,
        ))
        # Open trades consuming > 30% of portfolio ($300 limit)
        for _ in range(4):
            db.add(CopiedTrade(
                signal_id=None,
                market_ticker="OPEN-MARKET",
                side="yes",
                action="buy",
                contracts=100,
                price=0.80,
                cost=80.0,
                kalshi_order_id="blocking-order",
                status="pending",
                is_simulated=True,
            ))
        db.commit()

        factory, mock_db = self._make_db_factory(db)
        sim = AsyncMock()

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory), \
             patch.object(execution_service, "_execute_simulated", sim):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))

        sim.assert_not_awaited()
        db.refresh(signal)
        assert signal.status == "skipped"

    def test_duplicate_execution_is_idempotent(self, db):
        """Calling execute_signal twice for the same signal creates exactly one CopiedTrade."""
        from app.services import execution_service

        trader = _make_trader(db)
        signal = _make_signal(db, trader, price=40.0)

        factory, mock_db = self._make_db_factory(db)

        with patch.object(execution_service, "settings") as mock_settings, \
             patch("app.db.SessionLocal", factory):
            self._patch_settings(mock_settings)
            _run(execution_service.execute_signal(signal.id))   # first: executes
            _run(execution_service.execute_signal(signal.id))   # second: no-op

        trades = db.query(CopiedTrade).filter_by(signal_id=signal.id).all()
        assert len(trades) == 1
        db.refresh(signal)
        assert signal.status == "copied"
        assert trades[0].status == "simulated"
