"""Tests for settlement_service: _settle_real, _settle_simulated, settle_open_trades."""

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import CopiedTrade


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


def _make_trade(
    db,
    *,
    contracts=10,
    price=0.50,
    side="yes",
    is_simulated=False,
    status="pending",
    market_ticker="TEST-MKTX",
) -> CopiedTrade:
    trade = CopiedTrade(
        signal_id=None,
        market_ticker=market_ticker,
        side=side,
        action="buy",
        contracts=contracts,
        price=price,
        cost=contracts * price,
        kalshi_order_id=f"order-{uuid.uuid4().hex[:8]}",
        status=status,
        is_simulated=is_simulated,
    )
    db.add(trade)
    db.commit()
    db.refresh(trade)
    return trade


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# _settle_real
# ---------------------------------------------------------------------------


class TestSettleReal:
    def test_full_fill_yes_side_settled_status_and_pnl(self, db):
        """Full fill YES trade: pnl = (close_price - yes_price) * contracts / 100, status = 'settled'."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50, side="yes")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
                "filled_count": 10,
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.contracts == 10
        assert trade.status == "settled"
        assert trade.pnl == pytest.approx((100 - 50) * 10 / 100)  # 5.0
        assert trade.settled_at is not None

    def test_full_fill_no_side_uses_no_price_formula(self, db):
        """Full fill NO trade: pnl = ((100 - close_price) - no_price) * contracts / 100."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=5, price=0.45, side="no")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 0,  # NO wins
                "no_price": 45,
                "filled_count": 5,
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.status == "settled"
        # pnl = ((100 - 0) - 45) * 5 / 100 = 2.75
        assert trade.pnl == pytest.approx((100 - 0 - 45) * 5 / 100)

    def test_partial_fill_pnl_uses_filled_count_not_requested(self, db):
        """Partial fill: contracts and PnL computed on filled_count, not originally requested."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50, side="yes")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
                "filled_count": 6,
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.contracts == 6
        assert trade.status == "partial"
        # PnL must use 6, not 10
        assert trade.pnl == pytest.approx((100 - 50) * 6 / 100)  # 3.0
        assert trade.settled_at is not None

    def test_zero_fill_marks_cancelled_and_returns_zero(self, db):
        """filled_count == 0: trade marked cancelled, pnl stays None, returns 0."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50, side="yes")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
                "filled_count": 0,
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "cancelled"
        assert trade.pnl is None

    def test_order_status_cancelled_marks_trade_cancelled(self, db):
        """Kalshi order_status == 'cancelled': trade marked cancelled, returns 0."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50)
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(return_value={"status": "cancelled"})

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "cancelled"
        assert trade.pnl is None

    def test_close_price_absent_leaves_trade_open(self, db):
        """close_price absent (market not yet resolved): returns 0, trade unchanged."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50)
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "yes_price": 50,
                # close_price deliberately absent
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "pending"
        assert trade.pnl is None

    def test_fill_price_falls_back_to_trade_price_in_dollars(self, db):
        """No yes_price in order: fill_price_cents = trade.price * 100 (dollars → cents)."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.40, side="yes")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "filled_count": 10,
                # no yes_price key
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        # fill_price_cents = 0.40 * 100 = 40
        assert trade.pnl == pytest.approx((100 - 40) * 10 / 100)  # 6.0

    def test_api_error_leaves_trade_open(self, db):
        """get_order raises exception (e.g. 404): returns 0, trade left unchanged."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=10, price=0.50)
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(side_effect=Exception("404 not found"))

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "pending"
        assert trade.pnl is None

    def test_zero_contracts_trade_settles_with_zero_pnl(self, db):
        """Trade with zero contracts: no filled_count fallback gives pnl=0, status='settled'."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=0, price=0.50, side="yes")
        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
                # no filled_count; falls back to trade.contracts == 0
            }
        )

        result = _run(settlement_service._settle_real(db, trade, mock_client))

        db.refresh(trade)
        # Zero contracts → pnl = 0, not a crash
        assert result == 1
        assert trade.contracts == 0
        assert trade.pnl == pytest.approx(0.0)
        assert trade.status == "settled"


# ---------------------------------------------------------------------------
# _settle_simulated
# ---------------------------------------------------------------------------


class TestSettleSimulated:
    def test_yes_side_market_win(self, db):
        """YES-side trade, result='yes': pnl = (100 - fill_price_cents) * contracts / 100."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=5, price=0.40, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": "yes"})

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        assert trade.status == "settled"
        # close_price=100, fill_price_cents=40 → pnl = (100-40)*5/100 = 3.0
        assert trade.pnl == pytest.approx((100 - 40) * 5 / 100)
        assert trade.settled_at is not None

    def test_yes_side_market_loss(self, db):
        """YES-side trade, result='no': pnl is negative (loss)."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=5, price=0.40, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": "no"})

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        # close_price=0, fill_price_cents=40 → pnl = (0-40)*5/100 = -2.0
        assert trade.pnl == pytest.approx((0 - 40) * 5 / 100)

    def test_no_side_market_win(self, db):
        """NO-side trade, result='no': pnl = ((100-close_price) - fill_price_cents) * contracts / 100."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=3, price=0.55, side="no", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": "no"})

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        # close_price=0 (result='no'), fill_price_cents=55
        # pnl = ((100-0) - 55) * 3 / 100 = 1.35
        assert trade.pnl == pytest.approx((100 - 0 - 55) * 3 / 100)

    def test_no_side_market_loss(self, db):
        """NO-side trade, result='yes': pnl is negative."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=4, price=0.45, side="no", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": "yes"})

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 1
        # close_price=100, fill_price_cents=45
        # pnl = ((100-100) - 45) * 4 / 100 = -1.80
        assert trade.pnl == pytest.approx(((100 - 100) - 45) * 4 / 100)

    def test_is_simulated_flag_preserved_after_settlement(self, db):
        """is_simulated must remain True after _settle_simulated settles the trade."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=2, price=0.60, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": "yes"})

        _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert trade.is_simulated is True

    def test_result_none_market_not_resolved_returns_zero(self, db):
        """result is None (market not yet resolved): returns 0, trade unchanged."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=5, price=0.40, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={"result": None})

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "simulated"
        assert trade.pnl is None

    def test_result_key_absent_returns_zero(self, db):
        """result key absent from market data: returns 0, trade unchanged."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=5, price=0.40, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(return_value={})  # no result key

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.pnl is None

    def test_api_error_leaves_trade_open(self, db):
        """get_market raises exception: returns 0, trade left unchanged."""
        from app.services import settlement_service

        trade = _make_trade(
            db, contracts=5, price=0.40, side="yes", is_simulated=True, status="simulated"
        )
        mock_client = MagicMock()
        mock_client.get_market = AsyncMock(side_effect=Exception("connection timeout"))

        result = _run(settlement_service._settle_simulated(db, trade, mock_client))

        db.refresh(trade)
        assert result == 0
        assert trade.status == "simulated"
        assert trade.pnl is None


# ---------------------------------------------------------------------------
# settle_open_trades (bulk)
# ---------------------------------------------------------------------------


class TestSettleOpenTrades:
    def test_bulk_settles_all_eligible_real_trades(self, db):
        """All eligible real trades with resolved orders are settled; returns settled count."""
        from app.services import settlement_service

        t1 = _make_trade(
            db, contracts=5, price=0.50, side="yes",
            is_simulated=False, status="pending", market_ticker="MKT-A",
        )
        t2 = _make_trade(
            db, contracts=3, price=0.40, side="yes",
            is_simulated=False, status="filled", market_ticker="MKT-B",
        )

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
            }
        )

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 2
        db.refresh(t1)
        db.refresh(t2)
        assert t1.status == "settled"
        assert t2.status == "settled"

    def test_already_settled_trade_is_idempotent(self, db):
        """Trade with status='settled' is excluded from the query; pnl unchanged."""
        from app.services import settlement_service

        already_settled = _make_trade(db, contracts=5, price=0.50, status="settled")
        already_settled.pnl = 2.50
        db.commit()

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock()

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0
        mock_client.get_order.assert_not_awaited()
        db.refresh(already_settled)
        assert already_settled.pnl == pytest.approx(2.50)  # unchanged

    def test_already_cancelled_trade_is_skipped(self, db):
        """Trade with status='cancelled' is excluded from the settlement query."""
        from app.services import settlement_service

        _make_trade(db, contracts=5, price=0.50, status="cancelled")

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock()

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0
        mock_client.get_order.assert_not_awaited()

    def test_partial_status_trade_is_skipped(self, db):
        """Trade with status='partial' is excluded from the settlement query."""
        from app.services import settlement_service

        _make_trade(db, contracts=5, price=0.50, status="partial")

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock()

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0

    def test_bulk_settles_mixed_simulated_and_real_trades(self, db):
        """Simulated trades routed to _settle_simulated; real trades to _settle_real."""
        from app.services import settlement_service

        real_trade = _make_trade(
            db, contracts=4, price=0.50, side="yes",
            is_simulated=False, status="pending", market_ticker="REAL-MKT",
        )
        sim_trade = _make_trade(
            db, contracts=3, price=0.40, side="yes",
            is_simulated=True, status="simulated", market_ticker="SIM-MKT",
        )

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(
            return_value={
                "status": "filled",
                "close_price": 100,
                "yes_price": 50,
                "filled_count": 4,
            }
        )
        mock_client.get_market = AsyncMock(return_value={"result": "yes"})

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 2
        db.refresh(real_trade)
        db.refresh(sim_trade)
        assert real_trade.status == "settled"
        assert sim_trade.status == "settled"
        assert sim_trade.is_simulated is True  # flag preserved

    def test_no_eligible_trades_returns_zero(self, db):
        """When no eligible trades exist, returns 0 without calling the Kalshi API."""
        from app.services import settlement_service

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock()

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0
        mock_client.get_order.assert_not_awaited()

    def test_market_404_leaves_trade_open(self, db):
        """get_order raises exception (e.g. 404): trade remains open, count = 0."""
        from app.services import settlement_service

        trade = _make_trade(db, contracts=5, price=0.50, status="pending")

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock(side_effect=Exception("404 not found"))

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0
        db.refresh(trade)
        assert trade.status == "pending"
        assert trade.pnl is None

    def test_trade_without_order_id_is_excluded(self, db):
        """Trade with kalshi_order_id=None is filtered from the settlement query."""
        from app.services import settlement_service

        trade = CopiedTrade(
            signal_id=None,
            market_ticker="TEST-MKTX",
            side="yes",
            action="buy",
            contracts=5,
            price=0.50,
            cost=2.50,
            kalshi_order_id=None,
            status="pending",
            is_simulated=False,
        )
        db.add(trade)
        db.commit()

        mock_client = MagicMock()
        mock_client.get_order = AsyncMock()

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 0
        mock_client.get_order.assert_not_awaited()

    def test_one_failure_does_not_block_other_trades(self, db):
        """If one trade's order fetch fails, remaining trades are still settled."""
        from app.services import settlement_service

        failing_trade = _make_trade(
            db, contracts=5, price=0.50, status="pending", market_ticker="FAIL-MKT"
        )
        ok_trade = _make_trade(
            db, contracts=3, price=0.50, side="yes",
            status="pending", market_ticker="OK-MKT",
        )

        mock_client = MagicMock()

        async def _get_order_side_effect(order_id):
            if order_id == failing_trade.kalshi_order_id:
                raise Exception("transient error")
            return {"status": "filled", "close_price": 100, "yes_price": 50, "filled_count": 3}

        mock_client.get_order = AsyncMock(side_effect=_get_order_side_effect)

        with patch("app.services.kalshi_client.get_kalshi_client", return_value=mock_client):
            count = _run(settlement_service.settle_open_trades(db))

        assert count == 1
        db.refresh(failing_trade)
        db.refresh(ok_trade)
        assert failing_trade.status == "pending"  # left open
        assert ok_trade.status == "settled"
