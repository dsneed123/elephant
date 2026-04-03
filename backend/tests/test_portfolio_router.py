"""Tests for GET /api/portfolio/traders endpoint."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import CopiedTrade, TradeSignal, TrackedTrader


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = Session()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def client(db_session):
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.pop(get_db, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _add_trader(db, username, elephant_score=80.0, tier="top_1", display_name=None):
    trader = TrackedTrader(
        kalshi_username=username,
        display_name=display_name,
        elephant_score=elephant_score,
        tier=tier,
        is_active=True,
    )
    db.add(trader)
    db.flush()
    return trader


def _add_signal(db, trader):
    signal = TradeSignal(
        trader_id=trader.id,
        market_ticker="AAPL-2024",
        side="yes",
        action="buy",
        detected_price=0.55,
        detected_volume=100,
        confidence=0.9,
        status="copied",
    )
    db.add(signal)
    db.flush()
    return signal


def _add_trade(db, signal, pnl, cost=10.0, status="settled"):
    trade = CopiedTrade(
        signal_id=signal.id,
        market_ticker=signal.market_ticker,
        side="yes",
        action="buy",
        contracts=10,
        price=0.55,
        cost=cost,
        status=status,
        is_simulated=True,
        pnl=pnl,
    )
    db.add(trade)
    db.flush()
    return trade


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTraderPnlAttribution:
    def test_empty_returns_empty_list(self, client):
        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_single_trader_winning(self, client, db_session):
        trader = _add_trader(db_session, "alice", elephant_score=90.0)
        signal = _add_signal(db_session, trader)
        _add_trade(db_session, signal, pnl=5.0, cost=10.0)
        _add_trade(db_session, signal, pnl=3.0, cost=10.0)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        row = data[0]
        assert row["kalshi_username"] == "alice"
        assert row["trade_count"] == 2
        assert row["total_pnl"] == pytest.approx(8.0)
        assert row["total_cost"] == pytest.approx(20.0)
        assert row["win_rate"] == pytest.approx(1.0)
        assert row["roi"] == pytest.approx(0.4)

    def test_win_rate_partial(self, client, db_session):
        trader = _add_trader(db_session, "bob")
        signal = _add_signal(db_session, trader)
        _add_trade(db_session, signal, pnl=4.0, cost=10.0)
        _add_trade(db_session, signal, pnl=-2.0, cost=10.0)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        row = resp.json()[0]
        assert row["win_rate"] == pytest.approx(0.5)
        assert row["total_pnl"] == pytest.approx(2.0)

    def test_sorted_by_total_pnl_descending(self, client, db_session):
        for username, pnl in [("charlie", -5.0), ("alice", 20.0), ("bob", 10.0)]:
            trader = _add_trader(db_session, username)
            signal = _add_signal(db_session, trader)
            _add_trade(db_session, signal, pnl=pnl, cost=10.0)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        usernames = [r["kalshi_username"] for r in resp.json()]
        assert usernames == ["alice", "bob", "charlie"]

    def test_excludes_non_settled_trades(self, client, db_session):
        trader = _add_trader(db_session, "dave")
        signal = _add_signal(db_session, trader)
        _add_trade(db_session, signal, pnl=None, cost=10.0, status="filled")
        _add_trade(db_session, signal, pnl=None, cost=10.0, status="pending")
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_excludes_trades_without_signal(self, client, db_session):
        # A trade with no signal_id cannot be attributed to a trader
        trade = CopiedTrade(
            signal_id=None,
            market_ticker="AAPL-2024",
            side="yes",
            action="buy",
            contracts=5,
            price=0.5,
            cost=5.0,
            status="settled",
            is_simulated=True,
            pnl=2.0,
        )
        db_session.add(trade)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_response_includes_trader_metadata(self, client, db_session):
        trader = _add_trader(
            db_session,
            "eve",
            elephant_score=75.5,
            tier="top_01",
            display_name="Eve Smith",
        )
        signal = _add_signal(db_session, trader)
        _add_trade(db_session, signal, pnl=1.0, cost=10.0)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        row = resp.json()[0]
        assert row["elephant_score"] == pytest.approx(75.5)
        assert row["tier"] == "top_01"
        assert row["display_name"] == "Eve Smith"

    def test_multiple_signals_same_trader(self, client, db_session):
        trader = _add_trader(db_session, "frank")
        sig1 = _add_signal(db_session, trader)
        sig2 = _add_signal(db_session, trader)
        _add_trade(db_session, sig1, pnl=3.0, cost=10.0)
        _add_trade(db_session, sig2, pnl=-1.0, cost=10.0)
        db_session.commit()

        resp = client.get("/api/portfolio/traders")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        row = data[0]
        assert row["trade_count"] == 2
        assert row["total_pnl"] == pytest.approx(2.0)
        assert row["total_cost"] == pytest.approx(20.0)
