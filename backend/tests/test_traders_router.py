"""Tests for PATCH /api/traders/{id} endpoint."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import TrackedTrader


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def client(db_session):
    app.dependency_overrides[get_db] = lambda: db_session
    yield TestClient(app)
    app.dependency_overrides.pop(get_db, None)


def _make_trader(db, is_active=True, is_enabled=True) -> TrackedTrader:
    trader = TrackedTrader(
        kalshi_username="test_trader",
        elephant_score=80.0,
        win_rate=0.70,
        total_trades=40,
        is_active=is_active,
        is_enabled=is_enabled,
    )
    db.add(trader)
    db.commit()
    db.refresh(trader)
    return trader


class TestPatchTrader:
    def test_disable_trader_sets_is_enabled_false(self, client, db_session):
        """PATCH with is_enabled=False disables the trader and returns the updated record."""
        trader = _make_trader(db_session, is_enabled=True)

        resp = client.patch(f"/api/traders/{trader.id}", json={"is_enabled": False})

        assert resp.status_code == 200
        data = resp.json()
        assert data["is_enabled"] is False
        assert data["id"] == trader.id

    def test_enable_trader_sets_is_enabled_true(self, client, db_session):
        """PATCH with is_enabled=True enables a previously disabled trader."""
        trader = _make_trader(db_session, is_enabled=False)

        resp = client.patch(f"/api/traders/{trader.id}", json={"is_enabled": True})

        assert resp.status_code == 200
        data = resp.json()
        assert data["is_enabled"] is True

    def test_patch_nonexistent_trader_returns_404(self, client, db_session):
        """PATCH on a non-existent trader ID returns 404."""
        resp = client.patch("/api/traders/99999", json={"is_enabled": False})

        assert resp.status_code == 404

    def test_patch_persists_to_db(self, client, db_session):
        """Change is reflected in the database after PATCH."""
        trader = _make_trader(db_session, is_enabled=True)

        client.patch(f"/api/traders/{trader.id}", json={"is_enabled": False})

        db_session.refresh(trader)
        assert trader.is_enabled is False
