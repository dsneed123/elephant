"""Tests for LeaderboardScraper scoring, normalization, and market enrichment."""

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import TrackedTrader
from app.services.leaderboard_scraper import LeaderboardScraper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def scraper():
    return LeaderboardScraper()


@pytest.fixture
def db():
    """In-memory SQLite session for upsert tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def _make_http_response(status_code: int, json_body: dict) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


class TestComputeElephantScore:
    def _data(self, **kwargs):
        base = {
            "win_rate": 0.0,
            "total_profit": 0.0,
            "consistency_score": 0.0,
            "market_diversity": 0,
        }
        base.update(kwargs)
        return base

    def test_zero_data_returns_ten(self, scraper):
        # All zeros except recency (no last_active → defaults to 1.0)
        # raw = 0.10 * 1.0 = 0.10 → 10.0
        score = scraper._compute_elephant_score(self._data())
        assert score == pytest.approx(10.0, abs=0.01)

    def test_weights_sum_correctly(self, scraper):
        # Perfect trader, active now → score should be 100
        data = self._data(
            win_rate=1.0,
            total_profit=10_000.0,
            consistency_score=1.0,
            market_diversity=20,
        )
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(100.0, abs=0.01)

    def test_recency_within_7_days(self, scraper):
        last_active = datetime.now(timezone.utc) - timedelta(days=3)
        data = self._data(win_rate=1.0, consistency_score=1.0, market_diversity=20,
                          total_profit=10_000.0, last_active=last_active)
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(100.0, abs=0.01)

    def test_recency_exactly_7_days(self, scraper):
        last_active = datetime.now(timezone.utc) - timedelta(days=7)
        data = self._data(last_active=last_active)
        score_7 = scraper._compute_elephant_score(data)
        score_no_date = scraper._compute_elephant_score(self._data())
        # Both should have recency=1.0
        assert score_7 == pytest.approx(score_no_date, abs=0.1)

    def test_recency_at_90_days_is_zero(self, scraper):
        last_active = datetime.now(timezone.utc) - timedelta(days=90)
        data = self._data(last_active=last_active)
        # recency=0, all other zeros → raw=0 → score=0
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_recency_beyond_90_days_clamped(self, scraper):
        last_active = datetime.now(timezone.utc) - timedelta(days=200)
        data = self._data(last_active=last_active)
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_recency_midpoint_linear(self, scraper):
        # At day 48.5 (midpoint of 7–90), recency ≈ 0.5
        days_mid = 7 + (90 - 7) / 2  # = 48.5
        last_active = datetime.now(timezone.utc) - timedelta(days=days_mid)
        data = self._data(last_active=last_active)
        score = scraper._compute_elephant_score(data)
        # Only recency contributes (others = 0), weight 10%
        expected = round(0.10 * 0.5 * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.2)

    def test_win_rate_weight_30pct(self, scraper):
        # win_rate=1, all else zero, no last_active (recency=1.0 → 10%)
        data = self._data(win_rate=1.0)
        score = scraper._compute_elephant_score(data)
        # 0.30*1 + 0.10*1 = 0.40 → 40.0
        assert score == pytest.approx(40.0, abs=0.01)

    def test_consistency_weight_25pct(self, scraper):
        data = self._data(consistency_score=1.0)
        score = scraper._compute_elephant_score(data)
        # 0.25 + 0.10 = 0.35 → 35.0
        assert score == pytest.approx(35.0, abs=0.01)

    def test_diversity_weight_15pct(self, scraper):
        data = self._data(market_diversity=20)
        score = scraper._compute_elephant_score(data)
        # 0.15 + 0.10 = 0.25 → 25.0
        assert score == pytest.approx(25.0, abs=0.01)

    def test_profit_log_scaling(self, scraper):
        # At $10k profit, profit_score should be 1.0
        data = self._data(total_profit=10_000.0)
        score = scraper._compute_elephant_score(data)
        # 0.20*1.0 + 0.10*1.0 = 0.30 → 30.0
        assert score == pytest.approx(30.0, abs=0.01)

    def test_negative_profit_ignored(self, scraper):
        data = self._data(total_profit=-500.0)
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(10.0, abs=0.01)  # only recency

    def test_win_rate_clamped_above_1(self, scraper):
        data = self._data(win_rate=1.5)
        score_clamped = scraper._compute_elephant_score(data)
        data_normal = self._data(win_rate=1.0)
        score_normal = scraper._compute_elephant_score(data_normal)
        assert score_clamped == score_normal


# ---------------------------------------------------------------------------
# _normalize_entry — top_markets extraction
# ---------------------------------------------------------------------------

class TestNormalizeEntryTopMarkets:
    def test_extracts_top_markets_list(self, scraper):
        entry = {"username": "alice", "top_markets": ["MARKET-A", "MARKET-B"]}
        result = scraper._normalize_entry(entry, 0)
        assert result["top_markets"] == ["MARKET-A", "MARKET-B"]

    def test_extracts_topMarkets_camelcase(self, scraper):
        entry = {"username": "alice", "topMarkets": ["MARKET-X"]}
        result = scraper._normalize_entry(entry, 0)
        assert result["top_markets"] == ["MARKET-X"]

    def test_no_market_data_gives_empty_list(self, scraper):
        entry = {"username": "alice"}
        result = scraper._normalize_entry(entry, 0)
        assert result["top_markets"] == []

    def test_non_list_market_data_gives_empty_list(self, scraper):
        entry = {"username": "alice", "top_markets": "MARKET-A"}
        result = scraper._normalize_entry(entry, 0)
        assert result["top_markets"] == []


# ---------------------------------------------------------------------------
# _upsert_trader — top_markets persistence
# ---------------------------------------------------------------------------

class TestUpsertTraderTopMarkets:
    def _base_data(self, **kwargs):
        data = {
            "username": "whale_trader",
            "display_name": "Whale Trader",
            "total_profit": 5000.0,
            "win_rate": 0.75,
            "total_trades": 50,
            "avg_position_size": 100.0,
            "market_diversity": 10,
            "consistency_score": 0.8,
            "top_markets": [],
        }
        data.update(kwargs)
        return data

    def test_new_trader_stores_top_markets(self, scraper, db):
        data = self._base_data(top_markets=["NASDAQ-24DEC31", "FED-24DEC18"])
        trader = scraper._upsert_trader(db, data, rank=1, total=10)
        db.flush()
        assert trader.top_markets == json.dumps(["NASDAQ-24DEC31", "FED-24DEC18"])

    def test_new_trader_with_empty_markets_stores_null(self, scraper, db):
        data = self._base_data(top_markets=[])
        trader = scraper._upsert_trader(db, data, rank=1, total=10)
        db.flush()
        assert trader.top_markets is None

    def test_existing_trader_markets_updated_when_new_data(self, scraper, db):
        # Insert trader with initial markets
        data = self._base_data(top_markets=["MARKET-A"])
        scraper._upsert_trader(db, data, rank=1, total=10)
        db.commit()

        # Update with new markets
        data["top_markets"] = ["MARKET-A", "MARKET-B"]
        trader = scraper._upsert_trader(db, data, rank=1, total=10)
        db.commit()
        assert json.loads(trader.top_markets) == ["MARKET-A", "MARKET-B"]

    def test_existing_trader_markets_preserved_when_no_new_data(self, scraper, db):
        # Insert trader with markets
        data = self._base_data(top_markets=["MARKET-A"])
        scraper._upsert_trader(db, data, rank=1, total=10)
        db.commit()

        # Update with empty markets — existing data should be preserved
        data["top_markets"] = []
        trader = scraper._upsert_trader(db, data, rank=1, total=10)
        db.commit()
        assert json.loads(trader.top_markets) == ["MARKET-A"]


# ---------------------------------------------------------------------------
# _fetch_trader_markets — Kalshi public API
# ---------------------------------------------------------------------------

class TestFetchTraderMarkets:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_returns_sorted_tickers_from_trades(self, scraper):
        resp = _make_http_response(200, {
            "trades": [
                {"ticker": "NASDAQ-24DEC31"},
                {"ticker": "FED-24DEC18"},
                {"ticker": "NASDAQ-24DEC31"},  # duplicate — should be deduplicated
            ]
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == ["FED-24DEC18", "NASDAQ-24DEC31"]

    def test_returns_empty_on_401(self, scraper):
        resp = _make_http_response(401, {})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == []

    def test_returns_empty_on_403(self, scraper):
        resp = _make_http_response(403, {})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == []

    def test_returns_empty_on_404(self, scraper):
        resp = _make_http_response(404, {})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == []

    def test_returns_empty_on_network_error(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.RequestError("timeout"))
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == []

    def test_handles_market_ticker_field_name(self, scraper):
        resp = _make_http_response(200, {
            "trades": [{"market_ticker": "BTCUSDT-24DEC31"}]
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == ["BTCUSDT-24DEC31"]

    def test_empty_trades_list_returns_empty(self, scraper):
        resp = _make_http_response(200, {"trades": []})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_trader_markets(client, "alice"))
        assert result == []
