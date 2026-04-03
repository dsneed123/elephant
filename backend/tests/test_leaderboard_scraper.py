"""Tests for LeaderboardScraper: scoring, merging, persistence, and pipeline."""

import asyncio
import math
from unittest.mock import AsyncMock, MagicMock, patch

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


# ---------------------------------------------------------------------------
# _compute_elephant_score
# ---------------------------------------------------------------------------

class TestComputeElephantScore:
    def test_rank1_all_metrics_returns_100(self, scraper):
        data = {"rank_pnl": 1, "rank_volume": 1, "rank_markets": 1, "markets_traded": 100}
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(100.0, abs=0.1)

    def test_no_rank_data_returns_zero(self, scraper):
        # rank defaults to 999 → pnl_score=0, vol_score=0; markets=0; cross=0
        score = scraper._compute_elephant_score({})
        assert score == pytest.approx(0.0, abs=0.01)

    def test_pnl_rank1_contributes_correctly(self, scraper):
        # rank_pnl=1 → pnl_score=1.0, cross_score=1/3
        data = {"rank_pnl": 1}
        score = scraper._compute_elephant_score(data)
        expected = round((0.40 * 1.0 + 0.15 * (1 / 3)) * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.01)

    def test_rank_201_gives_zero_pnl_score(self, scraper):
        # pnl_score = max(0, 1 - 200/200) = 0; cross_score = 1/3
        data = {"rank_pnl": 201}
        score = scraper._compute_elephant_score(data)
        expected = round(0.15 * (1 / 3) * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.01)

    def test_diversity_log_scaled(self, scraper):
        # markets_traded=100 → diversity_score=1.0; no rank keys
        data = {"markets_traded": 100}
        score = scraper._compute_elephant_score(data)
        expected = round(0.20 * 1.0 * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.01)

    def test_cross_metric_bonus_all_three_present(self, scraper):
        # rank keys present but beyond 200; only cross contributes
        data = {"rank_pnl": 999, "rank_volume": 999, "rank_markets": 999}
        score = scraper._compute_elephant_score(data)
        expected = round(0.15 * 1.0 * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.01)

    def test_cross_metric_bonus_one_metric(self, scraper):
        data = {"rank_pnl": 999}
        score = scraper._compute_elephant_score(data)
        expected = round(0.15 * (1 / 3) * 100.0, 2)
        assert score == pytest.approx(expected, abs=0.01)


# ---------------------------------------------------------------------------
# _assign_tier
# ---------------------------------------------------------------------------

class TestAssignTier:
    @pytest.mark.parametrize("rank,expected", [
        (1, "top_001"),
        (10, "top_001"),
        (11, "top_01"),
        (25, "top_01"),
        (26, "top_1"),
        (50, "top_1"),
        (51, "top_25"),
        (100, "top_25"),
        (101, "ranked"),
        (500, "ranked"),
    ])
    def test_tier_assignment(self, scraper, rank, expected):
        assert scraper._assign_tier(rank) == expected


# ---------------------------------------------------------------------------
# _fetch_metric — Kalshi leaderboard API
# ---------------------------------------------------------------------------

class TestFetchMetric:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_returns_rank_list_on_success(self, scraper):
        resp = _make_http_response(200, {
            "rank_list": [
                {"nickname": "trader1", "rank": 1, "value": 5000.0, "is_anonymous": False},
                {"nickname": "trader2", "rank": 2, "value": 3000.0, "is_anonymous": False},
            ]
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_metric(client, "projected_pnl", "weekly", 50))
        assert len(result) == 2
        assert result[0]["nickname"] == "trader1"

    def test_returns_empty_list_on_success_with_no_data(self, scraper):
        resp = _make_http_response(200, {"rank_list": []})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_metric(client, "projected_pnl", "weekly", 50))
        assert result == []

    def test_returns_empty_on_http_error(self, scraper):
        resp = _make_http_response(500, {})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_metric(client, "projected_pnl", "weekly", 50))
        assert result == []

    def test_returns_empty_on_network_error(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.RequestError("connection refused"))
        result = self._run(scraper._fetch_metric(client, "projected_pnl", "weekly", 50))
        assert result == []

    @pytest.mark.parametrize("status_code", [401, 403, 404])
    def test_returns_empty_on_4xx_error(self, scraper, status_code):
        resp = _make_http_response(status_code, {})
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        result = self._run(scraper._fetch_metric(client, "projected_pnl", "weekly", 50))
        assert result == []


# ---------------------------------------------------------------------------
# fetch_all_metrics — merging logic
# ---------------------------------------------------------------------------

class TestFetchAllMetrics:
    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_client(self, responses: dict) -> MagicMock:
        """Build a mock AsyncClient that dispatches by metric_name param."""
        async def fake_get(url, params=None):
            metric = (params or {}).get("metric_name", "")
            entries = responses.get(metric, [])
            return _make_http_response(200, {"rank_list": entries})

        client = MagicMock()
        client.get = AsyncMock(side_effect=fake_get)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        return client

    def test_merges_pnl_and_volume_for_same_trader(self, scraper):
        mock = self._mock_client({
            "projected_pnl": [
                {"nickname": "alice", "rank": 1, "value": 5000.0, "is_anonymous": False}
            ],
            "volume": [
                {"nickname": "alice", "rank": 2, "value": 10000.0, "is_anonymous": False}
            ],
        })
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient", return_value=mock), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = self._run(scraper.fetch_all_metrics())

        assert "alice" in result
        assert result["alice"]["rank_pnl"] == 1
        assert result["alice"]["rank_volume"] == 2

    def test_skips_anonymous_entries(self, scraper):
        mock = self._mock_client({
            "projected_pnl": [
                {"nickname": "anon", "rank": 1, "value": 9999.0, "is_anonymous": True}
            ],
        })
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient", return_value=mock), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = self._run(scraper.fetch_all_metrics())

        assert result == {}

    def test_skips_empty_nickname(self, scraper):
        mock = self._mock_client({
            "projected_pnl": [
                {"nickname": "", "rank": 1, "value": 9999.0, "is_anonymous": False}
            ],
        })
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient", return_value=mock), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = self._run(scraper.fetch_all_metrics())

        assert result == {}

    def test_multiple_traders_across_metrics(self, scraper):
        mock = self._mock_client({
            "projected_pnl": [
                {"nickname": "alice", "rank": 1, "value": 5000.0, "is_anonymous": False},
                {"nickname": "bob", "rank": 2, "value": 3000.0, "is_anonymous": False},
            ],
            "num_markets_traded": [
                {"nickname": "bob", "rank": 1, "value": 20, "is_anonymous": False},
            ],
        })
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient", return_value=mock), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            result = self._run(scraper.fetch_all_metrics())

        assert "alice" in result
        assert "bob" in result
        assert result["bob"]["rank_markets"] == 1


# ---------------------------------------------------------------------------
# _upsert_trader — persistence
# ---------------------------------------------------------------------------

class TestUpsertTrader:
    def _base_data(self, **kwargs):
        data = {
            "nickname": "WhaleTrader",
            "pnl": 5000.0,
            "volume": 50000.0,
            "markets_traded": 15,
            "rank_pnl": 5,
            "rank_volume": 8,
            "rank_markets": 3,
        }
        data.update(kwargs)
        return data

    def test_new_trader_inserted_with_correct_username(self, scraper, db):
        data = self._base_data()
        trader = scraper._upsert_trader(db, data)
        db.flush()
        assert trader.kalshi_username == "whaletrader"
        assert trader.display_name == "WhaleTrader"

    def test_new_trader_has_positive_elephant_score(self, scraper, db):
        data = self._base_data()
        trader = scraper._upsert_trader(db, data)
        db.flush()
        assert trader.elephant_score is not None
        assert trader.elephant_score > 0

    def test_new_trader_win_rate_is_not_none(self, scraper, db):
        data = self._base_data()
        trader = scraper._upsert_trader(db, data)
        db.flush()
        assert trader.win_rate is not None

    def test_new_trader_is_active(self, scraper, db):
        data = self._base_data()
        trader = scraper._upsert_trader(db, data)
        db.flush()
        assert trader.is_active is True

    def test_existing_trader_profit_updated(self, scraper, db):
        data = self._base_data()
        scraper._upsert_trader(db, data)
        db.commit()

        data["pnl"] = 9000.0
        trader = scraper._upsert_trader(db, data)
        db.commit()
        assert trader.total_profit == 9000.0

    def test_existing_trader_elephant_score_recalculated(self, scraper, db):
        data = self._base_data(rank_pnl=50)
        scraper._upsert_trader(db, data)
        db.commit()
        old_score = db.query(TrackedTrader).first().elephant_score

        data["rank_pnl"] = 1
        scraper._upsert_trader(db, data)
        db.commit()
        new_score = db.query(TrackedTrader).first().elephant_score

        assert new_score > old_score


# ---------------------------------------------------------------------------
# End-to-end pipeline: the required test
# ---------------------------------------------------------------------------

class TestScrapePipelineEndToEnd:
    def test_scrape_produces_trader_with_non_null_win_rate_and_elephant_score(self, scraper, db):
        """scrape() upserts TrackedTrader rows with non-null win_rate and elephant_score > 0."""
        sample_merged = {
            "whale_one": {
                "nickname": "whale_one",
                "pnl": 5000.0,
                "volume": 40000.0,
                "markets_traded": 12,
                "rank_pnl": 3,
                "rank_volume": 5,
                "rank_markets": 2,
            }
        }

        with patch.object(scraper, "fetch_all_metrics", new=AsyncMock(return_value=sample_merged)), \
             patch.object(scraper, "is_rate_limited", return_value=False):
            asyncio.run(scraper.scrape(db))

        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "whale_one"
        ).first()
        assert trader is not None, "scrape() must upsert at least one TrackedTrader"
        assert trader.win_rate is not None, "win_rate must not be None"
        assert trader.elephant_score is not None, "elephant_score must not be None"
        assert trader.elephant_score > 0, "elephant_score must be positive for a ranked trader"
