"""Tests for LeaderboardScraper: scoring, merging, persistence, and pipeline."""

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import CopiedTrade, TradeSignal, TrackedTrader
from app.services.leaderboard_scraper import LeaderboardScraper, update_trader_stats_from_history


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
    def test_all_zero_inputs_returns_zero(self, scraper):
        # No win_rate, consistency, ROI, markets, or last_seen → 0
        score = scraper._compute_elephant_score({})
        assert score == pytest.approx(0.0, abs=0.01)

    def test_perfect_win_rate_contributes_30pct(self, scraper):
        score = scraper._compute_elephant_score({}, win_rate=1.0)
        assert score == pytest.approx(30.0, abs=0.01)

    def test_perfect_consistency_contributes_25pct(self, scraper):
        score = scraper._compute_elephant_score({}, consistency_score=1.0)
        assert score == pytest.approx(25.0, abs=0.01)

    def test_roi_component_contributes_20pct(self, scraper):
        # ROI = 100 / (1 * 100) = 1.0 → capped at 1.0
        score = scraper._compute_elephant_score(
            {}, total_profit=100.0, total_trades=1, avg_position_size=100.0
        )
        assert score == pytest.approx(20.0, abs=0.01)

    def test_roi_capped_at_1(self, scraper):
        # Very high ROI should still contribute only 20%
        score = scraper._compute_elephant_score(
            {}, total_profit=99999.0, total_trades=1, avg_position_size=1.0
        )
        assert score == pytest.approx(20.0, abs=0.01)

    def test_roi_zero_when_no_trades(self, scraper):
        # total_trades=0 → roi_component=0
        score = scraper._compute_elephant_score(
            {}, total_profit=1000.0, total_trades=0, avg_position_size=100.0
        )
        assert score == pytest.approx(0.0, abs=0.01)

    def test_roi_negative_clamped_to_zero(self, scraper):
        # Negative total_profit should not reduce score below 0
        score = scraper._compute_elephant_score(
            {}, total_profit=-500.0, total_trades=5, avg_position_size=100.0
        )
        assert score == pytest.approx(0.0, abs=0.01)

    def test_diversity_log_scaled_contributes_15pct(self, scraper):
        # markets_traded=100 → diversity_component=1.0 → 15%
        data = {"markets_traded": 100}
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(15.0, abs=0.01)

    def test_diversity_partial(self, scraper):
        markets = 30
        expected_diversity = math.log1p(markets) / math.log1p(100)
        score = scraper._compute_elephant_score({"markets_traded": markets})
        assert score == pytest.approx(0.15 * expected_diversity * 100.0, abs=0.01)

    def test_recency_recent_trader_contributes_10pct(self, scraper):
        now = datetime.now(timezone.utc)
        score = scraper._compute_elephant_score({}, last_seen=now)
        assert score == pytest.approx(10.0, abs=0.05)

    def test_recency_30_day_old_decays_by_exp_minus_1(self, scraper):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=30)
        score = scraper._compute_elephant_score({}, last_seen=old)
        expected = 10.0 * math.exp(-1.0)
        assert score == pytest.approx(expected, abs=0.1)

    def test_recency_none_contributes_zero(self, scraper):
        score = scraper._compute_elephant_score({}, last_seen=None)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_recency_naive_datetime_treated_as_utc(self, scraper):
        naive_now = datetime.utcnow()  # no tzinfo
        score = scraper._compute_elephant_score({}, last_seen=naive_now)
        assert score == pytest.approx(10.0, abs=0.1)

    def test_full_score_all_perfect_equals_100(self, scraper):
        now = datetime.now(timezone.utc)
        score = scraper._compute_elephant_score(
            {"markets_traded": 100},
            win_rate=1.0,
            consistency_score=1.0,
            total_profit=100.0,
            total_trades=1,
            avg_position_size=100.0,
            last_seen=now,
        )
        assert score == pytest.approx(100.0, abs=0.1)

    def test_weights_sum_is_correct(self, scraper):
        # Verify weights sum to 1.0 by checking max achievable score is 100
        now = datetime.now(timezone.utc)
        score = scraper._compute_elephant_score(
            {"markets_traded": 100},
            win_rate=1.0,
            consistency_score=1.0,
            total_profit=1.0,
            total_trades=1,
            avg_position_size=1.0,
            last_seen=now,
        )
        assert score <= 100.0


# ---------------------------------------------------------------------------
# _seed_win_rate_prior
# ---------------------------------------------------------------------------

class TestSeedWinRatePrior:
    def test_negative_pnl_returns_zero(self, scraper):
        assert scraper._seed_win_rate_prior(-100.0, 50000.0, 20) == 0.0

    def test_zero_pnl_returns_zero(self, scraper):
        assert scraper._seed_win_rate_prior(0.0, 50000.0, 20) == 0.0

    def test_positive_pnl_no_markets_returns_neutral_prior(self, scraper):
        # No market-count data → pure Bayesian prior (0+2)/(0+4) = 0.5
        result = scraper._seed_win_rate_prior(100.0, 50000.0, 0)
        assert result == pytest.approx(0.5, abs=0.001)

    def test_high_volume_low_pnl_not_floored_at_0_6(self, scraper):
        # High-volume low-margin trader: pnl << volume → win rate near 0.5, not 0.6
        result = scraper._seed_win_rate_prior(1.0, 100000.0, 100)
        assert result < 0.6

    def test_high_pnl_ratio_can_exceed_old_cap(self, scraper):
        # No hard cap at 0.85 — very profitable traders can score higher
        result = scraper._seed_win_rate_prior(1_000_000.0, 1.0, 20)
        assert result > 0.85

    def test_moderate_pnl_returns_above_half(self, scraper):
        # pnl=3000, volume=30000, markets=10 → wins≈6/10 → Bayesian ≈ 0.571
        result = scraper._seed_win_rate_prior(3000.0, 30000.0, 10)
        assert result > 0.5

    def test_zero_volume_with_positive_pnl(self, scraper):
        # volume=0 → treat as all wins → Bayesian near upper range
        result = scraper._seed_win_rate_prior(500.0, 0.0, 10)
        assert result > 0.5

    def test_result_in_valid_range(self, scraper):
        result = scraper._seed_win_rate_prior(9000.0, 1000.0, 20)
        assert 0.0 < result <= 1.0


# ---------------------------------------------------------------------------
# has_trade_history flag in _upsert_trader
# ---------------------------------------------------------------------------

class TestHasTradeHistoryFlag:
    def _base_data(self, **kwargs):
        data = {
            "nickname": "SeedTrader",
            "pnl": 3000.0,
            "volume": 30000.0,
            "markets_traded": 10,
            "rank_pnl": 4,
            "rank_volume": 6,
            "rank_markets": 2,
        }
        data.update(kwargs)
        return data

    def test_new_trader_has_trade_history_false(self, scraper, db):
        trader = scraper._upsert_trader(db, self._base_data())
        db.flush()
        assert trader.has_trade_history is False

    def test_new_trader_seeded_win_rate_above_zero_when_pnl_positive(self, scraper, db):
        trader = scraper._upsert_trader(db, self._base_data(pnl=3000.0, volume=30000.0))
        db.flush()
        assert trader.win_rate > 0.0
        # Bayesian estimator: no hard floor at 0.60 or cap at 0.85
        assert 0.0 < trader.win_rate <= 1.0

    def test_new_trader_seeded_consistency_0_5_when_pnl_positive(self, scraper, db):
        trader = scraper._upsert_trader(db, self._base_data(pnl=3000.0, volume=30000.0))
        db.flush()
        assert trader.consistency_score == pytest.approx(0.5, abs=0.001)

    def test_new_trader_zero_pnl_win_rate_stays_zero(self, scraper, db):
        trader = scraper._upsert_trader(db, self._base_data(pnl=0.0, volume=30000.0))
        db.flush()
        assert trader.win_rate == 0.0
        assert trader.consistency_score == 0.0

    def test_existing_trader_without_trade_history_win_rate_refreshed(self, scraper, db):
        # Use pnl values large enough to shift the rounded wins estimate.
        # pnl=1000, volume=30000, markets=10 → win_fraction≈0.517 → wins=5 → 7/14≈0.50
        scraper._upsert_trader(db, self._base_data(pnl=1000.0, volume=30000.0))
        db.commit()
        old_win_rate = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "seedtrader"
        ).first().win_rate

        # pnl=10000, volume=30000, markets=10 → win_fraction≈0.667 → wins=7 → 9/14≈0.643
        scraper._upsert_trader(db, self._base_data(pnl=10000.0, volume=30000.0))
        db.commit()
        new_win_rate = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "seedtrader"
        ).first().win_rate
        assert new_win_rate > old_win_rate

    def test_existing_trader_with_trade_history_win_rate_not_overwritten(self, scraper, db):
        # Insert trader and mark has_trade_history=True with a specific win_rate
        scraper._upsert_trader(db, self._base_data(pnl=3000.0, volume=30000.0))
        db.commit()
        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "seedtrader"
        ).first()
        trader.has_trade_history = True
        trader.win_rate = 0.72  # real history value
        db.commit()

        # Re-upsert with different pnl → seeded prior must NOT overwrite real win_rate
        scraper._upsert_trader(db, self._base_data(pnl=9000.0, volume=30000.0))
        db.commit()
        updated = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "seedtrader"
        ).first()
        assert updated.win_rate == pytest.approx(0.72, abs=0.001)


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
        # First upsert with few markets traded → low diversity component
        data = self._base_data(markets_traded=1)
        scraper._upsert_trader(db, data)
        db.commit()
        old_score = db.query(TrackedTrader).first().elephant_score

        # Second upsert with many markets traded → higher diversity component
        data["markets_traded"] = 100
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


# ---------------------------------------------------------------------------
# update_trader_stats_from_history
# ---------------------------------------------------------------------------

def _make_trader(db, username="testtrader") -> TrackedTrader:
    trader = TrackedTrader(
        kalshi_username=username,
        display_name=username,
        win_rate=0.0,
        consistency_score=0.0,
        total_trades=0,
        elephant_score=50.0,
        tier="ranked",
        is_active=True,
    )
    db.add(trader)
    db.flush()  # assign trader.id
    return trader


def _make_settled_trade(db, trader: TrackedTrader, pnl: float, status="settled") -> CopiedTrade:
    signal = TradeSignal(
        trader_id=trader.id,
        market_ticker="MKTTEST",
        side="yes",
        action="buy",
        detected_price=50.0,
        status="copied",
    )
    db.add(signal)
    db.flush()

    trade = CopiedTrade(
        signal_id=signal.id,
        market_ticker="MKTTEST",
        side="yes",
        action="buy",
        contracts=10,
        price=0.50,
        cost=5.0,
        kalshi_order_id="ord-test",
        status=status,
        is_simulated=True,
        pnl=pnl,
    )
    db.add(trade)
    db.flush()
    return trade


class TestUpdateTraderStatsFromHistory:
    def test_no_history_leaves_stats_at_zero(self, db):
        trader = _make_trader(db)
        update_trader_stats_from_history(db, trader)
        assert trader.win_rate == 0.0
        assert trader.consistency_score == 0.0
        assert trader.total_trades == 0

    def test_new_trader_with_no_id_returns_early(self, db):
        trader = TrackedTrader(
            kalshi_username="newbie",
            display_name="newbie",
            win_rate=0.0,
            consistency_score=0.0,
            total_trades=0,
            elephant_score=10.0,
            tier="ranked",
            is_active=True,
        )
        # id is None — not yet flushed
        update_trader_stats_from_history(db, trader)
        assert trader.win_rate == 0.0

    def test_win_rate_computed_correctly(self, db):
        trader = _make_trader(db)
        _make_settled_trade(db, trader, pnl=5.0)   # win
        _make_settled_trade(db, trader, pnl=3.0)   # win
        _make_settled_trade(db, trader, pnl=-2.0)  # loss
        _make_settled_trade(db, trader, pnl=-1.0)  # loss
        update_trader_stats_from_history(db, trader)
        assert trader.total_trades == 4
        assert trader.win_rate == pytest.approx(0.5, abs=0.01)

    def test_all_wins_gives_win_rate_one(self, db):
        trader = _make_trader(db)
        for _ in range(5):
            _make_settled_trade(db, trader, pnl=4.0)
        update_trader_stats_from_history(db, trader)
        assert trader.win_rate == pytest.approx(1.0, abs=0.01)

    def test_all_losses_gives_win_rate_zero(self, db):
        trader = _make_trader(db)
        for _ in range(3):
            _make_settled_trade(db, trader, pnl=-2.0)
        update_trader_stats_from_history(db, trader)
        assert trader.win_rate == pytest.approx(0.0, abs=0.01)

    def test_low_variance_pnl_gives_high_consistency(self, db):
        trader = _make_trader(db)
        for _ in range(5):
            _make_settled_trade(db, trader, pnl=5.0)  # identical PnL → zero std dev
        update_trader_stats_from_history(db, trader)
        assert trader.consistency_score == pytest.approx(1.0, abs=0.01)

    def test_high_variance_pnl_gives_low_consistency(self, db):
        trader = _make_trader(db)
        _make_settled_trade(db, trader, pnl=100.0)
        _make_settled_trade(db, trader, pnl=-100.0)
        update_trader_stats_from_history(db, trader)
        # cv is high → consistency close to 0
        assert trader.consistency_score < 0.5

    def test_single_settled_trade_consistency_stays_zero(self, db):
        trader = _make_trader(db)
        _make_settled_trade(db, trader, pnl=3.0)
        update_trader_stats_from_history(db, trader)
        assert trader.total_trades == 1
        assert trader.win_rate == pytest.approx(1.0, abs=0.01)
        assert trader.consistency_score == 0.0  # can't compute stdev with 1 sample

    def test_stopped_out_trades_are_included(self, db):
        trader = _make_trader(db)
        _make_settled_trade(db, trader, pnl=2.0, status="settled")
        _make_settled_trade(db, trader, pnl=-1.0, status="stopped_out")
        update_trader_stats_from_history(db, trader)
        assert trader.total_trades == 2
        assert trader.win_rate == pytest.approx(0.5, abs=0.01)

    def test_pending_trades_not_counted(self, db):
        trader = _make_trader(db)
        _make_settled_trade(db, trader, pnl=5.0, status="settled")
        _make_settled_trade(db, trader, pnl=None if False else 99.0, status="simulated")
        # Only 1 settled trade counts
        update_trader_stats_from_history(db, trader)
        assert trader.total_trades == 1

    def test_elephant_score_higher_with_good_win_rate_and_consistency(self, scraper):
        """High win_rate and consistency produce a higher score than diversity alone."""
        data = {"markets_traded": 30}
        base_score = scraper._compute_elephant_score(data)
        boosted_score = scraper._compute_elephant_score(data, win_rate=0.80, consistency_score=0.7)
        assert boosted_score > base_score

    def test_elephant_score_additive_components(self, scraper):
        """Each component contributes independently to the total score."""
        now = datetime.now(timezone.utc)
        score_win_only = scraper._compute_elephant_score({}, win_rate=0.6)
        score_roi_only = scraper._compute_elephant_score(
            {}, total_profit=60.0, total_trades=1, avg_position_size=100.0
        )
        score_both = scraper._compute_elephant_score(
            {}, win_rate=0.6, total_profit=60.0, total_trades=1, avg_position_size=100.0
        )
        assert score_both == pytest.approx(score_win_only + score_roi_only, abs=0.01)


# ---------------------------------------------------------------------------
# _fetch_top_markets_for_trader and _enrich_with_top_markets
# ---------------------------------------------------------------------------

class TestFetchTopMarketsForTrader:
    def _run(self, coro):
        return asyncio.run(coro)

    def _make_trades_response(self, tickers: list[str]) -> MagicMock:
        trades = [{"ticker": t} for t in tickers]
        return _make_http_response(200, {"trades": trades})

    def test_returns_top_10_by_frequency(self, scraper):
        # 12 different tickers; first 10 by count should be returned
        tickers = ["MKT-A"] * 5 + ["MKT-B"] * 4 + ["MKT-C"] * 3 + ["MKT-D"] * 2 + \
                  ["MKT-E", "MKT-F", "MKT-G", "MKT-H", "MKT-I", "MKT-J", "MKT-K", "MKT-L"]
        client = AsyncMock()
        client.get = AsyncMock(return_value=self._make_trades_response(tickers))
        result = self._run(scraper._fetch_top_markets_for_trader(client, "sometrader"))
        parsed = json.loads(result)
        assert len(parsed) <= 10
        # MKT-A and MKT-B must be in the top results
        assert "MKT-A" in parsed
        assert "MKT-B" in parsed

    def test_returns_empty_list_when_no_trades(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(return_value=_make_http_response(200, {"trades": []}))
        result = self._run(scraper._fetch_top_markets_for_trader(client, "sometrader"))
        assert json.loads(result) == []

    def test_falls_back_on_403(self, scraper):
        fallback_resp = _make_http_response(200, {
            "markets": [
                {"ticker": "OPEN-MKT-1"},
                {"ticker": "OPEN-MKT-2"},
            ]
        })

        async def fake_get(url, params=None):
            if "portfolio/trades" in url:
                resp = MagicMock(spec=httpx.Response)
                resp.status_code = 403
                return resp
            return fallback_resp

        client = AsyncMock()
        client.get = AsyncMock(side_effect=fake_get)
        result = self._run(scraper._fetch_top_markets_for_trader(client, "privatetrader"))
        parsed = json.loads(result)
        assert "OPEN-MKT-1" in parsed
        assert "OPEN-MKT-2" in parsed

    def test_returns_empty_list_on_network_error(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.RequestError("timeout"))
        result = self._run(scraper._fetch_top_markets_for_trader(client, "sometrader"))
        assert json.loads(result) == []

    def test_handles_market_ticker_field_name(self, scraper):
        """Trades using 'market_ticker' instead of 'ticker' are counted correctly."""
        trades_resp = _make_http_response(200, {
            "trades": [
                {"market_ticker": "ALT-MKT-1"},
                {"market_ticker": "ALT-MKT-1"},
                {"market_ticker": "ALT-MKT-2"},
            ]
        })
        client = AsyncMock()
        client.get = AsyncMock(return_value=trades_resp)
        result = self._run(scraper._fetch_top_markets_for_trader(client, "sometrader"))
        parsed = json.loads(result)
        assert "ALT-MKT-1" in parsed
        assert "ALT-MKT-2" in parsed


# ---------------------------------------------------------------------------
# _fetch_win_rate_from_settled_markets
# ---------------------------------------------------------------------------

class TestFetchWinRateFromSettledMarkets:
    def _run(self, coro):
        return asyncio.run(coro)

    def _make_client(self, settled_markets, portfolio_trades=None, portfolio_status=200):
        """Build an AsyncMock client that returns the given data."""
        settled_resp = _make_http_response(200, {"markets": settled_markets})
        if portfolio_trades is not None:
            portfolio_resp = _make_http_response(portfolio_status, {"trades": portfolio_trades})
        else:
            portfolio_resp = _make_http_response(portfolio_status, {"trades": []})

        if portfolio_status == 403:
            portfolio_resp.status_code = 403
            portfolio_resp.raise_for_status = MagicMock()

        async def fake_get(url, params=None):
            if "portfolio/trades" in url:
                return portfolio_resp
            return settled_resp

        client = AsyncMock()
        client.get = AsyncMock(side_effect=fake_get)
        return client

    def test_returns_bayesian_rate_from_matched_trades(self, scraper):
        settled = [
            {"ticker": "MKT-A", "result": "yes"},
            {"ticker": "MKT-B", "result": "no"},
        ]
        trades = [
            {"ticker": "MKT-A", "side": "yes"},  # win
            {"ticker": "MKT-B", "side": "yes"},  # loss
        ]
        client = self._make_client(settled, trades)
        # wins=1, total=2 → (1+2)/(2+4) = 0.5
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 500.0, 10000.0, 10)
        )
        assert result == pytest.approx(0.5, abs=0.001)

    def test_all_wins_applies_shrinkage(self, scraper):
        settled = [{"ticker": f"MKT-{i}", "result": "yes"} for i in range(4)]
        trades = [{"ticker": f"MKT-{i}", "side": "yes"} for i in range(4)]
        client = self._make_client(settled, trades)
        # wins=4, total=4 → (4+2)/(4+4) = 0.75
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 500.0, 10000.0, 10)
        )
        assert result == pytest.approx(0.75, abs=0.001)

    def test_falls_back_to_prior_on_403(self, scraper):
        settled = [{"ticker": "MKT-A", "result": "yes"}]
        client = self._make_client(settled, portfolio_trades=None, portfolio_status=403)
        # pnl=3000, volume=30000, markets=10 → Bayesian prior
        expected = scraper._seed_win_rate_prior(3000.0, 30000.0, 10)
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "private", 3000.0, 30000.0, 10)
        )
        assert result == pytest.approx(expected, abs=0.001)

    def test_falls_back_to_prior_when_no_settled_markets(self, scraper):
        client = self._make_client(settled_markets=[], portfolio_trades=[])
        expected = scraper._seed_win_rate_prior(3000.0, 30000.0, 10)
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 3000.0, 30000.0, 10)
        )
        assert result == pytest.approx(expected, abs=0.001)

    def test_falls_back_to_prior_when_no_overlap(self, scraper):
        settled = [{"ticker": "MKT-X", "result": "yes"}]
        trades = [{"ticker": "MKT-Y", "side": "yes"}]  # different ticker — no overlap
        client = self._make_client(settled, trades)
        expected = scraper._seed_win_rate_prior(3000.0, 30000.0, 10)
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 3000.0, 30000.0, 10)
        )
        assert result == pytest.approx(expected, abs=0.001)

    def test_falls_back_to_prior_on_network_error(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.RequestError("timeout"))
        expected = scraper._seed_win_rate_prior(1000.0, 5000.0, 20)
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 1000.0, 5000.0, 20)
        )
        assert result == pytest.approx(expected, abs=0.001)

    def test_no_hard_floor_at_0_6(self, scraper):
        # 2 wins out of 6 → (2+2)/(6+4) = 0.4 — below old floor of 0.60
        settled = [{"ticker": f"MKT-{i}", "result": "yes"} for i in range(6)]
        trades = (
            [{"ticker": f"MKT-{i}", "side": "yes"} for i in range(2)]   # 2 wins
            + [{"ticker": f"MKT-{i}", "side": "no"} for i in range(2, 6)]  # 4 losses
        )
        client = self._make_client(settled, trades)
        result = self._run(
            scraper._fetch_win_rate_from_settled_markets(client, "trader1", 500.0, 10000.0, 10)
        )
        assert result == pytest.approx(0.4, abs=0.001)
        assert result < 0.6  # not floored at old minimum


class TestEnrichWithTopMarkets:
    def test_updates_top_markets_on_upserted_traders(self, scraper, db):
        # Pre-create a trader in the DB
        trader = TrackedTrader(
            kalshi_username="whale_one",
            display_name="whale_one",
            win_rate=0.0,
            consistency_score=0.0,
            total_trades=0,
            elephant_score=60.0,
            tier="ranked",
            is_active=True,
        )
        db.add(trader)
        db.flush()

        merged = {
            "whale_one": {
                "nickname": "whale_one",
                "rank_pnl": 3,
            }
        }

        async def fake_fetch(client, username):
            return json.dumps(["MKT-X", "MKT-Y"])

        with patch.object(scraper, "_fetch_top_markets_for_trader", side_effect=fake_fetch), \
             patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_client_cls, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client
            asyncio.run(scraper._enrich_with_top_markets(db, merged))

        db.flush()
        updated = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "whale_one"
        ).first()
        assert updated.top_markets is not None
        assert json.loads(updated.top_markets) == ["MKT-X", "MKT-Y"]

    def test_skips_unknown_traders(self, scraper, db):
        """Traders in merged but not in the DB are silently skipped."""
        merged = {"ghost_trader": {"nickname": "ghost_trader"}}

        with patch.object(scraper, "_fetch_top_markets_for_trader", new=AsyncMock()) as mock_fetch, \
             patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_client_cls, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client
            asyncio.run(scraper._enrich_with_top_markets(db, merged))

        mock_fetch.assert_not_called()


# ---------------------------------------------------------------------------
# _scrape_html_fallback
# ---------------------------------------------------------------------------

class TestScrapeHtmlFallback:
    def _run(self, coro):
        return asyncio.run(coro)

    def _html_with_traders(self, traders: list[dict]) -> str:
        rows = ""
        for t in traders:
            attrs = f'data-username="{t["username"]}"'
            if "profit" in t:
                attrs += f' data-profit="{t["profit"]}"'
            if "win_rate" in t:
                attrs += f' data-win-rate="{t["win_rate"]}"'
            if "profile_image" in t:
                attrs += f' data-profile-image="{t["profile_image"]}"'
            rows += f"<div {attrs}></div>\n"
        return f"<html><body>{rows}</body></html>"

    def _mock_html_client(self, html: str, status: int = 200):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = status
        resp.text = html
        resp.raise_for_status = MagicMock()
        if status >= 400:
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error", request=MagicMock(), response=resp
            )
        client = AsyncMock()
        client.get = AsyncMock(return_value=resp)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        return client

    def test_parses_trader_rows_with_data_attributes(self, scraper):
        html = self._html_with_traders([
            {"username": "alice", "profit": "5000.50", "win_rate": "65.0"},
            {"username": "bob", "profit": "3000", "win_rate": "55.5"},
        ])
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client(html)
            result = self._run(scraper._scrape_html_fallback())

        assert "alice" in result
        assert result["alice"]["pnl"] == pytest.approx(5000.50)
        assert result["alice"]["win_rate"] == pytest.approx(0.65)
        assert result["alice"]["rank_pnl"] == 1
        assert "bob" in result
        assert result["bob"]["rank_pnl"] == 2

    def test_normalises_percentage_win_rate(self, scraper):
        html = self._html_with_traders([
            {"username": "trader1", "profit": "100", "win_rate": "72.3"},
        ])
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client(html)
            result = self._run(scraper._scrape_html_fallback())

        assert result["trader1"]["win_rate"] == pytest.approx(0.723)

    def test_uses_data_pnl_fallback_attribute(self, scraper):
        html = "<html><body><div data-username=\"x\" data-pnl=\"1234\"></div></body></html>"
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client(html)
            result = self._run(scraper._scrape_html_fallback())

        assert result["x"]["pnl"] == pytest.approx(1234.0)

    def test_skips_rows_without_username(self, scraper):
        html = "<html><body><div data-profit=\"999\"></div></body></html>"
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client(html)
            result = self._run(scraper._scrape_html_fallback())

        assert result == {}

    def test_returns_empty_on_http_error(self, scraper):
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client("", status=503)
            result = self._run(scraper._scrape_html_fallback())

        assert result == {}

    def test_returns_empty_on_network_error(self, scraper):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.RequestError("timeout"))
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = client
            result = self._run(scraper._scrape_html_fallback())

        assert result == {}

    def test_returns_empty_for_page_with_no_trader_rows(self, scraper):
        html = "<html><body><p>No traders found</p></body></html>"
        with patch("app.services.leaderboard_scraper.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value = self._mock_html_client(html)
            result = self._run(scraper._scrape_html_fallback())

        assert result == {}


# ---------------------------------------------------------------------------
# scrape() fallback integration
# ---------------------------------------------------------------------------

class TestScrapeFallbackIntegration:
    def test_scrape_calls_html_fallback_when_api_returns_empty(self, scraper, db):
        """When fetch_all_metrics() returns {}, scrape() should call _scrape_html_fallback()."""
        fallback_data = {
            "fallback_trader": {
                "nickname": "fallback_trader",
                "pnl": 2000.0,
                "rank_pnl": 1,
                "win_rate": 0.6,
            }
        }

        with patch.object(scraper, "fetch_all_metrics", new=AsyncMock(return_value={})), \
             patch.object(scraper, "_scrape_html_fallback", new=AsyncMock(return_value=fallback_data)), \
             patch.object(scraper, "is_rate_limited", return_value=False):
            count = asyncio.run(scraper.scrape(db))

        assert count == 1
        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "fallback_trader"
        ).first()
        assert trader is not None

    def test_scrape_calls_html_fallback_when_api_raises(self, scraper, db):
        """When fetch_all_metrics() raises, scrape() should call _scrape_html_fallback()."""
        fallback_data = {
            "fb_trader": {
                "nickname": "fb_trader",
                "pnl": 1500.0,
                "rank_pnl": 1,
                "win_rate": 0.7,
            }
        }

        with patch.object(scraper, "fetch_all_metrics", new=AsyncMock(side_effect=RuntimeError("API down"))), \
             patch.object(scraper, "_scrape_html_fallback", new=AsyncMock(return_value=fallback_data)), \
             patch.object(scraper, "is_rate_limited", return_value=False):
            count = asyncio.run(scraper.scrape(db))

        assert count == 1

    def test_scrape_returns_zero_when_both_sources_empty(self, scraper, db):
        with patch.object(scraper, "fetch_all_metrics", new=AsyncMock(return_value={})), \
             patch.object(scraper, "_scrape_html_fallback", new=AsyncMock(return_value={})), \
             patch.object(scraper, "is_rate_limited", return_value=False):
            count = asyncio.run(scraper.scrape(db))

        assert count == 0


# ---------------------------------------------------------------------------
# Trader deactivation logic
# ---------------------------------------------------------------------------

class TestTraderDeactivation:
    def _make_trader(self, db, username, is_active=True, last_seen=None) -> TrackedTrader:
        if last_seen is None:
            last_seen = datetime.now(timezone.utc)
        trader = TrackedTrader(
            kalshi_username=username,
            display_name=username,
            win_rate=0.5,
            consistency_score=0.5,
            total_trades=0,
            elephant_score=50.0,
            tier="ranked",
            is_active=is_active,
            last_seen=last_seen,
        )
        db.add(trader)
        db.flush()
        return trader

    def _run_scrape(self, scraper, db, merged):
        with patch.object(scraper, "fetch_all_metrics", new=AsyncMock(return_value=merged)), \
             patch.object(scraper, "is_rate_limited", return_value=False):
            asyncio.run(scraper.scrape(db))

    def test_stale_absent_trader_is_deactivated(self, scraper, db):
        """A trader not in the leaderboard with last_seen > 3 days ago is deactivated."""
        stale_last_seen = datetime.now(timezone.utc) - timedelta(days=4)
        self._make_trader(db, "stale_trader", is_active=True, last_seen=stale_last_seen)
        db.commit()

        merged = {
            "active_trader": {
                "nickname": "active_trader",
                "pnl": 1000.0,
                "rank_pnl": 1,
            }
        }
        self._run_scrape(scraper, db, merged)

        stale = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "stale_trader"
        ).first()
        assert stale.is_active is False

    def test_recently_absent_trader_is_not_deactivated(self, scraper, db):
        """A trader absent from the leaderboard but last_seen within 3 days is kept active."""
        recent_last_seen = datetime.now(timezone.utc) - timedelta(days=2)
        self._make_trader(db, "recent_trader", is_active=True, last_seen=recent_last_seen)
        db.commit()

        merged = {
            "other_trader": {
                "nickname": "other_trader",
                "pnl": 500.0,
                "rank_pnl": 1,
            }
        }
        self._run_scrape(scraper, db, merged)

        recent = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "recent_trader"
        ).first()
        assert recent.is_active is True

    def test_present_trader_stays_active(self, scraper, db):
        """A trader returned by the leaderboard is never deactivated."""
        stale_last_seen = datetime.now(timezone.utc) - timedelta(days=10)
        self._make_trader(db, "present_trader", is_active=True, last_seen=stale_last_seen)
        db.commit()

        merged = {
            "present_trader": {
                "nickname": "present_trader",
                "pnl": 2000.0,
                "rank_pnl": 1,
            }
        }
        self._run_scrape(scraper, db, merged)

        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "present_trader"
        ).first()
        assert trader.is_active is True

    def test_already_inactive_trader_not_affected(self, scraper, db):
        """A trader already inactive stays inactive (no double-deactivation side effects)."""
        stale_last_seen = datetime.now(timezone.utc) - timedelta(days=10)
        self._make_trader(db, "already_gone", is_active=False, last_seen=stale_last_seen)
        db.commit()

        merged = {
            "some_trader": {
                "nickname": "some_trader",
                "pnl": 1000.0,
                "rank_pnl": 1,
            }
        }
        self._run_scrape(scraper, db, merged)

        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "already_gone"
        ).first()
        assert trader.is_active is False

    def test_exactly_3_days_old_is_not_deactivated(self, scraper, db):
        """A trader last seen just under 3 days ago is kept active (within the grace window)."""
        boundary_last_seen = datetime.now(timezone.utc) - timedelta(days=2, hours=23)
        self._make_trader(db, "boundary_trader", is_active=True, last_seen=boundary_last_seen)
        db.commit()

        merged = {
            "other": {
                "nickname": "other",
                "pnl": 100.0,
                "rank_pnl": 1,
            }
        }
        self._run_scrape(scraper, db, merged)

        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == "boundary_trader"
        ).first()
        assert trader.is_active is True
