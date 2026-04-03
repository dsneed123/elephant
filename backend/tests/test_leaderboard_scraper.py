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
