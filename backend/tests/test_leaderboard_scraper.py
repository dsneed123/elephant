"""Tests for LeaderboardScraper._compute_elephant_score."""

import math
from datetime import datetime, timedelta

import pytest

from app.services.leaderboard_scraper import LeaderboardScraper


@pytest.fixture
def scraper():
    return LeaderboardScraper()


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
        last_active = datetime.utcnow() - timedelta(days=3)
        data = self._data(win_rate=1.0, consistency_score=1.0, market_diversity=20,
                          total_profit=10_000.0, last_active=last_active)
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(100.0, abs=0.01)

    def test_recency_exactly_7_days(self, scraper):
        last_active = datetime.utcnow() - timedelta(days=7)
        data = self._data(last_active=last_active)
        score_7 = scraper._compute_elephant_score(data)
        score_no_date = scraper._compute_elephant_score(self._data())
        # Both should have recency=1.0
        assert score_7 == pytest.approx(score_no_date, abs=0.1)

    def test_recency_at_90_days_is_zero(self, scraper):
        last_active = datetime.utcnow() - timedelta(days=90)
        data = self._data(last_active=last_active)
        # recency=0, all other zeros → raw=0 → score=0
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_recency_beyond_90_days_clamped(self, scraper):
        last_active = datetime.utcnow() - timedelta(days=200)
        data = self._data(last_active=last_active)
        score = scraper._compute_elephant_score(data)
        assert score == pytest.approx(0.0, abs=0.01)

    def test_recency_midpoint_linear(self, scraper):
        # At day 48.5 (midpoint of 7–90), recency ≈ 0.5
        days_mid = 7 + (90 - 7) / 2  # = 48.5
        last_active = datetime.utcnow() - timedelta(days=days_mid)
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
