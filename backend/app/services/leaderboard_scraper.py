"""Kalshi leaderboard scraper.

Fetches trader rankings from the Kalshi social leaderboard API at
https://api.elections.kalshi.com/v1/social/leaderboard

Stores results in the TrackedTrader table. Rate-limited to once per hour.
"""

import asyncio
import json
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import TrackedTrader

logger = logging.getLogger(__name__)

LEADERBOARD_API = "https://api.elections.kalshi.com/v1/social/leaderboard"
SCRAPE_MIN_INTERVAL_HOURS = 1

# Metrics and time periods to fetch
METRICS = ["projected_pnl", "volume", "num_markets_traded"]
TIME_PERIOD = "weekly"  # weekly gives the most actionable signal
LIMIT = 50  # top 50 per metric

# In-process rate-limit cache
_last_scrape_time: Optional[datetime] = None
_last_scrape_count: int = 0


class LeaderboardScraper:
    """Fetches Kalshi leaderboard data via their JSON API and persists results."""

    def is_rate_limited(self) -> bool:
        global _last_scrape_time
        if _last_scrape_time is None:
            return False
        return datetime.utcnow() - _last_scrape_time < timedelta(hours=SCRAPE_MIN_INTERVAL_HOURS)

    # ------------------------------------------------------------------ #
    # API fetching                                                        #
    # ------------------------------------------------------------------ #

    async def _fetch_metric(
        self, client: httpx.AsyncClient, metric: str, time_period: str, limit: int
    ) -> list[dict]:
        """Fetch one leaderboard metric and return the rank_list."""
        params = {
            "metric_name": metric,
            "limit": limit,
            "time_period": time_period,
        }
        try:
            resp = await client.get(LEADERBOARD_API, params=params)
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("rank_list", [])
            logger.info(
                "Fetched %d entries for metric=%s period=%s",
                len(entries), metric, time_period,
            )
            return entries
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.error("Failed to fetch metric %s: %s", metric, exc)
            return []

    async def fetch_all_metrics(self) -> dict[str, dict]:
        """Fetch all metrics and merge into a dict keyed by nickname.

        Returns {nickname: {pnl, volume, markets_traded, rank_pnl, ...}}.
        """
        merged: dict[str, dict] = {}

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            for metric in METRICS:
                entries = await self._fetch_metric(client, metric, TIME_PERIOD, LIMIT)
                for entry in entries:
                    if entry.get("is_anonymous"):
                        continue
                    nick = entry.get("nickname", "").strip()
                    if not nick:
                        continue

                    if nick not in merged:
                        merged[nick] = {
                            "nickname": nick,
                            "profile_image": entry.get("profile_image_path", ""),
                        }

                    value = entry.get("value", 0)
                    rank = entry.get("rank", 0)

                    if metric == "projected_pnl":
                        merged[nick]["pnl"] = value
                        merged[nick]["rank_pnl"] = rank
                    elif metric == "volume":
                        merged[nick]["volume"] = value
                        merged[nick]["rank_volume"] = rank
                    elif metric == "num_markets_traded":
                        merged[nick]["markets_traded"] = value
                        merged[nick]["rank_markets"] = rank

                await asyncio.sleep(0.3)  # polite delay between requests

        return merged

    # ------------------------------------------------------------------ #
    # Scoring                                                              #
    # ------------------------------------------------------------------ #

    def _compute_elephant_score(self, data: dict) -> float:
        """Composite score (0-100) from available API data.

        Weights:
          40%  PnL rank score      (top 1 = 1.0, rank 200 = 0.0)
          25%  Volume rank score   (same scale)
          20%  Market diversity    (log-scaled, 100 markets = 1.0)
          15%  Cross-metric bonus  (appears in multiple leaderboards)
        """
        rank_pnl = data.get("rank_pnl", 999)
        rank_volume = data.get("rank_volume", 999)
        markets = data.get("markets_traded", 0)

        # Rank score: rank 1 → 1.0, rank 200+ → 0.0
        pnl_score = max(0.0, 1.0 - (rank_pnl - 1) / 200)
        vol_score = max(0.0, 1.0 - (rank_volume - 1) / 200)

        # Market diversity: log-scaled, 100 markets → 1.0
        diversity_score = min(1.0, math.log1p(markets) / math.log1p(100)) if markets > 0 else 0.0

        # Cross-metric bonus: appears in multiple leaderboards
        appearances = sum(1 for k in ("rank_pnl", "rank_volume", "rank_markets") if k in data)
        cross_score = appearances / 3.0

        raw = (
            0.40 * pnl_score
            + 0.25 * vol_score
            + 0.20 * diversity_score
            + 0.15 * cross_score
        )
        return round(raw * 100.0, 2)

    def _assign_tier(self, rank: int) -> str:
        if rank <= 10:
            return "top_001"
        if rank <= 25:
            return "top_01"
        if rank <= 50:
            return "top_1"
        if rank <= 100:
            return "top_25"
        return "ranked"

    # ------------------------------------------------------------------ #
    # Database persistence                                                 #
    # ------------------------------------------------------------------ #

    def _upsert_trader(self, db: Session, data: dict) -> TrackedTrader:
        """Insert or update a TrackedTrader from API data."""
        username = data["nickname"].lower()
        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == username
        ).first()

        elephant_score = self._compute_elephant_score(data)
        best_rank = min(
            data.get("rank_pnl", 999),
            data.get("rank_volume", 999),
            data.get("rank_markets", 999),
        )
        tier = self._assign_tier(best_rank)

        pnl = data.get("pnl", 0.0)
        volume = data.get("volume", 0.0)
        markets = data.get("markets_traded", 0)

        if trader is None:
            trader = TrackedTrader(
                kalshi_username=username,
                display_name=data["nickname"],  # preserve original casing
                total_profit=pnl,
                win_rate=0.0,  # not available from leaderboard API
                total_trades=0,
                avg_position_size=volume / max(markets, 1) if volume and markets else 0.0,
                market_diversity=markets,
                consistency_score=0.0,
                elephant_score=elephant_score,
                tier=tier,
                is_active=True,
                last_seen=datetime.now(timezone.utc),
            )
            db.add(trader)
            logger.debug("New trader: %s score=%.1f tier=%s", username, elephant_score, tier)
        else:
            trader.display_name = data["nickname"]
            trader.total_profit = pnl if pnl else trader.total_profit
            trader.avg_position_size = volume / max(markets, 1) if volume and markets else trader.avg_position_size
            trader.market_diversity = markets if markets else trader.market_diversity
            trader.elephant_score = elephant_score
            trader.tier = tier
            trader.is_active = True
            trader.last_seen = datetime.now(timezone.utc)
            logger.debug("Updated trader: %s score=%.1f tier=%s", username, elephant_score, tier)

        return trader

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #

    async def scrape(self, db: Session) -> int:
        """Fetch leaderboard data and persist to DB.

        Respects rate limiting. Returns number of traders upserted.
        """
        global _last_scrape_time, _last_scrape_count

        if self.is_rate_limited():
            elapsed_min = int((datetime.utcnow() - _last_scrape_time).total_seconds() // 60)
            logger.info(
                "Rate-limited: last scrape %d min ago. Cached count: %d.",
                elapsed_min, _last_scrape_count,
            )
            return _last_scrape_count

        logger.info("Starting Kalshi leaderboard API fetch")
        started_at = datetime.utcnow()

        merged = await self.fetch_all_metrics()

        if not merged:
            logger.warning("Leaderboard API returned 0 traders")
            _last_scrape_time = datetime.utcnow()
            _last_scrape_count = 0
            return 0

        count = 0
        for data in merged.values():
            self._upsert_trader(db, data)
            count += 1

        db.commit()

        elapsed_s = (datetime.utcnow() - started_at).total_seconds()
        logger.info("Leaderboard fetch complete: %d traders upserted in %.1fs", count, elapsed_s)

        _last_scrape_time = datetime.utcnow()
        _last_scrape_count = count
        return count


# Module-level singleton
scraper = LeaderboardScraper()


async def run_scrape() -> int:
    """Scheduler / CLI entry point — manages its own DB session."""
    db = SessionLocal()
    try:
        return await scraper.scrape(db)
    finally:
        db.close()
