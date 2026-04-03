"""Kalshi leaderboard scraper.

Fetches trader rankings from the Kalshi social leaderboard API at
https://api.elections.kalshi.com/v1/social/leaderboard

Stores results in the TrackedTrader table. Rate-limited to once per hour.
"""

import asyncio
import json
import logging
import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import CopiedTrade, TradeSignal, TrackedTrader

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

    def _compute_elephant_score(
        self, data: dict, win_rate: float = 0.0, consistency_score: float = 0.0
    ) -> float:
        """Composite score (0-100) from available API data.

        Base weights (from leaderboard API):
          40%  PnL rank score      (top 1 = 1.0, rank 200 = 0.0)
          25%  Volume rank score   (same scale)
          20%  Market diversity    (log-scaled, 100 markets = 1.0)
          15%  Cross-metric bonus  (appears in multiple leaderboards)

        When real trade history is available (win_rate or consistency_score > 0),
        a quality multiplier of up to +25% is applied based on those values.
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

        # Apply a quality multiplier when real trade history is available.
        # win_edge: 0 at win_rate=0.5 (breakeven), 1.0 at win_rate=1.0
        # Multiplier caps at 1.25 (+25% for a perfect win rate and consistency)
        if win_rate > 0.0 or consistency_score > 0.0:
            win_edge = max(0.0, (win_rate - 0.5) * 2.0)
            quality = 0.60 * win_edge + 0.40 * consistency_score
            raw = raw * (1.0 + 0.25 * quality)

        return round(min(1.0, raw) * 100.0, 2)

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
            trader = self._upsert_trader(db, data)
            update_trader_stats_from_history(db, trader)
            trader.elephant_score = self._compute_elephant_score(
                data, trader.win_rate, trader.consistency_score
            )
            count += 1

        db.commit()

        elapsed_s = (datetime.utcnow() - started_at).total_seconds()
        logger.info("Leaderboard fetch complete: %d traders upserted in %.1fs", count, elapsed_s)

        _last_scrape_time = datetime.utcnow()
        _last_scrape_count = count
        return count


def update_trader_stats_from_history(db: Session, trader: TrackedTrader) -> None:
    """Compute win_rate and consistency_score from settled CopiedTrade history.

    Queries CopiedTrade records whose TradeSignal.trader_id matches this trader,
    then updates trader.win_rate, trader.consistency_score, and trader.total_trades
    in place.  No commit — the caller is responsible for flushing/committing.

    win_rate        = settled_wins / total_settled_trades
    consistency_score = 1 / (1 + cv), where cv = stdev(pnl) / mean(|pnl|).
                      Lower PnL variance → higher consistency (0–1).
    """
    if trader.id is None:
        # New trader just added to the session but not yet flushed; no history exists.
        return

    settled_trades = (
        db.query(CopiedTrade)
        .join(TradeSignal, CopiedTrade.signal_id == TradeSignal.id)
        .filter(
            TradeSignal.trader_id == trader.id,
            CopiedTrade.status.in_(["settled", "stopped_out"]),
            CopiedTrade.pnl.isnot(None),
        )
        .all()
    )

    total_settled = len(settled_trades)
    trader.total_trades = total_settled

    if total_settled == 0:
        return

    pnl_values = [t.pnl for t in settled_trades]
    settled_wins = sum(1 for p in pnl_values if p > 0)
    trader.win_rate = settled_wins / total_settled

    if total_settled >= 2:
        std = statistics.stdev(pnl_values)
        mean_abs = sum(abs(p) for p in pnl_values) / total_settled
        if mean_abs == 0.0:
            trader.consistency_score = 1.0
        else:
            cv = std / mean_abs
            trader.consistency_score = round(1.0 / (1.0 + cv), 4)
    # else: single trade — leave consistency_score at its current value (0.0 default)


# Module-level singleton
scraper = LeaderboardScraper()


async def run_scrape() -> int:
    """Scheduler / CLI entry point — manages its own DB session."""
    db = SessionLocal()
    try:
        return await scraper.scrape(db)
    finally:
        db.close()
