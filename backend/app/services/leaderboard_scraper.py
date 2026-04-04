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
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import CopiedTrade, TradeSignal, TrackedTrader

logger = logging.getLogger(__name__)

LEADERBOARD_API = "https://api.elections.kalshi.com/v1/social/leaderboard"
KALSHI_LEADERBOARD_HTML = "https://kalshi.com/social/leaderboard"
KALSHI_TRADE_API = "https://trading-api.kalshi.com/trade-api/v2"
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

    async def _scrape_html_fallback(self) -> dict[str, dict]:
        """Fallback: parse the public Kalshi leaderboard HTML page.

        Fetches https://kalshi.com/social/leaderboard and looks for trader
        rows with data-username, data-profit (or data-pnl), and data-win-rate
        attributes.  Returns the same merged dict format as fetch_all_metrics().
        """
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
                resp = await client.get(KALSHI_LEADERBOARD_HTML)
                resp.raise_for_status()
                html = resp.text
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.error("HTML fallback fetch failed: %s", exc)
            return {}

        soup = BeautifulSoup(html, "html.parser")
        merged: dict[str, dict] = {}

        # Look for elements carrying a data-username attribute; each represents
        # one trader row.  The page may also use data-pnl or data-profit for the
        # PnL value, and data-win-rate or data-winrate for the win percentage.
        rows = soup.find_all(attrs={"data-username": True})

        for rank, row in enumerate(rows, start=1):
            username = row.get("data-username", "").strip()
            if not username:
                continue

            pnl_raw = row.get("data-profit") or row.get("data-pnl") or "0"
            win_rate_raw = row.get("data-win-rate") or row.get("data-winrate") or "0"

            try:
                pnl = float(str(pnl_raw).replace(",", "").replace("$", ""))
            except (ValueError, TypeError):
                pnl = 0.0

            try:
                win_rate = float(str(win_rate_raw).replace("%", ""))
                # Normalise: values like "65.5" → 0.655
                if win_rate > 1.0:
                    win_rate = win_rate / 100.0
            except (ValueError, TypeError):
                win_rate = 0.0

            merged[username] = {
                "nickname": username,
                "profile_image": row.get("data-profile-image", ""),
                "pnl": pnl,
                "rank_pnl": rank,
                "win_rate": win_rate,
            }

        logger.info("HTML fallback: parsed %d traders from leaderboard page", len(merged))
        return merged

    # ------------------------------------------------------------------ #
    # Scoring                                                              #
    # ------------------------------------------------------------------ #

    def _compute_elephant_score(
        self,
        data: dict,
        win_rate: float = 0.0,
        consistency_score: float = 0.0,
        total_profit: float = 0.0,
        total_trades: int = 0,
        avg_position_size: float = 0.0,
        last_seen: Optional[datetime] = None,
    ) -> float:
        """Composite score (0-100) using spec-defined weights.

        Components:
          30%  win_rate         (normalized 0-1)
          25%  consistency      (consistency_score, 0-1)
          20%  ROI              (total_profit / (total_trades * avg_position_size), capped 0-1)
          15%  market_diversity (log-scaled, 100 markets = 1.0)
          10%  recency          (exponential decay from last_seen, 30-day half-life)
        """
        # win_rate component: directly 0-1
        win_component = max(0.0, min(1.0, win_rate))

        # consistency component: directly 0-1
        consistency_component = max(0.0, min(1.0, consistency_score))

        # ROI component: total_profit / (total_trades * avg_position_size), capped to [0, 1]
        if total_trades > 0 and avg_position_size > 0.0:
            roi_raw = total_profit / (total_trades * avg_position_size)
            roi_component = max(0.0, min(1.0, roi_raw))
        else:
            roi_component = 0.0

        # market_diversity component: log-scaled, 100 markets → 1.0
        markets = data.get("markets_traded", 0)
        diversity_component = min(1.0, math.log1p(markets) / math.log1p(100)) if markets > 0 else 0.0

        # recency component: exponential decay, 30-day half-life
        if last_seen is not None:
            now = datetime.now(timezone.utc)
            ls = last_seen if last_seen.tzinfo is not None else last_seen.replace(tzinfo=timezone.utc)
            days_ago = max(0.0, (now - ls).total_seconds() / 86400.0)
            recency_component = math.exp(-days_ago / 30.0)
        else:
            recency_component = 0.0

        raw = (
            0.30 * win_component
            + 0.25 * consistency_component
            + 0.20 * roi_component
            + 0.15 * diversity_component
            + 0.10 * recency_component
        )

        return round(min(1.0, raw) * 100.0, 2)

    @staticmethod
    def _seed_win_rate_prior(pnl: float, volume: float, markets_traded: int = 0) -> float:
        """Bayesian shrinkage estimator: (wins + 2) / (total + 4).

        Uses projected_pnl relative to volume as a proxy for positive-PnL markets
        to estimate the number of winning trades.  No hard floor (a 55% win rate
        is valid) and no hard cap.

        Returns 0.0 when pnl is not positive.
        """
        if pnl <= 0:
            return 0.0
        total = max(markets_traded, 0)
        if total == 0:
            # No market-count data: pure Bayesian prior with no observations.
            return (0 + 2) / (0 + 4)  # 0.5
        if volume > 0:
            win_fraction = min(1.0, 0.5 + pnl / (2.0 * volume))
        else:
            win_fraction = 1.0  # positive pnl, zero volume → treat as all wins
        wins = round(total * win_fraction)
        return (wins + 2) / (total + 4)

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
            # Seed win_rate and consistency from leaderboard data as a prior.
            # These are replaced once real CopiedTrade history accumulates.
            seeded_win_rate = self._seed_win_rate_prior(pnl, volume, markets)
            seeded_consistency = 0.5 if seeded_win_rate > 0.0 else 0.0
            trader = TrackedTrader(
                kalshi_username=username,
                display_name=data["nickname"],  # preserve original casing
                total_profit=pnl,
                win_rate=seeded_win_rate,
                total_trades=0,
                avg_position_size=volume / max(markets, 1) if volume and markets else 0.0,
                market_diversity=markets,
                consistency_score=seeded_consistency,
                has_trade_history=False,
                elephant_score=elephant_score,
                tier=tier,
                is_active=True,
                last_seen=datetime.now(timezone.utc),
            )
            db.add(trader)
            logger.debug(
                "New trader: %s score=%.1f tier=%s seeded_win_rate=%.3f",
                username, elephant_score, tier, seeded_win_rate,
            )
        else:
            trader.display_name = data["nickname"]
            trader.total_profit = pnl if pnl else trader.total_profit
            trader.avg_position_size = volume / max(markets, 1) if volume and markets else trader.avg_position_size
            trader.market_diversity = markets if markets else trader.market_diversity
            # Refresh the leaderboard-derived prior only while real history is absent.
            if not trader.has_trade_history:
                seeded_win_rate = self._seed_win_rate_prior(pnl, volume, markets)
                if seeded_win_rate > 0.0:
                    trader.win_rate = seeded_win_rate
                    trader.consistency_score = 0.5
            trader.elephant_score = elephant_score
            trader.tier = tier
            trader.is_active = True
            trader.last_seen = datetime.now(timezone.utc)
            logger.debug("Updated trader: %s score=%.1f tier=%s", username, elephant_score, tier)

        return trader

    # ------------------------------------------------------------------ #
    # Top-markets enrichment                                              #
    # ------------------------------------------------------------------ #

    async def _fallback_top_markets(self, client: httpx.AsyncClient) -> str:
        """Fallback when trade history is private: return tickers from open markets."""
        url = f"{KALSHI_TRADE_API}/markets"
        try:
            resp = await client.get(url, params={"status": "open", "limit": 10})
            resp.raise_for_status()
            data = resp.json()
            tickers = [
                m.get("ticker", "")
                for m in data.get("markets", [])
                if m.get("ticker")
            ][:10]
            return json.dumps(tickers)
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("Fallback markets fetch failed: %s", exc)
            return json.dumps([])

    async def _fetch_top_markets_for_trader(
        self, client: httpx.AsyncClient, username: str
    ) -> str:
        """Fetch a trader's most-traded market tickers; returns a JSON list of up to 10.

        Tries GET /trade-api/v2/portfolio/trades?user_id={username}.
        Falls back to open markets if the endpoint returns 403 (private profile).
        """
        url = f"{KALSHI_TRADE_API}/portfolio/trades"
        try:
            resp = await client.get(url, params={"user_id": username, "limit": 100})
            if resp.status_code == 403:
                logger.debug("Trade history private for %s; using fallback markets", username)
                return await self._fallback_top_markets(client)
            resp.raise_for_status()
            trades = resp.json().get("trades", [])
            ticker_counts: dict[str, int] = {}
            for trade in trades:
                ticker = trade.get("ticker") or trade.get("market_ticker", "")
                if ticker:
                    ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            top_tickers = sorted(ticker_counts, key=lambda t: ticker_counts[t], reverse=True)[:10]
            logger.debug("Top markets for %s: %s", username, top_tickers)
            return json.dumps(top_tickers)
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("Could not fetch trade history for %s: %s", username, exc)
            return json.dumps([])

    async def _enrich_with_top_markets(
        self, db: Session, merged: dict[str, dict]
    ) -> None:
        """Populate top_markets for every upserted trader from their public trade history."""
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            for nick in merged:
                username = nick.lower()
                trader = db.query(TrackedTrader).filter(
                    TrackedTrader.kalshi_username == username
                ).first()
                if trader is None:
                    continue
                trader.top_markets = await self._fetch_top_markets_for_trader(
                    client, username
                )
                await asyncio.sleep(0.3)  # polite delay

    # ------------------------------------------------------------------ #
    # Settled-market win-rate enrichment                                   #
    # ------------------------------------------------------------------ #

    async def _fetch_win_rate_from_settled_markets(
        self,
        client: httpx.AsyncClient,
        username: str,
        pnl: float,
        volume: float,
        markets_traded: int,
    ) -> float:
        """Estimate win rate from public settled-market data.

        Fetches recently settled markets via GET /trade-api/v2/markets?status=settled,
        then looks up the trader's portfolio to match trades against settled
        outcomes.  Falls back to the Bayesian shrinkage prior when the portfolio
        endpoint is unavailable (private profile) or returns no overlapping data.
        """
        # Step 1: fetch recently settled markets
        try:
            resp = await client.get(
                f"{KALSHI_TRADE_API}/markets",
                params={"status": "settled", "limit": 50},
            )
            resp.raise_for_status()
            settled_markets = resp.json().get("markets", [])
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.debug("Settled markets fetch failed: %s", exc)
            return self._seed_win_rate_prior(pnl, volume, markets_traded)

        if not settled_markets:
            return self._seed_win_rate_prior(pnl, volume, markets_traded)

        # Step 2: fetch the trader's portfolio trades
        try:
            trades_resp = await client.get(
                f"{KALSHI_TRADE_API}/portfolio/trades",
                params={"user_id": username, "limit": 200},
            )
            if trades_resp.status_code == 403:
                logger.debug("Portfolio private for %s; using Bayesian fallback", username)
                return self._seed_win_rate_prior(pnl, volume, markets_traded)
            trades_resp.raise_for_status()
            portfolio_trades = trades_resp.json().get("trades", [])
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.debug("Portfolio fetch failed for %s: %s", username, exc)
            return self._seed_win_rate_prior(pnl, volume, markets_traded)

        if not portfolio_trades:
            return self._seed_win_rate_prior(pnl, volume, markets_traded)

        # Step 3: match portfolio trades to settled market outcomes
        settled_results: dict[str, str] = {
            m.get("ticker", ""): m.get("result", "")
            for m in settled_markets
            if m.get("ticker") and m.get("result")
        }

        wins = 0
        total = 0
        seen: set[str] = set()
        for trade in portfolio_trades:
            ticker = trade.get("ticker") or trade.get("market_ticker", "")
            if not ticker or ticker in seen:
                continue
            result = settled_results.get(ticker)
            if result is None:
                continue
            seen.add(ticker)
            side = trade.get("side", "")
            won = (side == "yes" and result == "yes") or (side == "no" and result == "no")
            wins += 1 if won else 0
            total += 1

        if total == 0:
            return self._seed_win_rate_prior(pnl, volume, markets_traded)

        logger.debug(
            "Settled-market win rate for %s: %d/%d → %.3f",
            username, wins, total, (wins + 2) / (total + 4),
        )
        return (wins + 2) / (total + 4)

    async def _enrich_win_rate_from_settled_markets(
        self, db: Session, merged: dict[str, dict]
    ) -> None:
        """Seed win_rate for cold-start traders using settled market history.

        Skips traders that already have real CopiedTrade history
        (has_trade_history=True), since update_trader_stats_from_history()
        already owns their win_rate.
        """
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            for nick, data in merged.items():
                username = nick.lower()
                trader = db.query(TrackedTrader).filter(
                    TrackedTrader.kalshi_username == username
                ).first()
                if trader is None or trader.has_trade_history:
                    continue
                win_rate = await self._fetch_win_rate_from_settled_markets(
                    client,
                    username,
                    data.get("pnl", 0.0),
                    data.get("volume", 0.0),
                    data.get("markets_traded", 0),
                )
                if win_rate > 0.0:
                    trader.win_rate = win_rate
                    trader.consistency_score = 0.5
                await asyncio.sleep(0.3)

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

        try:
            merged = await self.fetch_all_metrics()
            if not merged:
                raise ValueError("API returned 0 traders")
        except Exception as exc:
            logger.warning(
                "Leaderboard API unavailable (%s); falling back to HTML scrape", exc
            )
            merged = await self._scrape_html_fallback()

        if not merged:
            logger.warning("Leaderboard scrape returned 0 traders (API and HTML fallback both empty)")
            _last_scrape_time = datetime.utcnow()
            _last_scrape_count = 0
            return 0

        count = 0
        for data in merged.values():
            trader = self._upsert_trader(db, data)
            update_trader_stats_from_history(db, trader)
            trader.elephant_score = self._compute_elephant_score(
                data,
                win_rate=trader.win_rate or 0.0,
                consistency_score=trader.consistency_score or 0.0,
                total_profit=trader.total_profit or 0.0,
                total_trades=trader.total_trades or 0,
                avg_position_size=trader.avg_position_size or 0.0,
                last_seen=trader.last_seen,
            )
            count += 1

        db.flush()
        await self._enrich_win_rate_from_settled_markets(db, merged)
        await self._enrich_with_top_markets(db, merged)
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

    # Real trade history is now available; mark that seeded priors should not
    # be refreshed from leaderboard data on future scrapes.
    trader.has_trade_history = True

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
