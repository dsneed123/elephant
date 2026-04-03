"""Kalshi leaderboard scraper.

Scrapes https://kalshi.com/social/leaderboard using httpx + BeautifulSoup4.
Stores results in the TrackedTrader table. Rate-limited to once per hour.
"""

import asyncio
import json
import logging
import math
import re
from datetime import datetime, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import TrackedTrader

logger = logging.getLogger(__name__)

LEADERBOARD_URL = "https://kalshi.com/social/leaderboard"
KALSHI_TRADES_URL = "https://api.kalshi.com/trade-api/v2/portfolio/trades"
SCRAPE_MIN_INTERVAL_HOURS = 1  # Never scrape more than once per hour

# In-process rate-limit cache
_last_scrape_time: Optional[datetime] = None
_last_scrape_count: int = 0


class LeaderboardScraper:
    """Scrapes the Kalshi social leaderboard and persists results."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # ------------------------------------------------------------------ #
    # Rate limiting                                                        #
    # ------------------------------------------------------------------ #

    def is_rate_limited(self) -> bool:
        """Return True if we scraped within the last SCRAPE_MIN_INTERVAL_HOURS."""
        global _last_scrape_time
        if _last_scrape_time is None:
            return False
        return datetime.utcnow() - _last_scrape_time < timedelta(hours=SCRAPE_MIN_INTERVAL_HOURS)

    # ------------------------------------------------------------------ #
    # HTTP fetching                                                        #
    # ------------------------------------------------------------------ #

    async def _fetch_page(self, client: httpx.AsyncClient, page: int) -> Optional[str]:
        """Fetch a single leaderboard page and return HTML text, or None on error."""
        url = LEADERBOARD_URL if page == 1 else f"{LEADERBOARD_URL}?page={page}"
        logger.info("Fetching leaderboard page %d: %s", page, url)
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError as exc:
            logger.error("HTTP %s fetching page %d: %s", exc.response.status_code, page, exc)
        except httpx.RequestError as exc:
            logger.error("Request error fetching page %d: %s", page, exc)
        return None

    async def scrape_all_pages(self) -> list[dict]:
        """Fetch all leaderboard pages and return a flat list of raw trader dicts."""
        all_traders: list[dict] = []
        page = 1

        async with httpx.AsyncClient(
            headers=self.HEADERS,
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            while True:
                html = await self._fetch_page(client, page)
                if html is None:
                    break

                traders = self._parse_page(html, page)
                if not traders:
                    logger.info("No traders on page %d — stopping pagination", page)
                    break

                all_traders.extend(traders)
                logger.info("Page %d: found %d traders (total so far: %d)", page, len(traders), len(all_traders))

                if not self._has_next_page(html, page):
                    break

                page += 1
                await asyncio.sleep(0.5)  # polite delay between pages

        return all_traders

    # ------------------------------------------------------------------ #
    # Parsing strategies                                                   #
    # ------------------------------------------------------------------ #

    def _parse_page(self, html: str, page: int) -> list[dict]:
        """Try multiple parse strategies in order; return first non-empty result."""
        for strategy in (
            self._parse_next_data,
            self._parse_inline_json,
            self._parse_html_elements,
        ):
            traders = strategy(html)
            if traders:
                return traders
        return []

    def _parse_next_data(self, html: str) -> list[dict]:
        """Extract trader list from a Next.js __NEXT_DATA__ script tag."""
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if not tag or not tag.string:
            return []

        try:
            data = json.loads(tag.string)
            page_props = data.get("props", {}).get("pageProps", {})
            for key in ("leaderboard", "traders", "users", "rankings", "entries"):
                entries = page_props.get(key, [])
                if entries and isinstance(entries, list):
                    return [self._normalize_entry(e, i) for i, e in enumerate(entries)]
            return self._find_trader_list_in(data)
        except (json.JSONDecodeError, AttributeError) as exc:
            logger.debug("__NEXT_DATA__ parse failed: %s", exc)
            return []

    def _parse_inline_json(self, html: str) -> list[dict]:
        """Look for JSON arrays embedded in script tags that resemble trader data."""
        soup = BeautifulSoup(html, "html.parser")
        patterns = [
            r'window\.__\w+__\s*=\s*(\{.*?\});',
            r'"leaderboard"\s*:\s*(\[.*?\])',
            r'"traders"\s*:\s*(\[.*?\])',
            r'"rankings"\s*:\s*(\[.*?\])',
        ]
        for script in soup.find_all("script"):
            text = script.string or ""
            for pattern in patterns:
                match = re.search(pattern, text, re.DOTALL)
                if not match:
                    continue
                try:
                    parsed = json.loads(match.group(1))
                    if isinstance(parsed, list) and parsed:
                        traders = [self._normalize_entry(e, i) for i, e in enumerate(parsed)]
                        if traders:
                            return traders
                    elif isinstance(parsed, dict):
                        result = self._find_trader_list_in(parsed)
                        if result:
                            return result
                except json.JSONDecodeError:
                    continue
        return []

    def _parse_html_elements(self, html: str) -> list[dict]:
        """Parse trader data from HTML table rows or data-attribute elements."""
        soup = BeautifulSoup(html, "html.parser")
        traders: list[dict] = []

        # Strategy: HTML tables
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]
            for rank, row in enumerate(rows[1:], start=1):
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                entry = self._cells_to_dict(headers, cells, rank)
                if entry:
                    traders.append(entry)
            if traders:
                return traders

        # Strategy: elements with data-username attributes
        for rank, item in enumerate(soup.find_all(attrs={"data-username": True}), start=1):
            entry = {
                "username": item.get("data-username", ""),
                "display_name": item.get("data-display-name") or item.get("data-username", ""),
                "total_profit": self._safe_float(item.get("data-profit")),
                "win_rate": self._safe_float(item.get("data-win-rate")),
                "total_trades": self._safe_int(item.get("data-trades")),
                "avg_position_size": self._safe_float(item.get("data-avg-position")),
                "market_diversity": self._safe_int(item.get("data-market-diversity")),
                "consistency_score": self._safe_float(item.get("data-consistency")),
                "rank": rank,
            }
            if entry["username"]:
                traders.append(entry)

        return traders

    def _cells_to_dict(self, headers: list[str], cells, rank: int) -> Optional[dict]:
        """Map a table row's cells to a trader dict using header name heuristics."""
        FIELD_PATTERNS: dict[str, list[str]] = {
            "username": ["username", "user", "handle", "trader", "account"],
            "display_name": ["name", "display name", "full name"],
            "total_profit": ["profit", "total profit", "p&l", "pnl", "earnings", "return"],
            "win_rate": ["win rate", "win%", "winrate", "win pct"],
            "total_trades": ["trades", "total trades", "# trades", "trade count"],
            "avg_position_size": ["avg position", "avg size", "position size", "avg pos"],
            "market_diversity": ["diversity", "markets", "market count"],
            "consistency_score": ["consistency", "consist"],
        }
        entry: dict = {}
        for i, cell in enumerate(cells):
            if i >= len(headers):
                break
            header = headers[i]
            text = cell.get_text(strip=True)
            for field, patterns in FIELD_PATTERNS.items():
                if any(p in header for p in patterns):
                    if field in ("total_profit", "win_rate", "avg_position_size", "consistency_score"):
                        entry[field] = self._safe_float(text)
                    elif field in ("total_trades", "market_diversity"):
                        entry[field] = self._safe_int(text)
                    else:
                        entry[field] = text

        # Fallback: use first non-numeric cell as username
        if not entry.get("username"):
            for cell in cells:
                text = cell.get_text(strip=True)
                cleaned = re.sub(r"[^0-9.\-]", "", text)
                if text and not cleaned == text:  # has non-numeric chars
                    entry["username"] = text.lower().replace(" ", "_")
                    entry.setdefault("display_name", text)
                    break

        if not entry.get("username"):
            return None

        entry["rank"] = rank
        return entry

    def _find_trader_list_in(self, data: dict, depth: int = 0) -> list[dict]:
        """Recursively search a nested dict for a list that looks like trader records."""
        if depth > 5 or not isinstance(data, dict):
            return []
        for val in data.values():
            if isinstance(val, list) and len(val) >= 3:
                sample = val[0] if val else {}
                if isinstance(sample, dict) and any(
                    k in sample
                    for k in ("username", "user_name", "userName", "handle", "profit", "win_rate", "winRate")
                ):
                    return [self._normalize_entry(e, i) for i, e in enumerate(val)]
            elif isinstance(val, dict):
                result = self._find_trader_list_in(val, depth + 1)
                if result:
                    return result
        return []

    def _normalize_entry(self, entry: dict, index: int) -> dict:
        """Normalise a raw JSON trader entry to our internal schema."""
        username = (
            entry.get("username")
            or entry.get("user_name")
            or entry.get("userName")
            or entry.get("handle")
            or entry.get("userId")
            or f"trader_{index + 1}"
        )
        display_name = (
            entry.get("display_name")
            or entry.get("displayName")
            or entry.get("name")
            or username
        )

        profit = self._safe_float(
            entry.get("total_profit")
            or entry.get("totalProfit")
            or entry.get("profit")
            or entry.get("pnl")
            or entry.get("total_pnl")
        )
        win_rate = self._safe_float(
            entry.get("win_rate")
            or entry.get("winRate")
            or entry.get("win_percentage")
            or entry.get("winPercentage")
        )
        # Normalise win_rate to [0, 1] if expressed as a percentage
        if win_rate > 1.0:
            win_rate /= 100.0

        total_trades = self._safe_int(
            entry.get("total_trades")
            or entry.get("totalTrades")
            or entry.get("trades")
            or entry.get("tradeCount")
        )
        avg_pos = self._safe_float(
            entry.get("avg_position_size")
            or entry.get("avgPositionSize")
            or entry.get("average_position")
            or entry.get("avgPosition")
        )
        diversity = self._safe_int(
            entry.get("market_diversity")
            or entry.get("marketDiversity")
            or entry.get("markets_traded")
            or entry.get("marketCount")
        )
        consistency = self._safe_float(
            entry.get("consistency_score")
            or entry.get("consistencyScore")
            or entry.get("consistency")
        )

        top_markets_raw = (
            entry.get("top_markets")
            or entry.get("topMarkets")
            or entry.get("top_market_tickers")
            or []
        )
        if isinstance(top_markets_raw, list):
            top_markets = [str(m).strip() for m in top_markets_raw if m]
        else:
            top_markets = []

        return {
            "username": str(username).lower().strip(),
            "display_name": str(display_name).strip(),
            "total_profit": profit,
            "win_rate": win_rate,
            "total_trades": total_trades,
            "avg_position_size": avg_pos,
            "market_diversity": diversity,
            "consistency_score": consistency,
            "top_markets": top_markets,
            "rank": index + 1,
        }

    def _has_next_page(self, html: str, current_page: int) -> bool:
        """Heuristic check for a next-page link in the HTML."""
        soup = BeautifulSoup(html, "html.parser")
        if soup.find("a", string=re.compile(r"next|›|»", re.I)):
            return True
        if soup.find_all("a", href=re.compile(rf"[?&]page={current_page + 1}")):
            return True
        return False

    # ------------------------------------------------------------------ #
    # Market enrichment via Kalshi public API                             #
    # ------------------------------------------------------------------ #

    async def _fetch_trader_markets(
        self, client: httpx.AsyncClient, username: str
    ) -> list[str]:
        """Fetch market tickers from a trader's recent public trades.

        Returns an empty list if the endpoint is inaccessible or returns nothing.
        """
        try:
            resp = await client.get(
                KALSHI_TRADES_URL,
                params={"username": username, "limit": 100},
            )
            if resp.status_code in (401, 403, 404):
                return []
            resp.raise_for_status()
            data = resp.json()
            trades = data.get("trades", [])
            seen: set[str] = set()
            for trade in trades:
                ticker = trade.get("ticker") or trade.get("market_ticker") or ""
                if ticker:
                    seen.add(ticker)
            return sorted(seen)
        except (httpx.RequestError, httpx.HTTPStatusError):
            return []

    async def _enrich_markets_from_api(self, traders: list[dict]) -> None:
        """Populate top_markets for traders whose leaderboard entry lacked market data.

        Makes a best-effort call to the Kalshi public trades API for each trader
        with an empty top_markets list. Skips silently if the API is not accessible.
        """
        to_enrich = [d for d in traders if not d.get("top_markets")]
        if not to_enrich:
            return

        async with httpx.AsyncClient(
            headers=self.HEADERS,
            follow_redirects=True,
            timeout=10.0,
        ) as client:
            for data in to_enrich:
                markets = await self._fetch_trader_markets(client, data["username"])
                if markets:
                    data["top_markets"] = markets
                await asyncio.sleep(0.1)  # polite delay between requests

    # ------------------------------------------------------------------ #
    # Scoring & tiering                                                    #
    # ------------------------------------------------------------------ #

    def _compute_elephant_score(self, data: dict) -> float:
        """
        Composite elephant score (0–100).

        Weights:
          30%  win_rate          (0–1)
          25%  consistency_score (0–1)
          20%  ROI score         (log-scaled, $10k → 1.0)
          15%  market_diversity  (normalised, cap 20 markets)
          10%  recency           (1.0 within 7 days, linear decay to 0 at 90 days)
        """
        win_rate = max(0.0, min(1.0, data.get("win_rate", 0.0)))
        profit = data.get("total_profit", 0.0)
        consistency = max(0.0, min(1.0, data.get("consistency_score", 0.0)))
        diversity = max(0, data.get("market_diversity", 0))

        profit_score = min(1.0, math.log1p(profit) / math.log1p(10_000)) if profit > 0 else 0.0
        diversity_score = min(1.0, diversity / 20.0)

        last_active: Optional[datetime] = data.get("last_active")
        if last_active is None:
            recency_score = 1.0  # appearing in current scrape means active now
        else:
            days = (datetime.utcnow() - last_active).days
            if days <= 7:
                recency_score = 1.0
            elif days >= 90:
                recency_score = 0.0
            else:
                recency_score = 1.0 - (days - 7) / (90 - 7)

        raw = (
            0.30 * win_rate
            + 0.25 * consistency
            + 0.20 * profit_score
            + 0.15 * diversity_score
            + 0.10 * recency_score
        )
        return round(raw * 100.0, 2)

    def _assign_tier(self, rank: int, total: int) -> str:
        """Assign a tier label based on percentile rank."""
        if total == 0:
            return "unranked"
        pct = rank / total
        if pct <= 0.001:
            return "top_001"
        if pct <= 0.01:
            return "top_01"
        if pct <= 0.1:
            return "top_1"
        if pct <= 0.25:
            return "top_25"
        return "ranked"

    # ------------------------------------------------------------------ #
    # Database persistence                                                 #
    # ------------------------------------------------------------------ #

    def _upsert_trader(self, db: Session, data: dict, rank: int, total: int) -> TrackedTrader:
        """Insert or update a TrackedTrader row."""
        username = data["username"]
        trader = db.query(TrackedTrader).filter(
            TrackedTrader.kalshi_username == username
        ).first()

        elephant_score = self._compute_elephant_score(data)
        tier = self._assign_tier(rank, total)

        top_markets_list = data.get("top_markets", [])
        top_markets_json = json.dumps(top_markets_list) if top_markets_list else None

        if trader is None:
            trader = TrackedTrader(
                kalshi_username=username,
                display_name=data.get("display_name", username),
                total_profit=data.get("total_profit", 0.0),
                win_rate=data.get("win_rate", 0.0),
                total_trades=data.get("total_trades", 0),
                avg_position_size=data.get("avg_position_size", 0.0),
                market_diversity=data.get("market_diversity", 0),
                consistency_score=data.get("consistency_score", 0.0),
                elephant_score=elephant_score,
                tier=tier,
                top_markets=top_markets_json,
                is_active=True,
                last_seen=datetime.utcnow(),
            )
            db.add(trader)
            logger.debug("New trader: %s  score=%.1f  tier=%s", username, elephant_score, tier)
        else:
            trader.display_name = data.get("display_name", trader.display_name)
            trader.total_profit = data.get("total_profit", trader.total_profit)
            trader.win_rate = data.get("win_rate", trader.win_rate)
            trader.total_trades = data.get("total_trades", trader.total_trades)
            trader.avg_position_size = data.get("avg_position_size", trader.avg_position_size)
            trader.market_diversity = data.get("market_diversity", trader.market_diversity)
            trader.consistency_score = data.get("consistency_score", trader.consistency_score)
            trader.elephant_score = elephant_score
            trader.tier = tier
            if top_markets_json is not None:
                trader.top_markets = top_markets_json
            trader.is_active = True
            trader.last_seen = datetime.utcnow()
            logger.debug("Updated trader: %s  score=%.1f  tier=%s", username, elephant_score, tier)

        return trader

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #

    async def scrape(self, db: Session) -> int:
        """
        Scrape the Kalshi leaderboard and persist results to *db*.

        Respects rate limiting — returns the cached count if called within
        SCRAPE_MIN_INTERVAL_HOURS of the last successful scrape.

        Returns the number of traders upserted.
        """
        global _last_scrape_time, _last_scrape_count

        if self.is_rate_limited():
            elapsed_min = int((datetime.utcnow() - _last_scrape_time).total_seconds() // 60)
            logger.info(
                "Rate-limited: last scrape was %d min ago (min interval %dh). "
                "Returning cached count %d.",
                elapsed_min,
                SCRAPE_MIN_INTERVAL_HOURS,
                _last_scrape_count,
            )
            return _last_scrape_count

        logger.info("Starting Kalshi leaderboard scrape")
        started_at = datetime.utcnow()

        raw_traders = await self.scrape_all_pages()
        total = len(raw_traders)

        if total == 0:
            logger.warning(
                "Scraper returned 0 traders. "
                "The leaderboard page may be JavaScript-rendered or its HTML structure has changed."
            )
            _last_scrape_time = datetime.utcnow()
            _last_scrape_count = 0
            return 0

        await self._enrich_markets_from_api(raw_traders)

        count = 0
        for rank, data in enumerate(raw_traders, start=1):
            if not data.get("username"):
                continue
            self._upsert_trader(db, data, rank, total)
            count += 1

        db.commit()

        elapsed_s = (datetime.utcnow() - started_at).total_seconds()
        logger.info("Leaderboard scrape complete: %d traders upserted in %.1fs", count, elapsed_s)

        _last_scrape_time = datetime.utcnow()
        _last_scrape_count = count
        return count

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _safe_float(value) -> float:
        if value is None:
            return 0.0
        try:
            cleaned = re.sub(r"[^0-9.\-]", "", str(value))
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _safe_int(value) -> int:
        if value is None:
            return 0
        try:
            cleaned = re.sub(r"[^0-9]", "", str(value))
            return int(cleaned) if cleaned else 0
        except (ValueError, TypeError):
            return 0


# Module-level singleton used by scheduler and CLI
scraper = LeaderboardScraper()


async def run_scrape() -> int:
    """Scheduler / CLI entry point — manages its own DB session."""
    db = SessionLocal()
    try:
        return await scraper.scrape(db)
    finally:
        db.close()
