"""
Swing trade signal scanner with multi-timeframe confirmation.

Signal generation pipeline:
  1. Daily RSI — primary signal source (oversold → LONG, overbought → SHORT)
  2. Weekly SMA20/SMA50 trend filter — only take signals in the trend direction
  3. 4-hour RSI divergence — bullish/bearish divergence boosts strength
  4. Support/resistance proximity — round numbers and previous swing highs/lows
  5. Volume profile — high-volume price nodes act as support or resistance (+15)
  6. Unusual options activity — call/put volume >3x open interest (+20 confirm, -10 oppose)
  7. Strength adjustments — +15 trend alignment, -20 against-trend penalty

Pre-market gap detection and earnings calendar awareness require yfinance.
Install with: pip install yfinance
"""

from __future__ import annotations

import json
import logging
import math
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Sequence
from urllib.request import Request, urlopen
import urllib.error

try:
    import yfinance as _yf
    _YFINANCE_AVAILABLE = True
except ImportError:  # pragma: no cover
    _yf = None  # type: ignore[assignment]
    _YFINANCE_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── strength adjustments ──────────────────────────────────────────────────────

TREND_ALIGNMENT_BONUS: int = 15
AGAINST_TREND_PENALTY: int = 20

# ── RSI thresholds ────────────────────────────────────────────────────────────

RSI_OVERSOLD: float = 30.0
RSI_OVERBOUGHT: float = 70.0
RSI_PERIOD: int = 14

# ── support/resistance tolerance (fraction of price) ─────────────────────────

SR_TOLERANCE: float = 0.005  # 0.5 %

# ── signal tracking ───────────────────────────────────────────────────────────

TARGET_1_PCT: float = 0.03        # 3 % first profit target
TARGET_2_PCT: float = 0.06        # 6 % second profit target
STOP_PCT: float = 0.02            # 2 % stop-loss
SIGNAL_EXPIRY_DAYS: int = 15      # open signals expire after 15 days
SUMMARY_LOOKBACK_DAYS: int = 30   # performance summary window

# ── volume profile ────────────────────────────────────────────────────────────

VOLUME_PROFILE_LOOKBACK: int = 20  # days of history used to build the profile
VOLUME_NODE_BONUS: int = 15        # strength added when price is at a volume node

# ── unusual options activity ──────────────────────────────────────────────────

OPTIONS_UNUSUAL_RATIO: float = 3.0  # volume-to-open-interest threshold
OPTIONS_CONFIRM_BONUS: int = 20     # options confirm signal direction
OPTIONS_CONTRADICT_PENALTY: int = 10  # options oppose signal direction

# ── pre-market gap detection ──────────────────────────────────────────────────

GAP_THRESHOLD: float = 0.02       # 2 % gap triggers a Gap Alert

# ── earnings calendar awareness ───────────────────────────────────────────────

EARNINGS_EXCLUDE_DAYS: int = 2    # exclude swing signals if earnings within N days
EARNINGS_WARN_DAYS: int = 7       # add "EARNINGS [date]" note if earnings within N days

# state/swing_signals.json sits at the repo root (parent of lib/)
_REPO_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE: str = os.path.join(_REPO_ROOT, "state", "swing_signals.json")

# ── watchlist ─────────────────────────────────────────────────────────────────

WATCHLIST: dict[str, list[str]] = {
    "technology": ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "AMD", "TSM", "AVGO", "ORCL", "QCOM", "INTC"],
    "financials": ["JPM", "BAC", "GS", "MS", "WFC", "V", "MA", "BRK.B", "C", "AXP", "BLK", "SCHW"],
    "energy": ["XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "HAL"],
    "healthcare": ["JNJ", "UNH", "LLY", "PFE", "ABBV", "MRK", "ABT", "AMGN", "MRNA", "BMY", "GILD", "CVS"],
    "industrials": ["CAT", "DE", "GE", "HON", "UPS", "BA", "MMM", "LMT", "RTX", "FDX"],
    "consumer_discretionary": ["TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX", "BKNG"],
    "consumer_staples": ["WMT", "COST", "TGT", "PG", "KO", "PEP", "MDLZ", "CL"],
    "real_estate": ["O", "AMT", "PLD", "SPG", "EQIX", "CCI", "PSA", "DLR"],
    "utilities": ["NEE", "DUK", "SO", "D", "AEP", "EXC", "XEL", "WEC"],
    "etfs": ["SPY", "QQQ", "IWM", "GLD", "SLV", "TLT", "HYG", "VIX"],
}

# Sector ETFs used for rotation detection
SECTOR_ETFS: dict[str, str] = {
    "XLF": "Financials",
    "XLE": "Energy",
    "XLK": "Technology",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLU": "Utilities",
    "XLP": "Consumer Staples",
    "XLC": "Communication",
    "XLRE": "Real Estate",
}

# Flat symbol → sector mapping built from WATCHLIST + SECTOR_ETFS
SYMBOL_SECTOR: dict[str, str] = {
    sym: sector
    for sector, syms in WATCHLIST.items()
    for sym in syms
}
SYMBOL_SECTOR.update(
    {etf: label.lower().replace(" ", "_") for etf, label in SECTOR_ETFS.items()}
)

# ── sector rotation windows ───────────────────────────────────────────────────

ROTATION_SHORT_WINDOW: int = 5   # 5-day relative-strength window
ROTATION_LONG_WINDOW: int = 20   # 20-day relative-strength window


# ── data structures ───────────────────────────────────────────────────────────


@dataclass
class PriceBar:
    """OHLCV price bar for any timeframe."""

    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class SwingSignal:
    """A detected swing trade signal."""

    symbol: str
    direction: str          # "LONG" or "SHORT"
    strength: int           # 0–100 (higher = stronger signal)
    reasons: list[str] = field(default_factory=list)


@dataclass
class SectorRotationSignal:
    """
    Detected rotation between sector ETFs based on relative strength.

    Each entry in *strengthening* / *weakening* is (etf_symbol, sector_name, 5d_return).
    *all_sectors* holds every ETF ranked best-to-worst by their short-window return,
    as (etf_symbol, sector_name, short_return, long_return).
    """

    strengthening: list[tuple[str, str, float]]     # gaining relative strength
    weakening: list[tuple[str, str, float]]         # losing relative strength
    all_sectors: list[tuple[str, str, float, float]]


# ── indicator helpers ─────────────────────────────────────────────────────────


def _compute_sma(prices: Sequence[float], period: int) -> list[float]:
    """Return SMA values; the initial (period-1) entries are NaN."""
    result: list[float] = [float("nan")] * len(prices)
    for i in range(period - 1, len(prices)):
        result[i] = sum(prices[i - period + 1 : i + 1]) / period
    return result


def _compute_rsi(prices: Sequence[float], period: int = RSI_PERIOD) -> list[float]:
    """
    Return RSI values using Wilder smoothing.

    The first *period* entries are NaN; index *period* holds the initial RSI
    computed from a simple average; subsequent values use Wilder's EMA.
    """
    n = len(prices)
    result: list[float] = [float("nan")] * n
    if n < period + 1:
        return result

    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, n):
        delta = prices[i] - prices[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    def _rsi_from_avgs(ag: float, al: float) -> float:
        if al == 0.0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + ag / al)

    result[period] = _rsi_from_avgs(avg_gain, avg_loss)

    for i in range(period + 1, n):
        idx = i - 1  # offset into gains/losses (length n-1)
        avg_gain = (avg_gain * (period - 1) + gains[idx]) / period
        avg_loss = (avg_loss * (period - 1) + losses[idx]) / period
        result[i] = _rsi_from_avgs(avg_gain, avg_loss)

    return result


# ── multi-timeframe analysis ──────────────────────────────────────────────────


def _detect_weekly_trend(weekly_bars: Sequence[PriceBar]) -> str | None:
    """
    Return "uptrend" when weekly SMA20 > SMA50, "downtrend" when opposite.

    Returns None when there are fewer than 50 weekly bars (insufficient data).
    """
    closes = [b.close for b in weekly_bars]
    if len(closes) < 50:
        logger.debug("Weekly trend: insufficient bars (%d < 50)", len(closes))
        return None
    sma20 = _compute_sma(closes, 20)[-1]
    sma50 = _compute_sma(closes, 50)[-1]
    return "uptrend" if sma20 > sma50 else "downtrend"


def _detect_rsi_divergence(
    bars: Sequence[PriceBar],
    rsi_values: Sequence[float],
) -> str | None:
    """
    Detect RSI divergence using the last two swing pivots.

    Bullish divergence: price makes lower low, RSI makes higher low → "bullish"
    Bearish divergence: price makes higher high, RSI makes lower high → "bearish"

    Returns None when no divergence is found or data is insufficient.
    """
    closes = [b.close for b in bars]
    n = len(closes)
    if n < 10 or len(rsi_values) != n:
        return None

    swing_lows: list[tuple[int, float, float]] = []   # (index, price, rsi)
    swing_highs: list[tuple[int, float, float]] = []

    for i in range(1, n - 1):
        rsi_i = rsi_values[i]
        if math.isnan(rsi_i):
            continue
        if closes[i] < closes[i - 1] and closes[i] < closes[i + 1]:
            swing_lows.append((i, closes[i], rsi_i))
        if closes[i] > closes[i - 1] and closes[i] > closes[i + 1]:
            swing_highs.append((i, closes[i], rsi_i))

    if len(swing_lows) >= 2:
        _, p1, r1 = swing_lows[-2]
        _, p2, r2 = swing_lows[-1]
        if p2 < p1 and r2 > r1:          # lower price low, higher RSI low
            return "bullish"

    if len(swing_highs) >= 2:
        _, p1, r1 = swing_highs[-2]
        _, p2, r2 = swing_highs[-1]
        if p2 > p1 and r2 < r1:          # higher price high, lower RSI high
            return "bearish"

    return None


def _is_near_support_resistance(
    price: float,
    price_history: Sequence[float],
    tolerance: float = SR_TOLERANCE,
) -> bool:
    """
    Return True when *price* sits within *tolerance* of a key level.

    Key levels checked:
      - Round numbers: nearest multiple of 1, 5, 10, 50, or 100
      - Previous swing highs/lows from *price_history*
    """
    if price <= 0:
        return False

    for div in (1, 5, 10, 50, 100):
        nearest = round(price / div) * div
        if nearest > 0 and abs(price - nearest) / price <= tolerance:
            return True

    hist = list(price_history)
    for i in range(1, len(hist) - 1):
        is_pivot = (
            (hist[i] < hist[i - 1] and hist[i] < hist[i + 1])
            or (hist[i] > hist[i - 1] and hist[i] > hist[i + 1])
        )
        if is_pivot and abs(price - hist[i]) / price <= tolerance:
            return True

    return False


# ── volume profile analysis ───────────────────────────────────────────────────


def _detect_volume_node(
    bars: Sequence[PriceBar],
    current_price: float,
    lookback: int = VOLUME_PROFILE_LOOKBACK,
    tolerance: float = SR_TOLERANCE,
) -> str | None:
    """
    Identify the point of control (highest-volume price level) in the last
    *lookback* bars and check whether *current_price* is near it.

    Uses the typical price ``(high + low + close) / 3`` weighted by volume to
    build a 20-bin histogram.  The bin with the most accumulated volume is the
    volume node.

    Returns:
      "support"    — node is at or below current price (price is sitting on
                     a high-volume level; acts as a floor)
      "resistance" — node is above current price (price is approaching a
                     high-volume ceiling)
      None         — price is not within *tolerance* of any volume node, or
                     there is insufficient volume data.
    """
    recent = list(bars[-lookback:])
    if len(recent) < 5:
        return None

    prices = [(b.high + b.low + b.close) / 3.0 for b in recent]
    volumes = [b.volume for b in recent]

    if all(v == 0.0 for v in volumes):
        return None

    price_min = min(prices)
    price_max = max(prices)
    if price_max == price_min:
        return None

    n_bins = 20
    bin_width = (price_max - price_min) / n_bins
    bin_volumes = [0.0] * n_bins
    for p, v in zip(prices, volumes):
        idx = min(int((p - price_min) / bin_width), n_bins - 1)
        bin_volumes[idx] += v

    poc_bin = bin_volumes.index(max(bin_volumes))
    poc_price = price_min + (poc_bin + 0.5) * bin_width

    if abs(current_price - poc_price) / current_price > tolerance:
        return None

    return "support" if poc_price <= current_price else "resistance"


# ── unusual options activity ──────────────────────────────────────────────────


def get_unusual_options_activity(symbol: str) -> str | None:
    """
    Detect unusual options activity for *symbol* using yfinance.

    Aggregates call and put volume vs open interest across the two nearest
    expiration dates.  Returns:
      "calls" — total call volume > OPTIONS_UNUSUAL_RATIO × call open interest
      "puts"  — total put volume  > OPTIONS_UNUSUAL_RATIO × put open interest
      None    — no unusual activity, or yfinance is unavailable / data missing

    When both calls and puts qualify, the higher volume-to-OI ratio wins.
    Silently returns None on any network or data error.
    """
    if not _YFINANCE_AVAILABLE:
        logger.debug("yfinance not available; skipping options activity for %s", symbol)
        return None
    try:
        ticker = _yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None

        total_call_vol = 0.0
        total_call_oi = 0.0
        total_put_vol = 0.0
        total_put_oi = 0.0

        for exp in expirations[:2]:
            chain = ticker.option_chain(exp)
            calls = chain.calls
            puts = chain.puts
            if calls is not None and not calls.empty:
                total_call_vol += calls["volume"].fillna(0).sum()
                total_call_oi += calls["openInterest"].fillna(0).sum()
            if puts is not None and not puts.empty:
                total_put_vol += puts["volume"].fillna(0).sum()
                total_put_oi += puts["openInterest"].fillna(0).sum()

        call_ratio = total_call_vol / total_call_oi if total_call_oi > 0 else 0.0
        put_ratio = total_put_vol / total_put_oi if total_put_oi > 0 else 0.0

        unusual_calls = call_ratio > OPTIONS_UNUSUAL_RATIO
        unusual_puts = put_ratio > OPTIONS_UNUSUAL_RATIO

        if unusual_calls and unusual_puts:
            return "calls" if call_ratio >= put_ratio else "puts"
        if unusual_calls:
            return "calls"
        if unusual_puts:
            return "puts"
        return None
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to fetch options activity for %s: %s", symbol, exc)
        return None


# ── sector rotation detection ─────────────────────────────────────────────────


def detect_sector_rotation(
    sector_bars: dict[str, Sequence[PriceBar]],
    short_window: int = ROTATION_SHORT_WINDOW,
    long_window: int = ROTATION_LONG_WINDOW,
) -> SectorRotationSignal | None:
    """
    Detect sector rotation by comparing relative strength of sector ETFs.

    For each ETF supplied in *sector_bars*, computes:
      - short-window return: close[-1] / close[-short_window-1] − 1
      - long-window return:  close[-1] / close[-long_window-1] − 1

    ETFs whose short-window return exceeds the cross-sector average are classified
    as "strengthening"; those below average are "weakening".

    Returns None when fewer than two ETFs have sufficient price history.
    """
    ranked: list[tuple[str, str, float, float]] = []

    for etf, bars in sector_bars.items():
        sector_name = SECTOR_ETFS.get(etf, etf)
        closes = [b.close for b in bars]
        if len(closes) < long_window + 1:
            logger.debug("Sector rotation: %s has only %d bars (need %d)", etf, len(closes), long_window + 1)
            continue
        price_now = closes[-1]
        ret_short = price_now / closes[-(short_window + 1)] - 1.0
        ret_long = price_now / closes[-(long_window + 1)] - 1.0
        ranked.append((etf, sector_name, ret_short, ret_long))

    if len(ranked) < 2:
        return None

    ranked.sort(key=lambda x: x[2], reverse=True)  # best short-window return first
    avg_short = sum(r[2] for r in ranked) / len(ranked)

    strengthening = [(etf, name, ret_s) for etf, name, ret_s, _ in ranked if ret_s > avg_short]
    weakening = [(etf, name, ret_s) for etf, name, ret_s, _ in ranked if ret_s < avg_short]

    return SectorRotationSignal(
        strengthening=strengthening,
        weakening=weakening,
        all_sectors=ranked,
    )


# ── public interface ──────────────────────────────────────────────────────────


def scan(
    symbol: str,
    daily_bars: Sequence[PriceBar],
    weekly_bars: Sequence[PriceBar],
    four_hour_bars: Sequence[PriceBar],
    *,
    earnings_date: datetime | None = None,
    options_activity: str | None = None,
) -> list[SwingSignal]:
    """
    Scan a single symbol for swing trade signals using multi-timeframe confirmation.

    Signal pipeline:
      1. Daily RSI determines direction (LONG/SHORT) and base strength.
      2. Weekly SMA20/SMA50 trend filter applies alignment bonus or against-trend penalty.
      3. 4-hour RSI divergence boosts strength when it confirms the signal.
      4. Support/resistance proximity adds a small strength bonus.
      5. Volume profile — high-volume price node (POC) adds VOLUME_NODE_BONUS (+15)
         when price sits near it; reason is "Volume node support" or
         "Volume node resistance".
      6. Unusual options activity — *options_activity* should be the result of
         get_unusual_options_activity(symbol).  "calls" confirms LONG / opposes
         SHORT; "puts" confirms SHORT / opposes LONG.
         Confirms: +OPTIONS_CONFIRM_BONUS (+20); opposes: -OPTIONS_CONTRADICT_PENALTY (-10).
      7. Earnings awareness — "EARNINGS [date]" note added when earnings fall within
         EARNINGS_WARN_DAYS (7 days).  Callers should use *earnings_date* from
         get_earnings_date(); scan_with_tracking() enforces the 2-day exclusion.

    Returns a list with 0 or 1 SwingSignal entries.
    """
    if len(daily_bars) < RSI_PERIOD + 1:
        logger.debug("%s: insufficient daily bars (%d)", symbol, len(daily_bars))
        return []

    # ── 1. Daily signal source ────────────────────────────────────────────
    daily_closes = [b.close for b in daily_bars]
    daily_rsi = _compute_rsi(daily_closes)
    current_rsi = daily_rsi[-1]
    current_price = daily_closes[-1]

    if math.isnan(current_rsi):
        return []

    direction: str | None = None
    base_strength: int
    reasons: list[str] = []

    if current_rsi <= RSI_OVERSOLD:
        direction = "LONG"
        # Scale [0, RSI_OVERSOLD] → [100, 50]: more oversold = stronger signal
        base_strength = 50 + int((RSI_OVERSOLD - current_rsi) / RSI_OVERSOLD * 50)
        reasons.append(f"daily RSI oversold ({current_rsi:.1f})")
    elif current_rsi >= RSI_OVERBOUGHT:
        direction = "SHORT"
        # Scale [RSI_OVERBOUGHT, 100] → [50, 100]: more overbought = stronger signal
        base_strength = 50 + int((current_rsi - RSI_OVERBOUGHT) / (100.0 - RSI_OVERBOUGHT) * 50)
        reasons.append(f"daily RSI overbought ({current_rsi:.1f})")
    else:
        return []  # No daily signal

    # ── 2. Weekly trend filter ────────────────────────────────────────────
    weekly_trend = _detect_weekly_trend(weekly_bars)

    if weekly_trend is not None:
        aligned = (direction == "LONG" and weekly_trend == "uptrend") or (
            direction == "SHORT" and weekly_trend == "downtrend"
        )
        if aligned:
            base_strength = min(100, base_strength + TREND_ALIGNMENT_BONUS)
            reasons.append(f"weekly trend aligned ({weekly_trend})")
        else:
            base_strength = max(0, base_strength - AGAINST_TREND_PENALTY)
            reasons.append(
                f"weekly trend opposed ({weekly_trend}, -{AGAINST_TREND_PENALTY} strength)"
            )

    # ── 3. 4-hour RSI divergence ──────────────────────────────────────────
    if len(four_hour_bars) >= 10:
        fh_closes = [b.close for b in four_hour_bars]
        fh_rsi = _compute_rsi(fh_closes)
        divergence = _detect_rsi_divergence(four_hour_bars, fh_rsi)
        if divergence == "bullish" and direction == "LONG":
            base_strength = min(100, base_strength + 10)
            reasons.append("4h bullish RSI divergence")
        elif divergence == "bearish" and direction == "SHORT":
            base_strength = min(100, base_strength + 10)
            reasons.append("4h bearish RSI divergence")

    # ── 4. Support/resistance proximity ───────────────────────────────────
    if _is_near_support_resistance(current_price, daily_closes[:-1]):
        base_strength = min(100, base_strength + 5)
        reasons.append("near key support/resistance level")

    # ── 5. Volume profile ─────────────────────────────────────────────────
    volume_node = _detect_volume_node(daily_bars, current_price)
    if volume_node is not None:
        base_strength = min(100, base_strength + VOLUME_NODE_BONUS)
        reasons.append(f"Volume node {volume_node}")

    # ── 6. Unusual options activity ───────────────────────────────────────
    if options_activity == "calls":
        if direction == "LONG":
            base_strength = min(100, base_strength + OPTIONS_CONFIRM_BONUS)
            reasons.append("Unusual call activity")
        else:
            base_strength = max(0, base_strength - OPTIONS_CONTRADICT_PENALTY)
            reasons.append(f"Unusual call activity (opposes SHORT, -{OPTIONS_CONTRADICT_PENALTY} strength)")
    elif options_activity == "puts":
        if direction == "SHORT":
            base_strength = min(100, base_strength + OPTIONS_CONFIRM_BONUS)
            reasons.append("Unusual put activity")
        else:
            base_strength = max(0, base_strength - OPTIONS_CONTRADICT_PENALTY)
            reasons.append(f"Unusual put activity (opposes LONG, -{OPTIONS_CONTRADICT_PENALTY} strength)")

    # ── 7. Earnings awareness ─────────────────────────────────────────────
    if earnings_date is not None:
        now = datetime.now(tz=timezone.utc)
        days_to_earnings = (earnings_date - now).days
        if 0 <= days_to_earnings <= EARNINGS_WARN_DAYS:
            date_str = earnings_date.strftime("%Y-%m-%d")
            reasons.append(f"EARNINGS {date_str}")

    logger.info(
        "Signal: symbol=%s direction=%s strength=%d reasons=%s",
        symbol,
        direction,
        base_strength,
        reasons,
    )

    return [SwingSignal(symbol=symbol, direction=direction, strength=base_strength, reasons=reasons)]


# ── signal state management ───────────────────────────────────────────────────


def _load_signal_state() -> list[dict]:
    """Load persisted signal records from STATE_FILE. Returns [] on missing/corrupt file."""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_signal_state(records: list[dict]) -> None:
    """Write signal records to STATE_FILE, creating the directory if needed."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def _resolve_open_signals(
    records: list[dict],
    symbol: str,
    daily_bars: Sequence[PriceBar],
) -> list[dict]:
    """
    Update the status of OPEN signals for *symbol* against recent daily bars.

    Resolution rules (checked in priority order per bar):
      WIN     — target_1 reached: high >= target_1 (LONG) or low <= target_1 (SHORT)
      LOSS    — stop reached: low <= stop (LONG) or high >= stop (SHORT)
      EXPIRED — signal age exceeds SIGNAL_EXPIRY_DAYS with no resolution

    When WIN and LOSS conditions are both met on the same bar, WIN takes priority.
    Non-open records and records for other symbols are returned unchanged.
    """
    now = datetime.now(tz=timezone.utc)
    updated: list[dict] = []

    for rec in records:
        if rec["symbol"] != symbol or rec["status"] != "OPEN":
            updated.append(rec)
            continue

        sig_time = datetime.fromisoformat(rec["timestamp"])
        age_days = (now - sig_time).days

        if age_days > SIGNAL_EXPIRY_DAYS:
            updated.append({**rec, "status": "EXPIRED", "closed_at": now.isoformat()})
            continue

        # Approximate the bars that occurred since the signal was generated.
        # daily_bars are assumed chronological (oldest first, newest last).
        lookback = min(age_days + 1, len(daily_bars))
        recent_bars = list(daily_bars[-lookback:]) if lookback > 0 else []

        direction = rec["direction"]
        target_1: float = rec["target_1"]
        stop: float = rec["stop"]
        entry: float = rec["entry_price"]

        resolved_rec = rec
        for bar in recent_bars:
            if direction == "LONG":
                if bar.high >= target_1:
                    pnl = (target_1 - entry) / entry
                    resolved_rec = {**rec, "status": "WIN", "closed_at": now.isoformat(), "pnl_pct": pnl}
                    break
                if bar.low <= stop:
                    pnl = (stop - entry) / entry
                    resolved_rec = {**rec, "status": "LOSS", "closed_at": now.isoformat(), "pnl_pct": pnl}
                    break
            else:  # SHORT
                if bar.low <= target_1:
                    pnl = (entry - target_1) / entry
                    resolved_rec = {**rec, "status": "WIN", "closed_at": now.isoformat(), "pnl_pct": pnl}
                    break
                if bar.high >= stop:
                    pnl = (entry - stop) / entry
                    resolved_rec = {**rec, "status": "LOSS", "closed_at": now.isoformat(), "pnl_pct": pnl}
                    break

        updated.append(resolved_rec)

    return updated


def _performance_stats(records: list[dict]) -> dict:
    """
    Compute win/loss statistics from signals in the last SUMMARY_LOOKBACK_DAYS days.

    Returns a dict with keys:
      total, wins, losses, expired, open, win_rate, avg_win_pct, avg_loss_pct
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=SUMMARY_LOOKBACK_DAYS)
    recent = [r for r in records if datetime.fromisoformat(r["timestamp"]) >= cutoff]

    wins = [r for r in recent if r["status"] == "WIN"]
    losses = [r for r in recent if r["status"] == "LOSS"]
    expired = [r for r in recent if r["status"] == "EXPIRED"]
    open_sigs = [r for r in recent if r["status"] == "OPEN"]

    decided = len(wins) + len(losses)
    win_rate = len(wins) / decided if decided > 0 else 0.0
    avg_win_pct = sum(r["pnl_pct"] for r in wins) / len(wins) if wins else 0.0
    avg_loss_pct = sum(r["pnl_pct"] for r in losses) / len(losses) if losses else 0.0

    return {
        "total": len(recent),
        "wins": len(wins),
        "losses": len(losses),
        "expired": len(expired),
        "open": len(open_sigs),
        "win_rate": win_rate,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
    }


def _signal_embed(record: dict) -> dict:
    """Build a Discord embed dict for a newly detected swing signal."""
    direction = record["direction"]
    color = 0x57F287 if direction == "LONG" else 0xED4245
    entry: float = record["entry_price"]
    return {
        "title": f"{direction} Signal — {record['symbol']}",
        "color": color,
        "fields": [
            {"name": "Symbol", "value": record["symbol"], "inline": True},
            {"name": "Direction", "value": direction, "inline": True},
            {"name": "Strength", "value": str(record["strength"]), "inline": True},
            {"name": "Entry", "value": f"${entry:.2f}", "inline": True},
            {"name": "Target 1", "value": f"${record['target_1']:.2f}", "inline": True},
            {"name": "Stop", "value": f"${record['stop']:.2f}", "inline": True},
            {"name": "Reasons", "value": "; ".join(record.get("reasons", [])) or "—", "inline": False},
        ],
    }


def _performance_embed(stats: dict) -> dict:
    """Build a Discord embed dict for the performance summary."""
    decided = stats["wins"] + stats["losses"]
    win_rate_str = f"{stats['win_rate']:.1%}" if decided > 0 else "N/A"
    avg_win_str = f"+{stats['avg_win_pct']:.2%}" if stats["wins"] > 0 else "N/A"
    avg_loss_str = f"{stats['avg_loss_pct']:.2%}" if stats["losses"] > 0 else "N/A"
    return {
        "title": "Performance Summary",
        "description": f"Swing signal accuracy over the last {SUMMARY_LOOKBACK_DAYS} days",
        "color": 0x5865F2,
        "fields": [
            {"name": "Total Signals", "value": str(stats["total"]), "inline": True},
            {"name": "Wins", "value": str(stats["wins"]), "inline": True},
            {"name": "Losses", "value": str(stats["losses"]), "inline": True},
            {"name": "Expired", "value": str(stats["expired"]), "inline": True},
            {"name": "Win Rate", "value": win_rate_str, "inline": True},
            {"name": "Avg Win", "value": avg_win_str, "inline": True},
            {"name": "Avg Loss", "value": avg_loss_str, "inline": True},
        ],
    }


def _post_discord_embed(embed: dict, webhook_url: str) -> None:
    """POST a single Discord embed to *webhook_url*. Silently logs on failure."""
    if not webhook_url:
        return
    payload = json.dumps({"embeds": [embed]}).encode("utf-8")
    req = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=5) as resp:
            if resp.status >= 400:
                logger.warning("Discord webhook returned HTTP %d", resp.status)
    except urllib.error.URLError as exc:
        logger.warning("Discord webhook POST failed: %s", exc)


def _count_active_by_sector(records: list[dict]) -> dict[str, int]:
    """Return OPEN signal counts grouped by sector (uses SYMBOL_SECTOR mapping)."""
    counts: dict[str, int] = {}
    for rec in records:
        if rec["status"] != "OPEN":
            continue
        sector = SYMBOL_SECTOR.get(rec["symbol"], "other")
        counts[sector] = counts.get(sector, 0) + 1
    return counts


def _sector_rotation_embed(
    rotation: SectorRotationSignal,
    active_by_sector: dict[str, int] | None = None,
) -> dict:
    """Build a Discord embed for a Sector Rotation Alert."""

    def _row(etf: str, name: str, ret: float) -> str:
        arrow = "▲" if ret >= 0 else "▼"
        return f"{arrow} **{etf}** ({name}): {ret:+.2%}"

    strong_lines = [_row(e, n, r) for e, n, r in rotation.strengthening[:4]] or ["—"]
    weak_lines = [_row(e, n, r) for e, n, r in rotation.weakening[-4:]] or ["—"]

    fields: list[dict] = [
        {"name": "Strengthening ↑", "value": "\n".join(strong_lines), "inline": True},
        {"name": "Weakening ↓", "value": "\n".join(weak_lines), "inline": True},
    ]

    if active_by_sector:
        top = sorted(active_by_sector.items(), key=lambda x: x[1], reverse=True)[:5]
        sector_lines = [
            f"**{s}**: {n} signal{'s' if n != 1 else ''}" for s, n in top if n > 0
        ]
        if sector_lines:
            fields.append({
                "name": "Most Active Sectors",
                "value": "\n".join(sector_lines),
                "inline": False,
            })

    return {
        "title": "Sector Rotation Alert",
        "color": 0xFEE75C,
        "fields": fields,
    }


def _scan_header_embed(
    total_symbols: int,
    active_by_sector: dict[str, int],
) -> dict:
    """
    Build a Discord embed posted at the start of each scan run.

    Shows the number of symbols being scanned and which sectors currently
    have the most open signals.
    """
    top = sorted(active_by_sector.items(), key=lambda x: x[1], reverse=True)
    sector_lines = [
        f"**{s}**: {n} active signal{'s' if n != 1 else ''}" for s, n in top if n > 0
    ] or ["No active signals"]

    return {
        "title": "Swing Scanner — Scan Run",
        "description": (
            f"Scanning {total_symbols} symbol{'s' if total_symbols != 1 else ''} "
            f"across {len(WATCHLIST)} sectors"
        ),
        "color": 0x5865F2,
        "fields": [
            {
                "name": "Active Signals by Sector",
                "value": "\n".join(sector_lines),
                "inline": False,
            }
        ],
    }


# ── yfinance data fetchers ────────────────────────────────────────────────────


def get_earnings_date(symbol: str) -> datetime | None:
    """
    Return the next earnings date for *symbol* using yfinance, or None.

    Requires yfinance to be installed. Silently returns None on any failure
    (import error, network error, missing data).
    """
    if not _YFINANCE_AVAILABLE:
        logger.debug("yfinance not available; skipping earnings date for %s", symbol)
        return None
    try:
        cal = _yf.Ticker(symbol).calendar
        if not cal:
            return None
        raw = cal.get("Earnings Date")
        if raw is None:
            return None
        # calendar["Earnings Date"] may be a list of Timestamps or a single value
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        if raw is None:
            return None
        # Convert pandas Timestamp → datetime
        if hasattr(raw, "to_pydatetime"):
            dt = raw.to_pydatetime()
        elif isinstance(raw, datetime):
            dt = raw
        else:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to fetch earnings date for %s: %s", symbol, exc)
        return None


def get_pre_market_gap(symbol: str) -> tuple[float, float, float] | None:
    """
    Fetch pre-market price and compute the gap from previous close.

    Returns ``(gap_pct, pre_market_price, prev_close)`` where *gap_pct* is
    positive for gap-ups and negative for gap-downs.  Returns None when
    yfinance is unavailable or data is missing.
    """
    if not _YFINANCE_AVAILABLE:
        logger.debug("yfinance not available; skipping pre-market gap for %s", symbol)
        return None
    try:
        ticker = _yf.Ticker(symbol)
        fi = ticker.fast_info
        pre_price: float | None = getattr(fi, "pre_market_price", None)
        prev_close: float | None = getattr(fi, "previous_close", None)
        if prev_close is None:
            prev_close = getattr(fi, "regular_market_previous_close", None)
        if pre_price is None or prev_close is None or prev_close == 0:
            return None
        gap_pct = (pre_price - prev_close) / prev_close
        return (gap_pct, pre_price, prev_close)
    except Exception as exc:  # pragma: no cover
        logger.debug("Failed to fetch pre-market gap for %s: %s", symbol, exc)
        return None


# ── gap / earnings embed builders ────────────────────────────────────────────


def _gap_alert_embed(
    symbol: str,
    gap_pct: float,
    pre_price: float,
    prev_close: float,
) -> dict:
    """Build a Discord embed dict for a pre-market gap alert."""
    direction = "UP" if gap_pct >= 0 else "DOWN"
    color = 0x57F287 if gap_pct >= 0 else 0xED4245
    return {
        "title": f"Gap Alert — {symbol} ({direction})",
        "color": color,
        "fields": [
            {"name": "Symbol", "value": symbol, "inline": True},
            {"name": "Direction", "value": direction, "inline": True},
            {"name": "Gap", "value": f"{gap_pct:+.2%}", "inline": True},
            {"name": "Pre-Market Price", "value": f"${pre_price:.2f}", "inline": True},
            {"name": "Prev Close", "value": f"${prev_close:.2f}", "inline": True},
        ],
    }


def _earnings_watch_embed(earnings_list: list[tuple[str, str]]) -> dict:
    """
    Build a Discord embed dict for the weekly Earnings Watch.

    *earnings_list* is a list of ``(symbol, date_str)`` tuples sorted by date.
    """
    if not earnings_list:
        return {
            "title": "Earnings Watch — This Week",
            "description": "No watchlist stocks reporting earnings this week.",
            "color": 0xFEE75C,
            "fields": [],
        }
    lines = [f"**{sym}** — {date_str}" for sym, date_str in earnings_list]
    return {
        "title": "Earnings Watch — This Week",
        "description": (
            "Watchlist stocks reporting earnings this week. "
            f"Swing signals are excluded within {EARNINGS_EXCLUDE_DAYS} days of earnings."
        ),
        "color": 0xFEE75C,
        "fields": [
            {
                "name": "Upcoming Earnings",
                "value": "\n".join(lines),
                "inline": False,
            }
        ],
    }


# ── pre-market and earnings public functions ──────────────────────────────────


def scan_pre_market_gaps(
    symbols: list[str] | None = None,
    *,
    threshold: float = GAP_THRESHOLD,
    webhook_url: str | None = None,
) -> list[tuple[str, float, float, float]]:
    """
    Scan watchlist symbols for pre-market gaps exceeding *threshold*.

    For each gapping stock a Gap Alert embed is posted to Discord.

    Parameters
    ----------
    symbols:     Override the default full watchlist.
    threshold:   Minimum absolute gap percentage (default 2 %).
    webhook_url: Discord webhook URL; falls back to DISCORD_WEBHOOK_URL env var.

    Returns
    -------
    List of ``(symbol, gap_pct, pre_market_price, prev_close)`` for gappers.
    """
    if webhook_url is None:
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if symbols is None:
        symbols = [sym for syms in WATCHLIST.values() for sym in syms]

    gaps: list[tuple[str, float, float, float]] = []
    for sym in symbols:
        result = get_pre_market_gap(sym)
        if result is None:
            continue
        gap_pct, pre_price, prev_close = result
        if abs(gap_pct) >= threshold:
            gaps.append((sym, gap_pct, pre_price, prev_close))
            logger.info(
                "Gap alert: symbol=%s gap_pct=%.2f%% pre_price=%.2f prev_close=%.2f",
                sym, gap_pct * 100, pre_price, prev_close,
            )
            if webhook_url:
                _post_discord_embed(
                    _gap_alert_embed(sym, gap_pct, pre_price, prev_close),
                    webhook_url,
                )
    return gaps


def post_earnings_watch(
    symbols: list[str] | None = None,
    *,
    webhook_url: str | None = None,
) -> list[tuple[str, str]]:
    """
    Fetch earnings dates and post a weekly Earnings Watch embed to Discord.

    Intended to be called on Monday mornings.  Scans *symbols* (defaults to
    the full watchlist) for any earnings falling within the next 7 days and
    posts a single consolidated embed.

    Returns
    -------
    List of ``(symbol, date_str)`` for stocks reporting earnings this week,
    sorted by date.
    """
    if webhook_url is None:
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if symbols is None:
        symbols = [sym for syms in WATCHLIST.values() for sym in syms]

    now = datetime.now(tz=timezone.utc)
    week_end = now + timedelta(days=EARNINGS_WARN_DAYS)

    earnings_this_week: list[tuple[str, str]] = []
    for sym in symbols:
        edate = get_earnings_date(sym)
        if edate is None:
            continue
        if now <= edate <= week_end:
            date_str = edate.strftime("%Y-%m-%d")
            earnings_this_week.append((sym, date_str))
            logger.info("Earnings this week: %s on %s", sym, date_str)

    earnings_this_week.sort(key=lambda x: x[1])

    if webhook_url:
        _post_discord_embed(_earnings_watch_embed(earnings_this_week), webhook_url)

    return earnings_this_week


def scan_with_tracking(
    symbol: str,
    daily_bars: Sequence[PriceBar],
    weekly_bars: Sequence[PriceBar],
    four_hour_bars: Sequence[PriceBar],
    *,
    webhook_url: str | None = None,
    earnings_date: datetime | None = None,
    options_activity: str | None = None,
) -> list[SwingSignal]:
    """
    Scan a symbol for swing signals with persistent state tracking and Discord reporting.

    On each call:
      1. Load existing signal records from state/swing_signals.json.
      2. Resolve any OPEN signals for *symbol* (WIN / LOSS / EXPIRED) using daily_bars.
      3. If earnings fall within EARNINGS_EXCLUDE_DAYS (2 days), skip signal generation
         and return [] to avoid holding through earnings.
      4. Run the standard multi-timeframe scan() to detect new signals.
         Earnings within EARNINGS_WARN_DAYS (7 days) are flagged in the signal reasons
         as "EARNINGS [date]".
      5. Record each new signal (entry, target_1, target_2, stop) in state.
      6. Persist the updated records.
      7. If new signals were found and a webhook URL is available, post a signal embed
         followed by a Performance Summary embed to Discord.

    The webhook URL is taken from the *webhook_url* parameter or the
    DISCORD_WEBHOOK_URL environment variable (parameter takes priority).

    The *earnings_date* parameter should be the result of get_earnings_date(symbol).
    When None, earnings filtering is skipped.

    The *options_activity* parameter should be the result of
    get_unusual_options_activity(symbol).  When None, options signals are
    skipped (no network call is made inside this function).

    Returns the same list[SwingSignal] as scan().
    """
    if webhook_url is None:
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")

    records = _load_signal_state()
    records = _resolve_open_signals(records, symbol, daily_bars)

    # Exclude signals when earnings are within EARNINGS_EXCLUDE_DAYS days
    if earnings_date is not None:
        now_check = datetime.now(tz=timezone.utc)
        days_to_earnings = (earnings_date - now_check).days
        if 0 <= days_to_earnings < EARNINGS_EXCLUDE_DAYS:
            logger.info(
                "%s: skipping signal — earnings in %d day(s) on %s",
                symbol,
                days_to_earnings,
                earnings_date.strftime("%Y-%m-%d"),
            )
            _save_signal_state(records)
            return []

    signals = scan(
        symbol,
        daily_bars,
        weekly_bars,
        four_hour_bars,
        earnings_date=earnings_date,
        options_activity=options_activity,
    )

    now = datetime.now(tz=timezone.utc)
    open_keys = {(r["symbol"], r["direction"]) for r in records if r["status"] == "OPEN"}
    new_records: list[dict] = []

    for sig in signals:
        if (sig.symbol, sig.direction) in open_keys:
            continue  # already tracking an open signal for this symbol+direction
        entry = daily_bars[-1].close if daily_bars else 0.0
        if sig.direction == "LONG":
            target_1 = entry * (1.0 + TARGET_1_PCT)
            target_2 = entry * (1.0 + TARGET_2_PCT)
            stop = entry * (1.0 - STOP_PCT)
        else:
            target_1 = entry * (1.0 - TARGET_1_PCT)
            target_2 = entry * (1.0 - TARGET_2_PCT)
            stop = entry * (1.0 + STOP_PCT)

        rec: dict = {
            "id": str(uuid.uuid4()),
            "symbol": sig.symbol,
            "direction": sig.direction,
            "strength": sig.strength,
            "reasons": sig.reasons,
            "timestamp": now.isoformat(),
            "entry_price": entry,
            "target_1": target_1,
            "target_2": target_2,
            "stop": stop,
            "status": "OPEN",
            "closed_at": None,
            "pnl_pct": None,
        }
        records.append(rec)
        new_records.append(rec)

    _save_signal_state(records)

    if new_records and webhook_url:
        for rec in new_records:
            _post_discord_embed(_signal_embed(rec), webhook_url)
        stats = _performance_stats(records)
        _post_discord_embed(_performance_embed(stats), webhook_url)

    return signals
