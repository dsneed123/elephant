"""
Swing trade signal scanner with multi-timeframe confirmation.

Signal generation pipeline:
  1. Daily RSI — primary signal source (oversold → LONG, overbought → SHORT)
  2. Weekly SMA20/SMA50 trend filter — only take signals in the trend direction
  3. 4-hour RSI divergence — bullish/bearish divergence boosts strength
  4. Support/resistance proximity — round numbers and previous swing highs/lows
  5. Strength adjustments — +15 trend alignment, -20 against-trend penalty
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Sequence

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


# ── public interface ──────────────────────────────────────────────────────────


def scan(
    symbol: str,
    daily_bars: Sequence[PriceBar],
    weekly_bars: Sequence[PriceBar],
    four_hour_bars: Sequence[PriceBar],
) -> list[SwingSignal]:
    """
    Scan a single symbol for swing trade signals using multi-timeframe confirmation.

    Signal pipeline:
      1. Daily RSI determines direction (LONG/SHORT) and base strength.
      2. Weekly SMA20/SMA50 trend filter applies alignment bonus or against-trend penalty.
      3. 4-hour RSI divergence boosts strength when it confirms the signal.
      4. Support/resistance proximity adds a small strength bonus.

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

    logger.info(
        "Signal: symbol=%s direction=%s strength=%d reasons=%s",
        symbol,
        direction,
        base_strength,
        reasons,
    )

    return [SwingSignal(symbol=symbol, direction=direction, strength=base_strength, reasons=reasons)]
