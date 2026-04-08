"""Multi-timeframe swing trade signal scanner.

Applies weekly trend filter, 4-hour RSI divergence detection, and
support/resistance proximity checks to reduce false swing signals.

Signal strength is a 0–100 score. Adjustments applied:
  +15  trend_alignment bonus  — daily and weekly trend both agree with direction
  -20  weekly_trend_penalty   — signal direction opposes the weekly trend
"""

from dataclasses import dataclass, field
from enum import Enum


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class OHLCV:
    """A single price bar (open, high, low, close, volume)."""

    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class SwingSignal:
    """Result of multi-timeframe analysis for a single symbol."""

    symbol: str
    direction: Direction
    strength: float          # 0–100, adjusted for multi-timeframe factors
    daily_trend: str         # "up" | "down" | "neutral"
    weekly_trend: str        # "up" | "down" | "neutral"
    trend_aligned: bool      # daily and weekly trends both agree with direction
    near_support_resistance: bool
    rsi_divergence: bool     # 4H RSI divergence in the signal direction
    notes: list[str] = field(default_factory=list)


# Strength adjustment constants
TREND_ALIGNMENT_BONUS = 15
WEEKLY_TREND_PENALTY = 20


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sma(closes: list[float], period: int) -> float | None:
    """Return the simple moving average of the last *period* values, or None."""
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def _rsi(closes: list[float], period: int = 14) -> float | None:
    """
    Compute RSI using Wilder smoothing.

    Requires at least *period* + 1 values. Returns the RSI of the final bar,
    or None if there is insufficient data.
    """
    if len(closes) < period + 1:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _trend_from_sma(bars: list[OHLCV]) -> str:
    """
    Return 'up', 'down', or 'neutral' by comparing SMA20 and SMA50.

    Requires at least 50 bars for a definitive reading; returns 'neutral'
    when insufficient data is available.
    """
    closes = [b.close for b in bars]
    sma20 = _sma(closes, 20)
    sma50 = _sma(closes, 50)
    if sma20 is None or sma50 is None:
        return "neutral"
    if sma20 > sma50:
        return "up"
    if sma20 < sma50:
        return "down"
    return "neutral"


def _detect_rsi_divergence(
    four_hour_bars: list[OHLCV],
    rsi_period: int = 14,
    lookback: int = 5,
) -> str:
    """
    Detect RSI divergence on 4-hour bars.

    Bullish divergence: price makes a lower low while RSI makes a higher low.
    Bearish divergence: price makes a higher high while RSI makes a lower high.

    Returns 'bullish', 'bearish', or 'none'.
    Requires at least *rsi_period* + 1 + *lookback* bars.
    """
    min_bars = rsi_period + 1 + lookback
    if len(four_hour_bars) < min_bars:
        return "none"

    closes = [b.close for b in four_hour_bars]
    lows = [b.low for b in four_hour_bars]
    highs = [b.high for b in four_hour_bars]

    # Build a rolling RSI series aligned to the last len(closes) bars.
    rsi_series: list[float] = []
    for i in range(rsi_period + 1, len(closes) + 1):
        val = _rsi(closes[:i], rsi_period)
        if val is not None:
            rsi_series.append(val)

    if len(rsi_series) < lookback:
        return "none"

    recent_rsi = rsi_series[-lookback:]
    recent_lows = lows[-lookback:]
    recent_highs = highs[-lookback:]

    # Bullish divergence check: current bar makes the lowest low but RSI is
    # not at its lowest value within the lookback window.
    curr_low = recent_lows[-1]
    prior_lows = recent_lows[:-1]
    prior_low = min(prior_lows)
    if curr_low < prior_low:
        prior_low_idx = prior_lows.index(prior_low)
        prior_rsi_at_low = recent_rsi[prior_low_idx]
        if recent_rsi[-1] > prior_rsi_at_low:
            return "bullish"

    # Bearish divergence check: current bar makes the highest high but RSI is
    # not at its highest value within the lookback window.
    curr_high = recent_highs[-1]
    prior_highs = recent_highs[:-1]
    prior_high = max(prior_highs)
    if curr_high > prior_high:
        prior_high_idx = prior_highs.index(prior_high)
        prior_rsi_at_high = recent_rsi[prior_high_idx]
        if recent_rsi[-1] < prior_rsi_at_high:
            return "bearish"

    return "none"


def _near_support_resistance(
    price: float,
    daily_bars: list[OHLCV],
    tolerance_pct: float = 0.015,
) -> bool:
    """
    Return True if *price* is within *tolerance_pct* of a key level.

    Key levels:
    - Round numbers (multiples of 1, 5, 10, 25, 50, 100, 250, 500, 1000)
    - Previous daily swing highs and lows (local extrema)
    """
    tol = price * tolerance_pct

    # Round-number levels (whole dollars are not meaningful; start at $5)
    for magnitude in (5, 10, 25, 50, 100, 250, 500, 1000):
        if magnitude == 0:
            continue
        rounded = round(price / magnitude) * magnitude
        if abs(price - rounded) <= tol:
            return True

    # Daily swing highs / lows (simple 1-bar pivots)
    for i in range(1, len(daily_bars) - 1):
        bar = daily_bars[i]
        prev_bar = daily_bars[i - 1]
        next_bar = daily_bars[i + 1]
        if bar.high >= prev_bar.high and bar.high >= next_bar.high:
            if abs(price - bar.high) <= tol:
                return True
        if bar.low <= prev_bar.low and bar.low <= next_bar.low:
            if abs(price - bar.low) <= tol:
                return True

    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_multi_timeframe_filters(
    symbol: str,
    direction: Direction,
    base_strength: float,
    daily_bars: list[OHLCV],
    weekly_bars: list[OHLCV],
    four_hour_bars: list[OHLCV],
    current_price: float,
) -> SwingSignal:
    """
    Apply multi-timeframe confirmation filters to a raw swing signal.

    Parameters
    ----------
    symbol:         Ticker symbol (e.g. "AAPL").
    direction:      LONG or SHORT.
    base_strength:  Raw signal strength from the daily scanner (0–100).
    daily_bars:     Daily OHLCV history (50+ bars recommended).
    weekly_bars:    Weekly OHLCV history (50+ bars recommended).
    four_hour_bars: 4-hour OHLCV history (20+ bars recommended).
    current_price:  Latest price for S/R proximity check.

    Returns
    -------
    SwingSignal with adjusted strength and diagnostic notes.

    Strength adjustments
    --------------------
    -20  signal opposes the weekly trend
    +15  daily and weekly trends both agree with signal direction
    Strength is clamped to [0, 100].
    """
    notes: list[str] = []
    strength = float(base_strength)

    weekly_trend = _trend_from_sma(weekly_bars)
    daily_trend = _trend_from_sma(daily_bars)

    # --- Weekly trend filter ---
    against_weekly = (
        (direction == Direction.LONG and weekly_trend == "down")
        or (direction == Direction.SHORT and weekly_trend == "up")
    )
    if against_weekly:
        strength -= WEEKLY_TREND_PENALTY
        notes.append(
            f"weekly_trend_penalty: -{WEEKLY_TREND_PENALTY} "
            f"(signal opposes weekly {weekly_trend} trend)"
        )

    # --- Trend alignment bonus ---
    trend_aligned = (
        (direction == Direction.LONG and daily_trend == "up" and weekly_trend == "up")
        or (direction == Direction.SHORT and daily_trend == "down" and weekly_trend == "down")
    )
    if trend_aligned:
        strength += TREND_ALIGNMENT_BONUS
        notes.append(
            f"trend_alignment_bonus: +{TREND_ALIGNMENT_BONUS} "
            f"(daily and weekly both {weekly_trend})"
        )

    # --- 4-hour RSI divergence ---
    divergence = _detect_rsi_divergence(four_hour_bars)
    rsi_divergence = (
        (direction == Direction.LONG and divergence == "bullish")
        or (direction == Direction.SHORT and divergence == "bearish")
    )
    if rsi_divergence:
        notes.append(f"rsi_divergence: {divergence} divergence on 4H supports signal")

    # --- Support/resistance proximity ---
    near_sr = _near_support_resistance(current_price, daily_bars)
    if near_sr:
        notes.append("near_support_resistance: price near key level")

    strength = max(0.0, min(100.0, strength))

    return SwingSignal(
        symbol=symbol,
        direction=direction,
        strength=strength,
        daily_trend=daily_trend,
        weekly_trend=weekly_trend,
        trend_aligned=trend_aligned,
        near_support_resistance=near_sr,
        rsi_divergence=rsi_divergence,
        notes=notes,
    )
