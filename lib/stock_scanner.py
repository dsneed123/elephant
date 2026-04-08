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

# state/swing_signals.json sits at the repo root (parent of lib/)
_REPO_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE: str = os.path.join(_REPO_ROOT, "state", "swing_signals.json")


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


def scan_with_tracking(
    symbol: str,
    daily_bars: Sequence[PriceBar],
    weekly_bars: Sequence[PriceBar],
    four_hour_bars: Sequence[PriceBar],
    *,
    webhook_url: str | None = None,
) -> list[SwingSignal]:
    """
    Scan a symbol for swing signals with persistent state tracking and Discord reporting.

    On each call:
      1. Load existing signal records from state/swing_signals.json.
      2. Resolve any OPEN signals for *symbol* (WIN / LOSS / EXPIRED) using daily_bars.
      3. Run the standard multi-timeframe scan() to detect new signals.
      4. Record each new signal (entry, target_1, target_2, stop) in state.
      5. Persist the updated records.
      6. If new signals were found and a webhook URL is available, post a signal embed
         followed by a Performance Summary embed to Discord.

    The webhook URL is taken from the *webhook_url* parameter or the
    DISCORD_WEBHOOK_URL environment variable (parameter takes priority).

    Returns the same list[SwingSignal] as scan().
    """
    if webhook_url is None:
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")

    records = _load_signal_state()
    records = _resolve_open_signals(records, symbol, daily_bars)

    signals = scan(symbol, daily_bars, weekly_bars, four_hour_bars)

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
