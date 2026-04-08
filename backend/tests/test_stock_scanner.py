"""Tests for lib/stock_scanner multi-timeframe confirmation logic."""

import pytest

from lib.stock_scanner import (
    OHLCV,
    TREND_ALIGNMENT_BONUS,
    WEEKLY_TREND_PENALTY,
    Direction,
    SwingSignal,
    _detect_rsi_divergence,
    _near_support_resistance,
    _rsi,
    _sma,
    _trend_from_sma,
    apply_multi_timeframe_filters,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flat_bars(close: float, n: int) -> list[OHLCV]:
    """Return *n* bars all at the same price."""
    return [OHLCV(open=close, high=close, low=close, close=close, volume=1000.0)] * n


def _trending_bars(start: float, step: float, n: int) -> list[OHLCV]:
    """Return *n* bars stepping by *step* each bar."""
    bars = []
    price = start
    for _ in range(n):
        bars.append(OHLCV(open=price, high=price + abs(step), low=price - abs(step), close=price, volume=1000.0))
        price += step
    return bars


def _bars_with_sma_spread(sma20_above_sma50: bool, n: int = 60) -> list[OHLCV]:
    """
    Return *n* bars whose SMA20 is clearly above (or below) SMA50.

    When sma20_above_sma50=True the series trends up so that the 20-period
    average of recent closes is higher than the 50-period average (uptrend).
    """
    if sma20_above_sma50:
        # Uptrend: older bars at low price, recent bars at high price
        bars = _trending_bars(50.0, 1.0, n)
    else:
        # Downtrend: older bars at high price, recent bars at low price
        bars = _trending_bars(50.0 + n, -1.0, n)
    return bars


# ---------------------------------------------------------------------------
# _sma
# ---------------------------------------------------------------------------


class TestSma:
    def test_exact_period(self):
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _sma(closes, 5) == pytest.approx(3.0)

    def test_uses_last_n_values(self):
        closes = [100.0, 1.0, 2.0, 3.0]
        assert _sma(closes, 3) == pytest.approx(2.0)

    def test_insufficient_data_returns_none(self):
        assert _sma([1.0, 2.0], 5) is None

    def test_single_value(self):
        assert _sma([42.0], 1) == pytest.approx(42.0)


# ---------------------------------------------------------------------------
# _rsi
# ---------------------------------------------------------------------------


class TestRsi:
    def test_all_gains_returns_100(self):
        closes = [float(i) for i in range(1, 20)]  # always going up
        result = _rsi(closes, period=14)
        assert result == pytest.approx(100.0)

    def test_all_losses_returns_0(self):
        closes = [float(20 - i) for i in range(20)]  # always going down
        result = _rsi(closes, period=14)
        assert result is not None
        assert result == pytest.approx(0.0)

    def test_insufficient_data_returns_none(self):
        assert _rsi([1.0, 2.0, 3.0], period=14) is None

    def test_result_in_range(self):
        import random
        random.seed(42)
        closes = [50.0 + random.gauss(0, 2) for _ in range(30)]
        result = _rsi(closes, period=14)
        assert result is not None
        assert 0.0 <= result <= 100.0


# ---------------------------------------------------------------------------
# _trend_from_sma
# ---------------------------------------------------------------------------


class TestTrendFromSma:
    def test_uptrend_detected(self):
        bars = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        assert _trend_from_sma(bars) == "up"

    def test_downtrend_detected(self):
        bars = _bars_with_sma_spread(sma20_above_sma50=False, n=60)
        assert _trend_from_sma(bars) == "down"

    def test_insufficient_data_returns_neutral(self):
        bars = _flat_bars(100.0, 10)
        assert _trend_from_sma(bars) == "neutral"

    def test_flat_series_returns_neutral(self):
        bars = _flat_bars(100.0, 60)
        assert _trend_from_sma(bars) == "neutral"


# ---------------------------------------------------------------------------
# _detect_rsi_divergence
# ---------------------------------------------------------------------------


class TestDetectRsiDivergence:
    def _make_bullish_divergence_bars(self) -> list[OHLCV]:
        """
        Build a 4-hour bar sequence that produces bullish RSI divergence.

        The last bar has a lower low than the previous swing low, but RSI
        is higher (because the preceding bars had a sharp sell-off that
        drove RSI down, followed by a mild drift lower in price).
        """
        # 30 flat bars establish a baseline RSI near 50
        base = [OHLCV(open=100.0, high=101.0, low=99.0, close=100.0, volume=500.0)] * 20
        # 5 bars of sharp decline — RSI drops sharply
        decline = [
            OHLCV(open=100.0 - i * 3, high=101.0 - i * 3, low=99.0 - i * 3, close=100.0 - i * 3, volume=500.0)
            for i in range(1, 6)
        ]
        # 4 bars of mild recovery — RSI rises
        recovery = [
            OHLCV(open=85.0 + i, high=86.0 + i, low=84.0 + i, close=85.0 + i, volume=500.0)
            for i in range(1, 5)
        ]
        # Final bar: price dips slightly below the prior swing low (lower low),
        # but the preceding recovery means RSI is higher than it was at that low
        final = [OHLCV(open=84.0, high=84.5, low=83.0, close=83.5, volume=500.0)]
        return base + decline + recovery + final

    def test_insufficient_bars_returns_none(self):
        bars = _flat_bars(100.0, 5)
        assert _detect_rsi_divergence(bars, rsi_period=14, lookback=5) == "none"

    def test_flat_bars_returns_none(self):
        bars = _flat_bars(100.0, 30)
        assert _detect_rsi_divergence(bars, rsi_period=14, lookback=5) == "none"

    def test_uptrend_does_not_trigger_bullish_divergence(self):
        # Continuous uptrend: price and RSI both moving up — no divergence
        bars = _trending_bars(50.0, 1.0, 40)
        result = _detect_rsi_divergence(bars, rsi_period=14, lookback=5)
        assert result != "bullish"

    def test_bearish_divergence_detected(self):
        """
        Price makes a higher high while RSI makes a lower high = bearish divergence.
        """
        # Baseline
        base = [OHLCV(open=100.0, high=101.0, low=99.0, close=100.0, volume=500.0)] * 20
        # Sharp rally — RSI spikes
        rally = [
            OHLCV(open=100.0 + i * 3, high=101.0 + i * 3, low=99.0 + i * 3, close=100.0 + i * 3, volume=500.0)
            for i in range(1, 6)
        ]
        # Mild pullback — RSI drops back toward midpoint
        pullback = [
            OHLCV(open=115.0 - i, high=116.0 - i, low=114.0 - i, close=115.0 - i, volume=500.0)
            for i in range(1, 5)
        ]
        # Final bar: price pushes above the prior high (higher high),
        # but RSI is lower than it was at the prior high
        final = [OHLCV(open=116.0, high=117.5, low=115.5, close=116.5, volume=500.0)]
        bars = base + rally + pullback + final
        result = _detect_rsi_divergence(bars, rsi_period=14, lookback=5)
        assert result == "bearish"


# ---------------------------------------------------------------------------
# _near_support_resistance
# ---------------------------------------------------------------------------


class TestNearSupportResistance:
    def _dummy_daily(self, n: int = 60) -> list[OHLCV]:
        return _flat_bars(200.0, n)

    def test_price_at_round_number(self):
        assert _near_support_resistance(100.0, self._dummy_daily()) is True

    def test_price_near_round_number_within_tolerance(self):
        # 100 * 1.5% = 1.5 — a price of 101.0 is 1% away from 100
        assert _near_support_resistance(101.0, self._dummy_daily()) is True

    def test_price_far_from_any_level(self):
        # 112.4 → nearest $5 level = 110 (dist 2.4 > 112.4*0.015=1.69), far from all others too
        assert _near_support_resistance(112.4, self._dummy_daily()) is False

    def test_price_near_swing_high(self):
        # Create bars with a clear swing high at 150
        bars = [
            OHLCV(open=140.0, high=140.0, low=139.0, close=140.0, volume=1000.0),
            OHLCV(open=149.0, high=150.0, low=148.0, close=149.0, volume=1000.0),  # swing high
            OHLCV(open=142.0, high=143.0, low=141.0, close=142.0, volume=1000.0),
        ]
        assert _near_support_resistance(150.5, bars) is True

    def test_price_near_swing_low(self):
        bars = [
            OHLCV(open=160.0, high=161.0, low=160.0, close=160.0, volume=1000.0),
            OHLCV(open=120.0, high=121.0, low=119.0, close=120.0, volume=1000.0),  # swing low
            OHLCV(open=155.0, high=156.0, low=155.0, close=155.0, volume=1000.0),
        ]
        assert _near_support_resistance(119.5, bars) is True

    def test_insufficient_bars_for_swing_check(self):
        bars = [OHLCV(open=112.4, high=113.0, low=112.0, close=112.4, volume=1000.0)]
        # Only one bar — no swing detection possible, and price not near any round level
        assert _near_support_resistance(112.4, bars) is False


# ---------------------------------------------------------------------------
# apply_multi_timeframe_filters — integration
# ---------------------------------------------------------------------------


class TestApplyMultiTimeframeFilters:
    """Integration tests for the main public function."""

    def _four_hour_bars(self, n: int = 30) -> list[OHLCV]:
        return _flat_bars(100.0, n)

    def test_long_with_aligned_uptrend_gets_bonus(self):
        daily = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        weekly = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        sig = apply_multi_timeframe_filters(
            symbol="AAPL",
            direction=Direction.LONG,
            base_strength=50.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.trend_aligned is True
        assert sig.strength == pytest.approx(50.0 + TREND_ALIGNMENT_BONUS)
        assert sig.weekly_trend == "up"
        assert sig.daily_trend == "up"
        assert any("trend_alignment_bonus" in n for n in sig.notes)

    def test_short_with_aligned_downtrend_gets_bonus(self):
        daily = _bars_with_sma_spread(sma20_above_sma50=False, n=60)
        weekly = _bars_with_sma_spread(sma20_above_sma50=False, n=60)
        sig = apply_multi_timeframe_filters(
            symbol="TSLA",
            direction=Direction.SHORT,
            base_strength=60.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.trend_aligned is True
        assert sig.strength == pytest.approx(60.0 + TREND_ALIGNMENT_BONUS)
        assert any("trend_alignment_bonus" in n for n in sig.notes)

    def test_long_against_weekly_downtrend_gets_penalty(self):
        daily = _bars_with_sma_spread(sma20_above_sma50=True, n=60)   # daily up
        weekly = _bars_with_sma_spread(sma20_above_sma50=False, n=60)  # weekly down
        sig = apply_multi_timeframe_filters(
            symbol="NVDA",
            direction=Direction.LONG,
            base_strength=55.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.trend_aligned is False
        assert sig.strength == pytest.approx(55.0 - WEEKLY_TREND_PENALTY)
        assert any("weekly_trend_penalty" in n for n in sig.notes)

    def test_short_against_weekly_uptrend_gets_penalty(self):
        daily = _bars_with_sma_spread(sma20_above_sma50=False, n=60)   # daily down
        weekly = _bars_with_sma_spread(sma20_above_sma50=True, n=60)   # weekly up
        sig = apply_multi_timeframe_filters(
            symbol="AMZN",
            direction=Direction.SHORT,
            base_strength=55.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.strength == pytest.approx(55.0 - WEEKLY_TREND_PENALTY)
        assert any("weekly_trend_penalty" in n for n in sig.notes)

    def test_strength_clamped_to_zero(self):
        """A heavy penalty cannot push strength below 0."""
        daily = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        weekly = _bars_with_sma_spread(sma20_above_sma50=False, n=60)
        sig = apply_multi_timeframe_filters(
            symbol="X",
            direction=Direction.LONG,
            base_strength=5.0,  # very low — penalty takes it below 0
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.strength == pytest.approx(0.0)

    def test_strength_clamped_to_100(self):
        """Bonus cannot push strength above 100."""
        daily = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        weekly = _bars_with_sma_spread(sma20_above_sma50=True, n=60)
        sig = apply_multi_timeframe_filters(
            symbol="Y",
            direction=Direction.LONG,
            base_strength=95.0,  # bonus takes it above 100
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.strength == pytest.approx(100.0)

    def test_neutral_weekly_trend_no_penalty_no_bonus(self):
        """Neutral weekly trend does not trigger penalty or alignment bonus."""
        daily = _flat_bars(100.0, 60)   # neutral
        weekly = _flat_bars(100.0, 60)  # neutral
        sig = apply_multi_timeframe_filters(
            symbol="Z",
            direction=Direction.LONG,
            base_strength=50.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert sig.strength == pytest.approx(50.0)
        assert sig.trend_aligned is False
        assert not any("penalty" in n or "bonus" in n for n in sig.notes)

    def test_near_sr_flag_set_correctly(self):
        daily = _flat_bars(100.0, 60)
        weekly = _flat_bars(100.0, 60)
        sig = apply_multi_timeframe_filters(
            symbol="SPY",
            direction=Direction.LONG,
            base_strength=50.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=100.0,  # round number
        )
        assert sig.near_support_resistance is True
        assert any("near_support_resistance" in n for n in sig.notes)

    def test_rsi_divergence_flag_set_on_bullish_divergence(self):
        # Construct bars that produce a bullish divergence
        base = [OHLCV(open=100.0, high=101.0, low=99.0, close=100.0, volume=500.0)] * 20
        decline = [
            OHLCV(open=100.0 - i * 3, high=101.0 - i * 3, low=99.0 - i * 3, close=100.0 - i * 3, volume=500.0)
            for i in range(1, 6)
        ]
        recovery = [
            OHLCV(open=85.0 + i, high=86.0 + i, low=84.0 + i, close=85.0 + i, volume=500.0)
            for i in range(1, 5)
        ]
        final = [OHLCV(open=84.0, high=84.5, low=83.0, close=83.5, volume=500.0)]
        four_hour = base + decline + recovery + final

        daily = _flat_bars(100.0, 60)
        weekly = _flat_bars(100.0, 60)
        sig = apply_multi_timeframe_filters(
            symbol="GLD",
            direction=Direction.LONG,
            base_strength=50.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=four_hour,
            current_price=200.0,
        )
        assert sig.rsi_divergence is True
        assert any("rsi_divergence" in n for n in sig.notes)

    def test_returns_swing_signal_dataclass(self):
        daily = _flat_bars(100.0, 60)
        weekly = _flat_bars(100.0, 60)
        result = apply_multi_timeframe_filters(
            symbol="TEST",
            direction=Direction.LONG,
            base_strength=50.0,
            daily_bars=daily,
            weekly_bars=weekly,
            four_hour_bars=self._four_hour_bars(),
            current_price=200.0,
        )
        assert isinstance(result, SwingSignal)
        assert result.symbol == "TEST"
        assert result.direction == Direction.LONG
