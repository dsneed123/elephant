"""Tests for orderbook_monitor whale detection logic."""

import pytest
from unittest.mock import patch

from app.services.orderbook_monitor import _detect_whale
from app.services.signal_generator import WhaleEvent


@pytest.fixture(autouse=True)
def patch_threshold():
    """Fix whale threshold to 1000.0 USD for all tests."""
    with patch("app.services.orderbook_monitor.settings") as mock_settings:
        mock_settings.whale_order_threshold = 1000.0
        yield mock_settings


class TestDetectWhale:
    def test_returns_event_above_threshold(self):
        msg = {"market_ticker": "INFL-25", "side": "yes", "price": 55, "delta": 2000}
        # 2000 * 55 / 100 = 1100 USD — above threshold
        event = _detect_whale(msg)
        assert event is not None
        assert isinstance(event, WhaleEvent)
        assert event.market_ticker == "INFL-25"
        assert event.side == "yes"
        assert event.action == "buy"
        assert event.order_size == pytest.approx(1100.0)

    def test_returns_none_below_threshold(self):
        msg = {"market_ticker": "INFL-25", "side": "yes", "price": 55, "delta": 100}
        # 100 * 55 / 100 = 55 USD — below threshold
        assert _detect_whale(msg) is None

    def test_returns_none_at_threshold_boundary(self):
        msg = {"market_ticker": "INFL-25", "side": "no", "price": 50, "delta": 1999}
        # 1999 * 50 / 100 = 999.5 USD — just below
        assert _detect_whale(msg) is None

    def test_returns_event_at_exact_threshold(self):
        msg = {"market_ticker": "INFL-25", "side": "no", "price": 50, "delta": 2000}
        # 2000 * 50 / 100 = 1000 USD — at threshold
        event = _detect_whale(msg)
        assert event is not None
        assert event.order_size == pytest.approx(1000.0)

    def test_ignores_negative_delta(self):
        msg = {"market_ticker": "INFL-25", "side": "yes", "price": 55, "delta": -5000}
        # Negative delta = order removal, not a new order
        assert _detect_whale(msg) is None

    def test_ignores_zero_delta(self):
        msg = {"market_ticker": "INFL-25", "side": "yes", "price": 55, "delta": 0}
        assert _detect_whale(msg) is None

    def test_returns_none_missing_ticker(self):
        msg = {"side": "yes", "price": 55, "delta": 5000}
        assert _detect_whale(msg) is None

    def test_returns_none_missing_side(self):
        msg = {"market_ticker": "INFL-25", "price": 55, "delta": 5000}
        assert _detect_whale(msg) is None

    def test_no_side(self):
        msg = {"market_ticker": "INFL-25", "side": "no", "price": 30, "delta": 4000}
        # 4000 * 30 / 100 = 1200 USD
        event = _detect_whale(msg)
        assert event is not None
        assert event.side == "no"
