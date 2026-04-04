"""Tests for orderbook_monitor whale detection logic and OrderbookMonitor class."""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.orderbook_monitor import OrderbookMonitor, _detect_whale
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
        assert event.price == 55

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
        assert event.price == 50

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


class TestOrderbookMonitorHealthCheck:
    def test_initial_state(self):
        monitor = OrderbookMonitor()
        health = monitor.health_check()
        assert health["connected"] is False
        assert health["subscribed_markets"] == 0
        assert health["last_message_at"] is None

    def test_reflects_connected_state(self):
        monitor = OrderbookMonitor()
        monitor._connected = True
        monitor._subscribed_markets = {"INFL-25", "BTCUSD"}
        monitor._last_message_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        health = monitor.health_check()
        assert health["connected"] is True
        assert health["subscribed_markets"] == 2
        assert health["last_message_at"] == "2026-01-01T12:00:00+00:00"

    def test_last_message_at_none_when_no_messages(self):
        monitor = OrderbookMonitor()
        assert monitor.health_check()["last_message_at"] is None


class TestSubscriptionRefresh:
    @pytest.mark.asyncio
    async def test_subscribes_to_new_tickers(self):
        """Refresh sends a subscribe message when new markets are discovered."""
        monitor = OrderbookMonitor()
        monitor._subscribed_markets = {"INFL-25"}

        sent_messages = []
        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        class FakeWS:
            async def send(self, data):
                sent_messages.append(json.loads(data))

        with (
            patch(
                "app.services.orderbook_monitor._get_tracked_market_tickers",
                return_value=["INFL-25", "BTCUSD"],
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            try:
                await monitor._refresh_subscriptions(FakeWS())
            except asyncio.CancelledError:
                pass

        assert len(sent_messages) == 1
        msg = sent_messages[0]
        assert msg["cmd"] == "subscribe"
        assert msg["params"]["channels"] == ["orderbook_delta"]
        assert "BTCUSD" in msg["params"]["market_tickers"]
        assert "INFL-25" not in msg["params"]["market_tickers"]
        assert "BTCUSD" in monitor._subscribed_markets
        assert "INFL-25" in monitor._subscribed_markets

    @pytest.mark.asyncio
    async def test_no_send_when_no_new_tickers(self):
        """Refresh does not send a message when all tickers are already subscribed."""
        monitor = OrderbookMonitor()
        monitor._subscribed_markets = {"INFL-25", "BTCUSD"}

        sent_messages = []
        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        class FakeWS:
            async def send(self, data):
                sent_messages.append(json.loads(data))

        with (
            patch(
                "app.services.orderbook_monitor._get_tracked_market_tickers",
                return_value=["INFL-25", "BTCUSD"],
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            try:
                await monitor._refresh_subscriptions(FakeWS())
            except asyncio.CancelledError:
                pass

        assert len(sent_messages) == 0

    @pytest.mark.asyncio
    async def test_refresh_task_cancelled_on_connection_end(self):
        """The subscription refresh task is done (cancelled) when the WS message loop exits."""
        monitor = OrderbookMonitor()
        monitor._attempt = 5

        raw_msg = json.dumps({"type": "orderbook_delta", "msg": {"delta": 0}})

        class FakeWS:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return False

            async def send(self, data):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield raw_msg

        fake_ws = FakeWS()
        created_tasks: list = []
        real_create_task = asyncio.create_task

        def spy_create_task(coro, **kwargs):
            task = real_create_task(coro, **kwargs)
            created_tasks.append(task)
            return task

        with (
            patch("app.services.orderbook_monitor._make_auth_headers", return_value={}),
            patch("app.services.orderbook_monitor.settings") as mock_settings,
            patch("websockets.connect", return_value=fake_ws),
            patch("asyncio.create_task", side_effect=spy_create_task),
        ):
            mock_settings.kalshi_ws_url = "wss://fake"
            mock_settings.whale_order_threshold = 1000.0
            await monitor._run_connection(["INFL-25"], MagicMock())

        assert len(created_tasks) == 1
        assert created_tasks[0].done()


class TestOrderbookMonitorReconnection:
    @pytest.mark.asyncio
    async def test_resets_attempt_on_first_message(self):
        """Attempt counter resets to 0 when the first WS message is received."""
        monitor = OrderbookMonitor()
        monitor._attempt = 5  # simulate several prior failures

        # Craft a non-whale orderbook_delta message
        raw_msg = json.dumps({"type": "orderbook_delta", "msg": {"delta": 0}})

        class FakeWS:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return False

            async def send(self, data):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield raw_msg

        fake_ws = FakeWS()

        with (
            patch("app.services.orderbook_monitor._make_auth_headers", return_value={}),
            patch("app.services.orderbook_monitor.settings") as mock_settings,
            patch("websockets.connect", return_value=fake_ws),
        ):
            mock_settings.kalshi_ws_url = "wss://fake"
            mock_settings.whale_order_threshold = 1000.0
            await monitor._run_connection(["INFL-25"], MagicMock())

        assert monitor._attempt == 0

    @pytest.mark.asyncio
    async def test_backoff_delays_on_reconnect(self):
        """run() sleeps for the correct exponential backoff duration on attempt > 0."""
        monitor = OrderbookMonitor()
        call_delays: list[float] = []

        async def fake_sleep(seconds):
            call_delays.append(seconds)

        attempt_count = 0

        async def fake_run_connection(tickers, pk):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count >= 2:
                # Stop the loop after 2 connection attempts
                monitor._running = False
            raise OSError("fake disconnect")

        with (
            patch("app.services.orderbook_monitor._load_private_key", return_value=MagicMock()),
            patch("app.services.orderbook_monitor._get_tracked_market_tickers", return_value=["INFL-25"]),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            monitor._run_connection = fake_run_connection
            await monitor.run()

        # First attempt: no sleep. Second attempt: sleep(min(2**1 * 1.0, 60)) = 2.0s
        assert len(call_delays) == 1
        assert call_delays[0] == pytest.approx(2.0)

    @pytest.mark.asyncio
    async def test_connected_flag_cleared_after_disconnect(self):
        """_connected is set to False in the finally block after a connection ends."""
        monitor = OrderbookMonitor()

        async def fake_run_connection(tickers, pk):
            monitor._connected = True
            raise OSError("dropped")

        monitor._running = True

        async def stop_after_one(*_):
            monitor._running = False

        with (
            patch("app.services.orderbook_monitor._load_private_key", return_value=MagicMock()),
            patch("app.services.orderbook_monitor._get_tracked_market_tickers", return_value=["INFL-25"]),
            patch("asyncio.sleep", side_effect=stop_after_one),
        ):
            monitor._run_connection = fake_run_connection
            await monitor.run()

        assert monitor._connected is False
