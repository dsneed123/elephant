"""Tests for notification_service webhook dispatch."""

from unittest.mock import MagicMock, patch

import pytest

import app.services.notification_service as ns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_signal(
    market_ticker="NASDAQ-24DEC31",
    side="yes",
    action="buy",
    confidence=0.90,
    detected_price=45.0,
):
    sig = MagicMock()
    sig.market_ticker = market_ticker
    sig.side = side
    sig.action = action
    sig.confidence = confidence
    sig.detected_price = detected_price
    return sig


def _mock_trade(
    market_ticker="NASDAQ-24DEC31",
    side="yes",
    action="buy",
    contracts=10,
    price=0.45,
    cost=4.5,
    kalshi_order_id="sim-abc123",
    pnl=-1.50,
):
    trade = MagicMock()
    trade.market_ticker = market_ticker
    trade.side = side
    trade.action = action
    trade.contracts = contracts
    trade.price = price
    trade.cost = cost
    trade.kalshi_order_id = kalshi_order_id
    trade.pnl = pnl
    return trade


# ---------------------------------------------------------------------------
# _post_webhook
# ---------------------------------------------------------------------------

class TestPostWebhook:
    def test_no_op_when_disabled(self):
        with patch.object(ns.settings, "webhook_enabled", False), \
             patch.object(ns.settings, "webhook_url", "https://example.com/hook"), \
             patch("httpx.Client") as mock_client:
            ns._post_webhook({"embeds": []})
        mock_client.assert_not_called()

    def test_no_op_when_url_empty(self):
        with patch.object(ns.settings, "webhook_enabled", True), \
             patch.object(ns.settings, "webhook_url", ""), \
             patch("httpx.Client") as mock_client:
            ns._post_webhook({"embeds": []})
        mock_client.assert_not_called()

    def test_posts_when_enabled(self):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_http = MagicMock()
        mock_http.__enter__ = MagicMock(return_value=mock_http)
        mock_http.__exit__ = MagicMock(return_value=False)
        mock_http.post = MagicMock(return_value=mock_response)

        payload = {"embeds": [{"title": "Test"}]}
        with patch.object(ns.settings, "webhook_enabled", True), \
             patch.object(ns.settings, "webhook_url", "https://discord.com/api/webhooks/123/abc"), \
             patch("httpx.Client", return_value=mock_http):
            ns._post_webhook(payload)

        mock_http.post.assert_called_once_with(
            "https://discord.com/api/webhooks/123/abc", json=payload
        )
        mock_response.raise_for_status.assert_called_once()

    def test_swallows_http_errors(self):
        mock_http = MagicMock()
        mock_http.__enter__ = MagicMock(return_value=mock_http)
        mock_http.__exit__ = MagicMock(return_value=False)
        mock_http.post = MagicMock(side_effect=Exception("connection refused"))

        with patch.object(ns.settings, "webhook_enabled", True), \
             patch.object(ns.settings, "webhook_url", "https://discord.com/api/webhooks/123/abc"), \
             patch("httpx.Client", return_value=mock_http):
            # Should not raise
            ns._post_webhook({"embeds": []})


# ---------------------------------------------------------------------------
# notify_high_confidence_signal
# ---------------------------------------------------------------------------

class TestNotifyHighConfidenceSignal:
    def test_sends_correct_embed_fields(self):
        captured = {}

        def fake_post(payload):
            captured["payload"] = payload

        signal = _mock_signal(
            market_ticker="INFL-24DEC31",
            side="no",
            action="sell",
            confidence=0.92,
            detected_price=30.0,
        )
        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_high_confidence_signal(signal)

        embed = captured["payload"]["embeds"][0]
        assert embed["title"] == "High-Confidence Signal"
        fields = {f["name"]: f["value"] for f in embed["fields"]}
        assert fields["Market"] == "INFL-24DEC31"
        assert fields["Side / Action"] == "no / sell"
        assert "92.0%" in fields["Confidence"]
        assert "30¢" in fields["Price"]


# ---------------------------------------------------------------------------
# notify_trade_executed
# ---------------------------------------------------------------------------

class TestNotifyTradeExecuted:
    def test_paper_trade_title(self):
        captured = {}
        trade = _mock_trade()

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_trade_executed(trade, dry_run=True)

        assert captured["payload"]["embeds"][0]["title"] == "Paper Trade Executed"

    def test_live_trade_title(self):
        captured = {}
        trade = _mock_trade()

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_trade_executed(trade, dry_run=False)

        assert captured["payload"]["embeds"][0]["title"] == "Live Trade Executed"

    def test_embed_contains_cost_and_contracts(self):
        captured = {}
        trade = _mock_trade(contracts=5, price=0.60, cost=3.0, kalshi_order_id="order-xyz")

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_trade_executed(trade, dry_run=True)

        fields = {f["name"]: f["value"] for f in captured["payload"]["embeds"][0]["fields"]}
        assert fields["Contracts"] == "5"
        assert "$3.00" in fields["Cost"]
        assert "60¢" in fields["Price"]
        assert fields["Order ID"] == "order-xyz"

    def test_none_order_id_renders_dash(self):
        captured = {}
        trade = _mock_trade(kalshi_order_id=None)

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_trade_executed(trade, dry_run=True)

        fields = {f["name"]: f["value"] for f in captured["payload"]["embeds"][0]["fields"]}
        assert fields["Order ID"] == "—"


# ---------------------------------------------------------------------------
# notify_stop_loss
# ---------------------------------------------------------------------------

class TestNotifyStopLoss:
    def test_sends_stop_loss_embed(self):
        captured = {}
        trade = _mock_trade(market_ticker="FED-RATE", side="yes", pnl=-2.25, price=0.70)

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_stop_loss(trade)

        embed = captured["payload"]["embeds"][0]
        assert embed["title"] == "Stop-Loss Triggered"
        fields = {f["name"]: f["value"] for f in embed["fields"]}
        assert fields["Market"] == "FED-RATE"
        assert "$-2.25" in fields["PnL"]
        assert "70¢" in fields["Entry Price"]


# ---------------------------------------------------------------------------
# notify_daily_loss_warning
# ---------------------------------------------------------------------------

class TestNotifyDailyLossWarning:
    def test_sends_warning_embed(self):
        captured = {}

        def fake_post(payload):
            captured["payload"] = payload

        with patch.object(ns.settings, "max_daily_loss_pct", 0.10), \
             patch.object(ns, "_post_webhook", side_effect=fake_post):
            ns.notify_daily_loss_warning(daily_loss=85.0, portfolio_value=1000.0)

        embed = captured["payload"]["embeds"][0]
        assert embed["title"] == "Daily Loss Warning"
        assert "8.5%" in embed["description"]
        fields = {f["name"]: f["value"] for f in embed["fields"]}
        assert "$85.00" in fields["Daily Loss"]
        assert "$1000.00" in fields["Portfolio Value"]

    def test_zero_portfolio_does_not_raise(self):
        with patch.object(ns, "_post_webhook"):
            # Should not raise ZeroDivisionError
            ns.notify_daily_loss_warning(daily_loss=10.0, portfolio_value=0.0)
