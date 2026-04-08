"""Webhook notification service for signal and risk events."""

import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


def _post_webhook(payload: dict) -> None:
    """POST a Discord-compatible webhook payload. Silently logs errors on failure."""
    if not settings.webhook_enabled or not settings.webhook_url:
        return
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.post(settings.webhook_url, json=payload)
            resp.raise_for_status()
    except Exception:
        logger.warning("Webhook POST failed", exc_info=True)


def notify_high_confidence_signal(signal) -> None:
    """Notify when a signal with confidence >= auto_execute_threshold is created."""
    _post_webhook({
        "embeds": [{
            "title": "High-Confidence Signal",
            "color": 0x5865F2,
            "fields": [
                {"name": "Market", "value": signal.market_ticker, "inline": True},
                {"name": "Side / Action", "value": f"{signal.side} / {signal.action}", "inline": True},
                {"name": "Confidence", "value": f"{signal.confidence:.1%}", "inline": True},
                {"name": "Price", "value": f"{int(signal.detected_price)}¢", "inline": True},
            ],
        }]
    })


def notify_trade_executed(trade, *, dry_run: bool) -> None:
    """Notify after a trade is successfully placed (paper or live)."""
    title = "Paper Trade Executed" if dry_run else "Live Trade Executed"
    color = 0x57F287
    _post_webhook({
        "embeds": [{
            "title": title,
            "color": color,
            "fields": [
                {"name": "Market", "value": trade.market_ticker, "inline": True},
                {"name": "Side / Action", "value": f"{trade.side} / {trade.action}", "inline": True},
                {"name": "Contracts", "value": str(trade.contracts), "inline": True},
                {"name": "Price", "value": f"{trade.price * 100:.0f}¢", "inline": True},
                {"name": "Cost", "value": f"${trade.cost:.2f}", "inline": True},
                {"name": "Order ID", "value": trade.kalshi_order_id or "—", "inline": True},
            ],
        }]
    })


def notify_stop_loss(trade) -> None:
    """Notify when a stop-loss closes a position."""
    _post_webhook({
        "embeds": [{
            "title": "Stop-Loss Triggered",
            "color": 0xED4245,
            "fields": [
                {"name": "Market", "value": trade.market_ticker, "inline": True},
                {"name": "Side", "value": trade.side, "inline": True},
                {"name": "PnL", "value": f"${trade.pnl:.2f}", "inline": True},
                {"name": "Entry Price", "value": f"{trade.price * 100:.0f}¢", "inline": True},
            ],
        }]
    })


def notify_gap_alerts(gaps: list) -> None:
    """Post a pre-market Gap Alert embed for stocks gapping significantly."""
    if not gaps:
        return
    fields = []
    for gap in gaps:
        direction = "UP" if gap.gap_pct > 0 else "DOWN"
        fields.append({
            "name": f"{gap.symbol}  {direction}",
            "value": (
                f"Prev close: ${gap.prev_close:.2f}\n"
                f"Pre-market: ${gap.premarket_price:.2f}\n"
                f"Gap: {gap.gap_pct:+.1%}"
            ),
            "inline": True,
        })
    _post_webhook({
        "embeds": [{
            "title": "Pre-Market Gap Alert",
            "color": 0xFFA500,
            "fields": fields,
        }]
    })


def notify_earnings_watch(earnings: dict) -> None:
    """Post a weekly Earnings Watch embed listing watchlist stocks reporting this week."""
    if not earnings:
        return
    fields = [
        {"name": symbol, "value": str(date), "inline": True}
        for symbol, date in sorted(earnings.items())
    ]
    _post_webhook({
        "embeds": [{
            "title": "Earnings Watch — This Week",
            "color": 0xFEE75C,
            "description": (
                "Watchlist stocks with earnings this week. "
                "Swing signals are suppressed within 2 days of earnings."
            ),
            "fields": fields,
        }]
    })


def notify_daily_loss_warning(daily_loss: float, portfolio_value: float) -> None:
    """Notify when daily loss exceeds 80% of max_daily_loss_pct."""
    loss_pct = daily_loss / portfolio_value if portfolio_value > 0 else 0.0
    _post_webhook({
        "embeds": [{
            "title": "Daily Loss Warning",
            "color": 0xFEE75C,
            "description": (
                f"Daily loss has reached **{loss_pct:.1%}** of portfolio value — "
                f"approaching the {settings.max_daily_loss_pct:.0%} limit."
            ),
            "fields": [
                {"name": "Daily Loss", "value": f"${daily_loss:.2f}", "inline": True},
                {"name": "Portfolio Value", "value": f"${portfolio_value:.2f}", "inline": True},
                {"name": "Limit", "value": f"{settings.max_daily_loss_pct:.0%}", "inline": True},
            ],
        }]
    })
