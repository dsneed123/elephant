"""Automatic signal execution via Kalshi API."""

import logging
import math

from app.config import settings
from app.models import CopiedTrade, TradeSignal

logger = logging.getLogger(__name__)


async def execute_signal(signal_id: int) -> None:
    """
    Auto-execute a pending trade signal: place a Kalshi order and record a CopiedTrade.

    Opens its own DB session since the caller's session may be closed by the time
    this coroutine runs as a scheduled task.
    """
    from app.db import SessionLocal
    from app.services.kalshi_client import get_kalshi_client

    db = SessionLocal()
    try:
        signal: TradeSignal | None = (
            db.query(TradeSignal).filter(TradeSignal.id == signal_id).first()
        )
        if signal is None or signal.status != "pending":
            return

        price_cents = int(signal.detected_price) if signal.detected_price else None
        if price_cents is None or not (1 <= price_cents <= 99):
            logger.warning(
                "Signal %d has no valid price (%s); skipping auto-execute",
                signal_id,
                signal.detected_price,
            )
            return

        client = get_kalshi_client()
        balance = await client.get_portfolio_balance()
        max_spend = balance * settings.max_position_pct
        count = max(1, math.floor(max_spend / (price_cents / 100)))

        order = await client.place_order(
            ticker=signal.market_ticker,
            side=signal.side,
            count=count,
            price=price_cents,
        )

        cost = count * (price_cents / 100)
        copied = CopiedTrade(
            signal_id=signal.id,
            market_ticker=signal.market_ticker,
            side=signal.side,
            action=signal.action,
            contracts=count,
            price=price_cents / 100,
            cost=cost,
            kalshi_order_id=order.get("order_id"),
            status="pending",
        )
        db.add(copied)
        signal.status = "copied"
        db.commit()

        logger.info(
            "Auto-executed signal %d: %s %s x%d @ %d¢ order_id=%s",
            signal_id,
            signal.market_ticker,
            signal.side,
            count,
            price_cents,
            order.get("order_id"),
        )
    except Exception:
        logger.exception("Failed to auto-execute signal %d", signal_id)
    finally:
        db.close()
