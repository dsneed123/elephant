"""Trade settlement and PnL reconciliation service."""

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from app.models import CopiedTrade

logger = logging.getLogger(__name__)


async def settle_open_trades(db: Session) -> int:
    """
    Settle open copied trades by fetching their order status from Kalshi.

    Queries all CopiedTrades where pnl IS NULL, a kalshi_order_id exists, and
    the trade is not already settled or cancelled. For each trade, fetches the
    order from Kalshi; if the market has resolved (close_price present), computes
    realized PnL and marks the trade settled.

    Returns the number of trades settled in this run.

    PnL formula (prices in cents, result in dollars):
      YES: pnl = (close_price - fill_price) * contracts / 100
      NO:  pnl = ((100 - close_price) - fill_price) * contracts / 100
    """
    from app.services.kalshi_client import get_kalshi_client

    trades = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.pnl.is_(None),
            CopiedTrade.kalshi_order_id.isnot(None),
            CopiedTrade.status.notin_(["settled", "cancelled"]),
        )
        .all()
    )

    if not trades:
        logger.debug("settle_open_trades: no eligible trades found")
        return 0

    client = get_kalshi_client()
    settled_count = 0

    for trade in trades:
        try:
            order = await client.get_order(trade.kalshi_order_id)
        except Exception:
            logger.exception(
                "Failed to fetch order %s for trade %d; will retry next run",
                trade.kalshi_order_id,
                trade.id,
            )
            continue

        order_status = order.get("status", "")

        if order_status == "cancelled":
            trade.status = "cancelled"
            db.commit()
            logger.info(
                "Trade %d cancelled (order %s)", trade.id, trade.kalshi_order_id
            )
            continue

        # close_price is set by Kalshi once the market resolves.
        # For binary YES/NO markets: 100 = YES wins, 0 = NO wins.
        close_price = order.get("close_price")
        if close_price is None:
            # Market not yet resolved; revisit on the next scheduled run.
            continue

        # Prefer the actual fill price from the order; fall back to the price
        # recorded at execution time (stored in dollars, so convert to cents).
        if trade.side == "yes":
            fill_price_cents = order.get("yes_price")
        else:
            fill_price_cents = order.get("no_price")

        if fill_price_cents is None:
            fill_price_cents = trade.price * 100  # trade.price is in dollars

        filled_count = order.get("filled_count") or trade.contracts

        if trade.side == "yes":
            pnl = (close_price - fill_price_cents) * filled_count / 100
        else:
            pnl = ((100 - close_price) - fill_price_cents) * filled_count / 100

        trade.pnl = pnl
        trade.status = "settled"
        trade.settled_at = datetime.utcnow()
        db.commit()
        settled_count += 1

        logger.info(
            "Settled trade %d: %s %s close_price=%d fill=%d¢ pnl=%.4f",
            trade.id,
            trade.market_ticker,
            trade.side,
            close_price,
            fill_price_cents,
            pnl,
        )

    logger.info("settle_open_trades: settled %d / %d trade(s)", settled_count, len(trades))
    return settled_count
