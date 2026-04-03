"""Trade settlement and PnL reconciliation service."""

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models import CopiedTrade

logger = logging.getLogger(__name__)


async def settle_open_trades(db: Session) -> int:
    """
    Settle open copied trades by fetching their order status from Kalshi.

    For real trades (is_simulated=False): fetches the order from Kalshi; if the
    market has resolved (close_price present), computes realized PnL and marks
    the trade settled.

    For simulated trades (is_simulated=True): fetches market data via get_market()
    to check resolution. When the market result is available, computes PnL using
    the same formula and marks the trade settled.

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
            CopiedTrade.status.notin_(["settled", "cancelled", "partial"]),
        )
        .all()
    )

    if not trades:
        logger.debug("settle_open_trades: no eligible trades found")
        return 0

    client = get_kalshi_client()
    settled_count = 0

    for trade in trades:
        if trade.is_simulated:
            settled_count += await _settle_simulated(db, trade, client)
        else:
            settled_count += await _settle_real(db, trade, client)

    logger.info("settle_open_trades: settled %d / %d trade(s)", settled_count, len(trades))
    return settled_count


async def _settle_simulated(db: Session, trade: CopiedTrade, client) -> int:
    """Settle a simulated (dry-run) trade by checking market resolution via get_market()."""
    try:
        market = await client.get_market(trade.market_ticker)
    except Exception:
        logger.exception(
            "Failed to fetch market %s for simulated trade %d; will retry next run",
            trade.market_ticker,
            trade.id,
        )
        return 0

    result = market.get("result")
    if result is None:
        # Market not yet resolved; revisit on the next scheduled run.
        return 0

    # Convert market result to close_price used in the standard PnL formula.
    close_price = 100 if result == "yes" else 0
    fill_price_cents = trade.price * 100  # trade.price stored in dollars at execution time

    if trade.side == "yes":
        pnl = (close_price - fill_price_cents) * trade.contracts / 100
    else:
        pnl = ((100 - close_price) - fill_price_cents) * trade.contracts / 100

    trade.pnl = pnl
    trade.status = "settled"
    trade.settled_at = datetime.now(timezone.utc)
    db.commit()

    logger.info(
        "[DRY RUN] Settled simulated trade %d: %s %s result=%s fill=%d¢ pnl=%.4f",
        trade.id,
        trade.market_ticker,
        trade.side,
        result,
        int(fill_price_cents),
        pnl,
    )
    return 1


async def poll_open_orders(db: Session) -> int:
    """
    Poll Kalshi for the current fill status of all pending orders.

    Queries all CopiedTrade rows with status='pending' and a non-null
    kalshi_order_id, fetches each order from Kalshi, and transitions the
    local status accordingly:

      Kalshi status      → local status
      ──────────────────────────────────
      resting            → pending (no change)
      filled             → filled
      partially_filled   → partial  (contracts and cost updated to filled qty)
      cancelled          → cancelled

    For partially-filled orders, trade.contracts is updated to the actual
    filled quantity and trade.cost is recalculated as filled_count * trade.price.

    Returns the number of trades whose status changed in this run.
    """
    from app.services.kalshi_client import get_kalshi_client

    trades = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.status == "pending",
            CopiedTrade.kalshi_order_id.isnot(None),
        )
        .all()
    )

    if not trades:
        logger.debug("poll_open_orders: no pending trades with order IDs")
        return 0

    client = get_kalshi_client()
    updated_count = 0

    for trade in trades:
        try:
            order = await client.get_order(trade.kalshi_order_id)
        except Exception:
            logger.exception(
                "poll_open_orders: failed to fetch order %s for trade %d; skipping",
                trade.kalshi_order_id,
                trade.id,
            )
            continue

        order_status = order.get("status", "")

        if order_status == "resting":
            # Still sitting on the book; nothing to update.
            continue

        elif order_status == "cancelled":
            trade.status = "cancelled"
            db.commit()
            logger.info(
                "poll_open_orders: trade %d order %s cancelled",
                trade.id,
                trade.kalshi_order_id,
            )
            updated_count += 1

        elif order_status == "filled":
            trade.status = "filled"
            db.commit()
            logger.info(
                "poll_open_orders: trade %d order %s fully filled",
                trade.id,
                trade.kalshi_order_id,
            )
            updated_count += 1

        elif order_status == "partially_filled":
            filled_count = order.get("filled_count")
            if filled_count is not None and filled_count > 0:
                trade.contracts = filled_count
                trade.cost = filled_count * trade.price
                trade.status = "partial"
                db.commit()
                logger.info(
                    "poll_open_orders: trade %d order %s partially filled (%d contracts, cost=%.4f)",
                    trade.id,
                    trade.kalshi_order_id,
                    filled_count,
                    trade.cost,
                )
                updated_count += 1

    logger.info(
        "poll_open_orders: updated %d / %d pending trade(s)", updated_count, len(trades)
    )
    return updated_count


async def _settle_real(db: Session, trade: CopiedTrade, client) -> int:
    """Settle a live trade by fetching its order status from Kalshi."""
    try:
        order = await client.get_order(trade.kalshi_order_id)
    except Exception:
        logger.exception(
            "Failed to fetch order %s for trade %d; will retry next run",
            trade.kalshi_order_id,
            trade.id,
        )
        return 0

    order_status = order.get("status", "")

    if order_status == "cancelled":
        trade.status = "cancelled"
        db.commit()
        logger.info(
            "Trade %d cancelled (order %s)", trade.id, trade.kalshi_order_id
        )
        return 0

    # close_price is set by Kalshi once the market resolves.
    # For binary YES/NO markets: 100 = YES wins, 0 = NO wins.
    close_price = order.get("close_price")
    if close_price is None:
        # Market not yet resolved; revisit on the next scheduled run.
        return 0

    # Prefer the actual fill price from the order; fall back to the price
    # recorded at execution time (stored in dollars, so convert to cents).
    if trade.side == "yes":
        fill_price_cents = order.get("yes_price")
    else:
        fill_price_cents = order.get("no_price")

    if fill_price_cents is None:
        fill_price_cents = trade.price * 100  # trade.price is in dollars

    raw_filled = order.get("filled_count")
    if raw_filled is not None and raw_filled == 0:
        # Zero fills: treat as cancelled.
        trade.status = "cancelled"
        db.commit()
        logger.info(
            "Trade %d cancelled (zero fills, order %s)", trade.id, trade.kalshi_order_id
        )
        return 0

    originally_requested = trade.contracts
    filled_count = raw_filled if raw_filled is not None else trade.contracts

    # Always update trade.contracts to the actual filled amount.
    trade.contracts = filled_count

    if trade.side == "yes":
        pnl = (close_price - fill_price_cents) * filled_count / 100
    else:
        pnl = ((100 - close_price) - fill_price_cents) * filled_count / 100

    trade.pnl = pnl
    trade.status = "partial" if filled_count < originally_requested else "settled"
    trade.settled_at = datetime.now(timezone.utc)
    db.commit()

    logger.info(
        "Settled trade %d (%s): %s %s close_price=%d fill=%d¢ filled=%d/%d pnl=%.4f",
        trade.id,
        trade.status,
        trade.market_ticker,
        trade.side,
        close_price,
        fill_price_cents,
        filled_count,
        originally_requested,
        pnl,
    )
    return 1
