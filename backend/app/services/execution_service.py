"""Automatic signal execution via Kalshi API."""

import logging
import math
import uuid

from app.config import settings
from app.models import CopiedTrade, TradeSignal

logger = logging.getLogger(__name__)


async def execute_signal(signal_id: int) -> None:
    """
    Auto-execute a pending trade signal: place a Kalshi order and record a CopiedTrade.

    When settings.dry_run is True, simulates the order at the detected market price
    without calling the Kalshi API. The resulting CopiedTrade is marked is_simulated=True
    with status="simulated" and a fake order ID (sim-<hex>).

    Opens its own DB session since the caller's session may be closed by the time
    this coroutine runs as a scheduled task.
    """
    from app.db import SessionLocal

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

        if settings.dry_run:
            await _execute_simulated(db, signal, price_cents)
        else:
            await _execute_real(db, signal, price_cents)
    except Exception:
        logger.exception("Failed to auto-execute signal %d", signal_id)
    finally:
        db.close()


async def _execute_simulated(db, signal: TradeSignal, price_cents: int) -> None:
    """Simulate order execution for dry-run / paper trading mode."""
    # Compute available paper balance: initial + settled PnL - cost of open simulated trades
    open_simulated = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.is_simulated.is_(True),
            CopiedTrade.status.notin_(["settled", "cancelled"]),
        )
        .all()
    )
    settled_simulated = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.is_simulated.is_(True),
            CopiedTrade.status == "settled",
            CopiedTrade.pnl.isnot(None),
        )
        .all()
    )

    settled_pnl = sum(t.pnl for t in settled_simulated if t.pnl is not None)
    open_costs = sum(t.cost for t in open_simulated)
    paper_balance = settings.paper_balance_initial + settled_pnl - open_costs

    max_spend = paper_balance * settings.max_position_pct
    count = max(1, math.floor(max_spend / (price_cents / 100)))
    fake_order_id = f"sim-{uuid.uuid4().hex[:12]}"
    cost = count * (price_cents / 100)

    copied = CopiedTrade(
        signal_id=signal.id,
        market_ticker=signal.market_ticker,
        side=signal.side,
        action=signal.action,
        contracts=count,
        price=price_cents / 100,
        cost=cost,
        kalshi_order_id=fake_order_id,
        status="simulated",
        is_simulated=True,
    )
    db.add(copied)
    signal.status = "copied"
    db.commit()

    logger.info(
        "[DRY RUN] Simulated signal %d: %s %s x%d @ %d¢ paper_order_id=%s paper_balance=%.2f",
        signal.id,
        signal.market_ticker,
        signal.side,
        count,
        price_cents,
        fake_order_id,
        paper_balance,
    )


async def _execute_real(db, signal: TradeSignal, price_cents: int) -> None:
    """Place a live order via the Kalshi API."""
    from app.services.kalshi_client import get_kalshi_client

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
        is_simulated=False,
    )
    db.add(copied)
    signal.status = "copied"
    db.commit()

    logger.info(
        "Auto-executed signal %d: %s %s x%d @ %d¢ order_id=%s",
        signal.id,
        signal.market_ticker,
        signal.side,
        count,
        price_cents,
        order.get("order_id"),
    )
