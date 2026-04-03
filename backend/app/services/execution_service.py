"""Automatic signal execution via Kalshi API."""

import logging
import math
import uuid
from datetime import datetime

from app.config import settings
from app.models import CopiedTrade, PortfolioSnapshot, TradeSignal

logger = logging.getLogger(__name__)


def _kelly_position_pct(win_rate: float, price: float, max_pct: float) -> float | None:
    """
    Compute half-Kelly position size as a fraction of portfolio.

    Returns None if the Kelly fraction is <= 0 (no positive edge).
    Caps at *max_pct* as a hard ceiling.

    Args:
        win_rate: Probability of winning in [0, 1].
        price:    Market price in dollars (e.g. 0.40 for 40¢).
        max_pct:  Hard cap as a fraction of portfolio.
    """
    b = 1.0 / price - 1.0  # net odds on a win
    kelly_f = (win_rate * b - (1.0 - win_rate)) / b
    half_kelly = kelly_f * 0.5
    if half_kelly <= 0:
        return None
    return min(half_kelly, max_pct)


def _check_risk_limits(db) -> str | None:
    """
    Check portfolio-level risk limits before placing an order.

    Returns a human-readable reason string if a limit is breached, None otherwise.
    Checks two guards:
      1. max_total_exposure_pct — open CopiedTrade costs vs portfolio value.
      2. max_daily_loss_pct    — today's realized PnL loss vs portfolio value.
    """
    # Reference portfolio value: latest snapshot or paper_balance_initial fallback
    latest_snapshot: PortfolioSnapshot | None = (
        db.query(PortfolioSnapshot)
        .order_by(PortfolioSnapshot.created_at.desc())
        .first()
    )
    portfolio_value = (
        latest_snapshot.total_value
        if latest_snapshot is not None
        else settings.paper_balance_initial
    )
    if portfolio_value <= 0:
        return None

    # Guard 1: total open exposure
    open_trades = (
        db.query(CopiedTrade)
        .filter(CopiedTrade.status.notin_(["settled", "cancelled"]))
        .all()
    )
    total_exposure = sum(t.cost for t in open_trades)
    exposure_limit = portfolio_value * settings.max_total_exposure_pct
    if total_exposure >= exposure_limit:
        return (
            f"total open exposure {total_exposure:.2f} >= "
            f"{settings.max_total_exposure_pct:.0%} limit ({exposure_limit:.2f})"
        )

    # Guard 2: daily realized loss
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    settled_today = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.settled_at >= today_start,
            CopiedTrade.pnl.isnot(None),
        )
        .all()
    )
    daily_pnl = sum(t.pnl for t in settled_today)
    loss_limit = portfolio_value * settings.max_daily_loss_pct
    if daily_pnl <= -loss_limit:
        return (
            f"daily loss {-daily_pnl:.2f} >= "
            f"{settings.max_daily_loss_pct:.0%} limit ({loss_limit:.2f})"
        )

    return None


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

        risk_error = _check_risk_limits(db)
        if risk_error:
            signal.status = "skipped"
            db.commit()
            logger.warning("Signal %d skipped by risk limit: %s", signal_id, risk_error)
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

    win_rate = signal.trader.win_rate if signal.trader else None
    if win_rate is not None:
        position_pct = _kelly_position_pct(win_rate, price_cents / 100, settings.max_position_pct)
        if position_pct is None:
            signal.status = "skipped"
            db.commit()
            logger.info(
                "[DRY RUN] Signal %d skipped: negative Kelly edge "
                "(win_rate=%.2f, price=%d¢)",
                signal.id,
                win_rate,
                price_cents,
            )
            return
    else:
        position_pct = settings.max_position_pct

    max_spend = paper_balance * position_pct
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
        "[DRY RUN] Simulated signal %d: %s %s x%d @ %d¢ "
        "position_pct=%.3f paper_order_id=%s paper_balance=%.2f",
        signal.id,
        signal.market_ticker,
        signal.side,
        count,
        price_cents,
        position_pct,
        fake_order_id,
        paper_balance,
    )


async def _execute_real(db, signal: TradeSignal, price_cents: int) -> None:
    """Place a live order via the Kalshi API."""
    from app.services.kalshi_client import get_kalshi_client

    client = get_kalshi_client()
    balance = await client.get_portfolio_balance()

    win_rate = signal.trader.win_rate if signal.trader else None
    if win_rate is not None:
        position_pct = _kelly_position_pct(win_rate, price_cents / 100, settings.max_position_pct)
        if position_pct is None:
            signal.status = "skipped"
            db.commit()
            logger.info(
                "Signal %d skipped: negative Kelly edge (win_rate=%.2f, price=%d¢)",
                signal.id,
                win_rate,
                price_cents,
            )
            return
    else:
        position_pct = settings.max_position_pct

    max_spend = balance * position_pct
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
