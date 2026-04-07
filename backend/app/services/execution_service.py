"""Automatic signal execution via Kalshi API."""

import logging
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import settings
from app.models import CopiedTrade, PortfolioSnapshot, TradeSignal
from app.websocket_manager import broadcast_event

logger = logging.getLogger(__name__)


def _trade_payload(trade: CopiedTrade) -> dict:
    """Serialise a CopiedTrade to a JSON-safe dict for WebSocket broadcasts."""
    return {
        "id": trade.id,
        "signal_id": trade.signal_id,
        "market_ticker": trade.market_ticker,
        "side": trade.side,
        "action": trade.action,
        "contracts": trade.contracts,
        "price": trade.price,
        "cost": trade.cost,
        "kalshi_order_id": trade.kalshi_order_id,
        "status": trade.status,
        "is_simulated": trade.is_simulated,
        "pnl": trade.pnl,
        "created_at": trade.created_at.isoformat() if trade.created_at else None,
        "settled_at": trade.settled_at.isoformat() if trade.settled_at else None,
    }


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


def _check_risk_limits(db, signal: TradeSignal) -> str | None:
    """
    Check portfolio-level risk limits before placing an order.

    Returns a human-readable reason string if a limit is breached, None otherwise.
    Checks three guards:
      1. max_total_exposure_pct      — open CopiedTrade costs vs portfolio value.
      2. max_daily_loss_pct          — today's realized PnL loss vs portfolio value.
      3. max_per_trader_exposure_pct — open costs for this signal's trader vs portfolio value.
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

    # Guard 1: total open exposure (stopped_out trades are already closed)
    open_trades = (
        db.query(CopiedTrade)
        .filter(CopiedTrade.status.notin_(["settled", "cancelled", "stopped_out"]))
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
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
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

    # Guard 3: per-trader exposure (stopped_out trades are already closed)
    open_trader_trades = (
        db.query(CopiedTrade)
        .join(TradeSignal, CopiedTrade.signal_id == TradeSignal.id)
        .filter(
            TradeSignal.trader_id == signal.trader_id,
            CopiedTrade.status.notin_(["settled", "cancelled", "stopped_out"]),
        )
        .all()
    )
    trader_exposure = sum(t.cost for t in open_trader_trades)
    trader_limit = portfolio_value * settings.max_per_trader_exposure_pct
    if trader_exposure >= trader_limit:
        return (
            f"per-trader exposure for trader {signal.trader_id} "
            f"{trader_exposure:.2f} >= "
            f"{settings.max_per_trader_exposure_pct:.0%} limit ({trader_limit:.2f})"
        )

    # Guard 4: max drawdown from 30-day peak
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    peak_snapshot: PortfolioSnapshot | None = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.created_at >= thirty_days_ago)
        .order_by(PortfolioSnapshot.total_value.desc())
        .first()
    )
    if peak_snapshot is not None and peak_snapshot.total_value > 0:
        peak_value = peak_snapshot.total_value
        drawdown = (peak_value - portfolio_value) / peak_value
        if drawdown >= settings.max_drawdown_pct:
            return (
                f"max drawdown {drawdown:.1%} >= "
                f"{settings.max_drawdown_pct:.0%} limit "
                f"(peak={peak_value:.2f} current={portfolio_value:.2f})"
            )

    return None


def _maybe_notify_daily_loss_warning(db) -> None:
    """Fire a daily loss warning if today's loss has exceeded 80% of the daily limit."""
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
        return

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
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
    warning_threshold = loss_limit * 0.8

    if daily_pnl <= -warning_threshold:
        from app.services.notification_service import notify_daily_loss_warning
        notify_daily_loss_warning(-daily_pnl, portfolio_value)


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

        risk_error = _check_risk_limits(db, signal)
        if risk_error:
            signal.status = "skipped"
            db.commit()
            logger.warning("Signal %d skipped by risk limit: %s", signal_id, risk_error)
            return

        open_market_count = (
            db.query(CopiedTrade)
            .filter(
                CopiedTrade.market_ticker == signal.market_ticker,
                CopiedTrade.status.notin_(["settled", "cancelled"]),
            )
            .count()
        )
        if open_market_count >= settings.max_trades_per_market:
            signal.status = "skipped"
            db.commit()
            logger.warning(
                "Signal %d skipped: market %s already has %d open trade(s) "
                "(limit=%d)",
                signal_id,
                signal.market_ticker,
                open_market_count,
                settings.max_trades_per_market,
            )
            return

        _maybe_notify_daily_loss_warning(db)

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
    # Compute available paper balance: initial + settled PnL - cost of open simulated trades.
    # "stopped_out" trades are closed (the loss is realised), so exclude them from open costs
    # and include their PnL in the settled bucket.
    open_simulated = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.is_simulated.is_(True),
            CopiedTrade.status.notin_(["settled", "cancelled", "stopped_out"]),
        )
        .all()
    )
    settled_simulated = (
        db.query(CopiedTrade)
        .filter(
            CopiedTrade.is_simulated.is_(True),
            CopiedTrade.status.in_(["settled", "stopped_out"]),
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
    db.refresh(copied)

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

    broadcast_event({"type": "trade_updated", "payload": _trade_payload(copied)})
    from app.services.notification_service import notify_trade_executed
    notify_trade_executed(copied, dry_run=True)


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
    db.refresh(copied)

    logger.info(
        "Auto-executed signal %d: %s %s x%d @ %d¢ order_id=%s",
        signal.id,
        signal.market_ticker,
        signal.side,
        count,
        price_cents,
        order.get("order_id"),
    )

    broadcast_event({"type": "trade_updated", "payload": _trade_payload(copied)})
    from app.services.notification_service import notify_trade_executed
    notify_trade_executed(copied, dry_run=False)


def _get_exit_price_cents(market: dict, side: str) -> Optional[int]:
    """Extract the current exit (bid) price in cents from market data for the given side.

    Uses the bid price (what you'd receive closing the position) so the stop-loss
    threshold is evaluated against a realistic exit value.  Falls back to
    ``last_price`` when no bid is available.
    """
    if side == "yes":
        price = market.get("yes_bid") or market.get("last_price")
    else:
        price = market.get("no_bid") or market.get("last_price")
    if price is None:
        return None
    return int(price)


async def check_stop_losses(db) -> None:
    """Check open trades for stop-loss conditions and close them if the threshold is exceeded.

    For each open CopiedTrade, fetches the current market price via
    ``kalshi_client.get_market()``, computes unrealized PnL, and triggers a
    close when the loss ratio (−pnl / cost) exceeds ``settings.stop_loss_pct``.

    In dry-run mode the trade is marked ``stopped_out`` with the simulated loss
    and no real orders are placed.  In live mode the open order is cancelled
    first; if cancellation fails (order already filled) a closing sell order is
    placed instead.

    Opens its own DB session so it can be called directly from an APScheduler job.
    """
    import app.services.kalshi_client as _kalshi_mod

    open_trades = (
        db.query(CopiedTrade)
        .filter(CopiedTrade.status.notin_(["settled", "cancelled", "stopped_out"]))
        .all()
    )
    if not open_trades:
        return

    client = _kalshi_mod.get_kalshi_client()
    for trade in open_trades:
        try:
            await _check_trade_stop_loss(db, trade, client)
        except Exception:
            logger.exception(
                "Stop-loss check failed for trade %d (%s); will retry next run",
                trade.id,
                trade.market_ticker,
            )


async def _check_trade_stop_loss(db, trade: CopiedTrade, client) -> None:
    """Evaluate and apply stop-loss for a single open trade."""
    try:
        market = await client.get_market(trade.market_ticker)
    except Exception:
        logger.warning(
            "Could not fetch market %s for stop-loss check on trade %d; skipping",
            trade.market_ticker,
            trade.id,
        )
        return

    current_price_cents = _get_exit_price_cents(market, trade.side)
    if current_price_cents is None:
        return

    current_price = current_price_cents / 100
    entry_price = trade.price  # stored in dollars
    unrealized_pnl = trade.contracts * (current_price - entry_price)
    loss_ratio = (-unrealized_pnl / trade.cost) if trade.cost > 0 else 0.0

    if loss_ratio < settings.stop_loss_pct:
        return

    logger.warning(
        "Stop-loss triggered for trade %d (%s %s): entry=%.2f current=%.2f "
        "loss=%.1f%% threshold=%.1f%%",
        trade.id,
        trade.market_ticker,
        trade.side,
        entry_price,
        current_price,
        loss_ratio * 100,
        settings.stop_loss_pct * 100,
    )

    if settings.dry_run:
        trade.pnl = unrealized_pnl
        trade.status = "stopped_out"
        trade.settled_at = datetime.now(timezone.utc)
        db.commit()
        logger.info(
            "[DRY RUN] Stopped out simulated trade %d (%s): pnl=%.4f",
            trade.id,
            trade.market_ticker,
            unrealized_pnl,
        )
        broadcast_event({"type": "trade_updated", "payload": _trade_payload(trade)})
        from app.services.notification_service import notify_stop_loss
        notify_stop_loss(trade)
        _maybe_notify_daily_loss_warning(db)
    else:
        await _close_trade_live(db, trade, client, unrealized_pnl, current_price_cents)


async def _close_trade_live(
    db,
    trade: CopiedTrade,
    client,
    unrealized_pnl: float,
    current_price_cents: int,
) -> None:
    """Cancel or close a live trade that has hit its stop-loss."""
    cancelled = False
    try:
        await client.cancel_order(trade.kalshi_order_id)
        cancelled = True
        logger.info(
            "Cancelled order %s for stop-loss trade %d",
            trade.kalshi_order_id,
            trade.id,
        )
    except Exception:
        logger.warning(
            "Could not cancel order %s for trade %d (may already be filled); "
            "attempting closing sell order",
            trade.kalshi_order_id,
            trade.id,
        )

    if not cancelled:
        try:
            await client.place_order(
                ticker=trade.market_ticker,
                side=trade.side,
                count=trade.contracts,
                price=current_price_cents,
                action="sell",
            )
        except Exception:
            logger.exception(
                "Failed to place closing sell order for trade %d; manual intervention required",
                trade.id,
            )
            return

    trade.pnl = unrealized_pnl
    trade.status = "stopped_out"
    trade.settled_at = datetime.now(timezone.utc)
    db.commit()
    logger.info(
        "Stopped out live trade %d (%s): pnl=%.4f",
        trade.id,
        trade.market_ticker,
        unrealized_pnl,
    )
    broadcast_event({"type": "trade_updated", "payload": _trade_payload(trade)})
    from app.services.notification_service import notify_stop_loss
    notify_stop_loss(trade)
    _maybe_notify_daily_loss_warning(db)
