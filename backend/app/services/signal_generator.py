"""Signal generation from order book whale events."""

import asyncio
import json
import logging
from datetime import datetime, timedelta

from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import settings
from app.models import TrackedTrader, TradeSignal

logger = logging.getLogger(__name__)


class WhaleEvent(BaseModel):
    """An order book whale detection event."""
    market_ticker: str
    side: str    # "yes" or "no" — which contract was traded
    action: str  # "buy" or "sell"
    order_size: float  # estimated order value in USD


def _compute_confidence(elephant_score: float, order_size: float) -> float:
    """Compute signal confidence: base 0.5 + score component + size component, capped at 0.95."""
    raw = 0.5 + (elephant_score / 100) * 0.3 + (order_size / 10_000) * 0.2
    return min(raw, 0.95)


def _trader_tracks_market(trader: TrackedTrader, ticker: str) -> bool:
    """Return True if the trader's top_markets JSON list includes ticker."""
    if not trader.top_markets:
        return False
    try:
        markets = json.loads(trader.top_markets)
        return ticker in markets
    except (json.JSONDecodeError, TypeError):
        return False


def expire_stale_signals(db: Session) -> int:
    """Bulk-update pending signals older than signal_ttl_minutes to 'expired'."""
    cutoff = datetime.utcnow() - timedelta(minutes=settings.signal_ttl_minutes)
    updated = (
        db.query(TradeSignal)
        .filter(TradeSignal.status == "pending", TradeSignal.created_at < cutoff)
        .update({"status": "expired"}, synchronize_session=False)
    )
    db.commit()
    if updated:
        logger.info(
            "Expired %d stale signals (older than %d minutes)",
            updated,
            settings.signal_ttl_minutes,
        )
    return updated


def process_whale_event(event: WhaleEvent, db: Session) -> list[TradeSignal]:
    """
    Consume a whale_detected event and produce TradeSignal rows.

    For each active TrackedTrader whose top_markets includes the event's
    market_ticker and whose elephant_score meets the minimum threshold,
    compute a confidence score and write a pending TradeSignal if the
    confidence also clears the minimum.

    Returns the list of TradeSignal rows committed to the DB.
    """
    created: list[TradeSignal] = []

    candidates = (
        db.query(TrackedTrader)
        .filter(
            TrackedTrader.is_active == True,  # noqa: E712
            TrackedTrader.elephant_score >= settings.min_elephant_score,
        )
        .all()
    )

    for trader in candidates:
        if not _trader_tracks_market(trader, event.market_ticker):
            continue

        confidence = _compute_confidence(trader.elephant_score, event.order_size)

        if confidence < settings.min_signal_confidence:
            logger.debug(
                "Skipping signal for trader %s on %s: confidence %.3f below threshold %.3f",
                trader.kalshi_username,
                event.market_ticker,
                confidence,
                settings.min_signal_confidence,
            )
            continue

        signal = TradeSignal(
            trader_id=trader.id,
            market_ticker=event.market_ticker,
            side=event.side,
            action=event.action,
            detected_volume=event.order_size,
            confidence=confidence,
            status="pending",
        )
        db.add(signal)
        created.append(signal)
        logger.info(
            "Signal created: trader=%s market=%s side=%s action=%s confidence=%.3f",
            trader.kalshi_username,
            event.market_ticker,
            event.side,
            event.action,
            confidence,
        )

    db.commit()
    for sig in created:
        db.refresh(sig)

    for sig in created:
        if sig.confidence >= settings.auto_execute_threshold:
            try:
                from app.services.execution_service import execute_signal
                loop = asyncio.get_running_loop()
                loop.create_task(execute_signal(sig.id))
                logger.info(
                    "Scheduled auto-execution for signal %d (confidence=%.3f)",
                    sig.id,
                    sig.confidence,
                )
            except RuntimeError:
                logger.warning(
                    "No running event loop; signal %d left pending for manual review",
                    sig.id,
                )

    return created
