"""Signal generation from order book whale events."""

import json
import logging
import math
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import settings
from app.models import TrackedTrader, TradeSignal
from app.websocket_manager import broadcast_event

logger = logging.getLogger(__name__)


class WhaleEvent(BaseModel):
    """An order book whale detection event."""
    market_ticker: str
    side: str    # "yes" or "no" — which contract was traded
    action: str  # "buy" or "sell"
    order_size: float  # estimated order value in USD
    price: float  # price in cents (1–99) from the orderbook delta
    market_title: str | None = None  # human-readable market title, if resolved


def _compute_confidence(elephant_score: float, order_size: float, win_rate: float) -> float:
    """Compute signal confidence from win rate, elephant score, and order size, capped at 0.95.

    Components (sum to 1.0):
      - win_rate                               weighted 40%
      - elephant_score / 100                   weighted 35%
      - log10(order_size) / log10(50_000)      weighted 25%
    """
    log_size = math.log10(max(order_size, 1)) / math.log10(50_000)
    raw = win_rate * 0.40 + (elephant_score / 100) * 0.35 + log_size * 0.25
    return min(raw, 0.95)


def _trader_tracks_market(trader: TrackedTrader, ticker: str) -> bool:
    """Return True if the trader's top_markets JSON list includes ticker.

    A None or empty top_markets means the trader has no market filter yet,
    so they are treated as tracking all markets.
    """
    if not trader.top_markets:
        return True  # No market data populated — include for all markets
    try:
        markets = json.loads(trader.top_markets)
        if not markets:
            return True  # Empty list — treat as tracking all markets
        return ticker in markets
    except (json.JSONDecodeError, TypeError):
        return True  # Malformed JSON — don't exclude trader


def expire_stale_signals(db: Session) -> int:
    """Bulk-update pending signals older than signal_ttl_minutes to 'expired'."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.signal_ttl_minutes)
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

        confidence = _compute_confidence(trader.elephant_score, event.order_size, trader.win_rate)

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
            market_title=event.market_title,
            side=event.side,
            action=event.action,
            detected_volume=event.order_size,
            detected_price=event.price,
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
        broadcast_event({
            "type": "signal_created",
            "payload": {
                "id": sig.id,
                "trader_id": sig.trader_id,
                "market_ticker": sig.market_ticker,
                "market_title": sig.market_title,
                "side": sig.side,
                "action": sig.action,
                "detected_price": sig.detected_price,
                "detected_volume": sig.detected_volume,
                "confidence": sig.confidence,
                "status": sig.status,
                "created_at": sig.created_at.isoformat() if sig.created_at else None,
            },
        })

    for sig in created:
        if sig.confidence >= settings.auto_execute_threshold:
            from app.main import scheduler
            from app.services.execution_service import execute_signal
            from app.services.notification_service import notify_high_confidence_signal
            scheduler.add_job(execute_signal, trigger="date", args=[sig.id])
            notify_high_confidence_signal(sig)
            logger.info(
                "Scheduled auto-execution for signal %d (confidence=%.3f)",
                sig.id,
                sig.confidence,
            )

    return created
