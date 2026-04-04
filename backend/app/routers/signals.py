"""Trade signal endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from app.db import get_db
from app.limiter import limiter
from app.models import CopiedTrade, TradeSignal
from app.services import execution_service
from app.services.signal_generator import WhaleEvent, process_whale_event

router = APIRouter()


@router.get("/")
@limiter.limit("200/minute")
def list_signals(request: Request, limit: int = 50, status: str = None, db: Session = Depends(get_db)):
    """List recent trade signals."""
    q = db.query(TradeSignal).order_by(TradeSignal.created_at.desc())
    if status:
        q = q.filter(TradeSignal.status == status)
    return q.limit(limit).all()


@router.get("/pending")
def pending_signals(db: Session = Depends(get_db)):
    """Get signals awaiting execution."""
    signals = db.query(TradeSignal).filter(
        TradeSignal.status == "pending"
    ).order_by(TradeSignal.confidence.desc()).all()
    return signals


@router.post("/generate")
def generate_signals(event: WhaleEvent, db: Session = Depends(get_db)):
    """Manually trigger signal generation from a whale event (for testing)."""
    signals = process_whale_event(event, db)
    return {"generated": len(signals), "signal_ids": [s.id for s in signals]}


@router.post("/{signal_id}/execute")
async def execute_signal_endpoint(signal_id: int, db: Session = Depends(get_db)):
    """Manually trigger execution of a pending signal."""
    signal = db.query(TradeSignal).filter(TradeSignal.id == signal_id).first()
    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")
    if signal.status != "pending":
        raise HTTPException(status_code=400, detail=f"Signal is not pending (status={signal.status})")

    await execution_service.execute_signal(signal_id)

    # execute_signal commits via its own session; expire to force a fresh read
    db.expire_all()
    trade = (
        db.query(CopiedTrade)
        .filter(CopiedTrade.signal_id == signal_id)
        .order_by(CopiedTrade.id.desc())
        .first()
    )
    if trade is None:
        raise HTTPException(status_code=422, detail="Signal was skipped by risk limits or price validation")
    return trade


@router.post("/{signal_id}/dismiss")
def dismiss_signal(signal_id: int, db: Session = Depends(get_db)):
    """Dismiss a pending signal without executing it."""
    signal = db.query(TradeSignal).filter(TradeSignal.id == signal_id).first()
    if signal is None:
        raise HTTPException(status_code=404, detail="Signal not found")
    if signal.status != "pending":
        raise HTTPException(status_code=400, detail=f"Signal is not pending (status={signal.status})")

    signal.status = "dismissed"
    db.commit()
    db.refresh(signal)
    return signal
