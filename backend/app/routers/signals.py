"""Trade signal endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from app.models import TradeSignal
from app.services.signal_generator import WhaleEvent, process_whale_event

router = APIRouter()


@router.get("/")
def list_signals(limit: int = 50, status: str = None, db: Session = Depends(get_db)):
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
