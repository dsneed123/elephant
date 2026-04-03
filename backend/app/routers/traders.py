"""Tracked traders endpoints."""

from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import TrackedTrader
from app.services.leaderboard_scraper import scraper

router = APIRouter()


@router.get("/")
def list_traders(db: Session = Depends(get_db)):
    """List all tracked traders, sorted by elephant score."""
    traders = db.query(TrackedTrader).filter(
        TrackedTrader.is_active == True
    ).order_by(TrackedTrader.elephant_score.desc()).all()
    return traders


@router.get("/top")
def top_traders(limit: int = 10, db: Session = Depends(get_db)):
    """Get top traders by elephant score."""
    traders = db.query(TrackedTrader).filter(
        TrackedTrader.is_active == True,
        TrackedTrader.elephant_score >= 80,
    ).order_by(TrackedTrader.elephant_score.desc()).limit(limit).all()
    return traders


@router.get("/{username}")
def get_trader(username: str, db: Session = Depends(get_db)):
    """Get a specific tracked trader."""
    trader = db.query(TrackedTrader).filter(
        TrackedTrader.kalshi_username == username
    ).first()
    if not trader:
        return {"error": "Trader not found"}
    return trader


@router.post("/scrape")
async def trigger_scrape(db: Session = Depends(get_db)):
    """Manually trigger a Kalshi leaderboard scrape."""
    count = await scraper.scrape(db)
    return {"scraped": count, "timestamp": datetime.utcnow().isoformat()}
