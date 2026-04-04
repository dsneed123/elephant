"""Tracked traders endpoints."""

import json
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.limiter import limiter
from app.models import TrackedTrader
from app.services.leaderboard_scraper import scraper

router = APIRouter()


class MarketsUpdate(BaseModel):
    markets: List[str]


class TraderUpdate(BaseModel):
    is_enabled: bool


@router.get("/")
@limiter.limit("200/minute")
def list_traders(request: Request, db: Session = Depends(get_db)):
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


@router.patch("/{trader_id}")
def update_trader(
    trader_id: int,
    payload: TraderUpdate,
    db: Session = Depends(get_db),
):
    """Toggle a trader's is_enabled flag."""
    trader = db.query(TrackedTrader).filter(TrackedTrader.id == trader_id).first()
    if not trader:
        raise HTTPException(status_code=404, detail="Trader not found")
    trader.is_enabled = payload.is_enabled
    db.commit()
    return trader


@router.patch("/{username}/markets")
def update_trader_markets(
    username: str,
    payload: MarketsUpdate,
    db: Session = Depends(get_db),
):
    """Manually override a trader's top_markets list."""
    trader = db.query(TrackedTrader).filter(
        TrackedTrader.kalshi_username == username
    ).first()
    if not trader:
        raise HTTPException(status_code=404, detail="Trader not found")
    trader.top_markets = json.dumps(payload.markets)
    db.commit()
    return trader


@router.post("/scrape")
@limiter.limit("60/minute")
async def trigger_scrape(request: Request, db: Session = Depends(get_db)):
    """Manually trigger a Kalshi leaderboard scrape."""
    count = await scraper.scrape(db)
    return {"scraped": count, "timestamp": datetime.utcnow().isoformat()}
