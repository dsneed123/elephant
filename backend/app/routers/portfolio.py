"""Portfolio and copy-trading endpoints."""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.config import settings
from app.db import get_db
from app.models import CopiedTrade, PortfolioSnapshot, TradeSignal, TrackedTrader
from app.services.kalshi_client import get_kalshi_client

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/trades")
def list_copied_trades(limit: int = 50, db: Session = Depends(get_db)):
    """List recently copied trades."""
    trades = db.query(CopiedTrade).order_by(
        CopiedTrade.created_at.desc()
    ).limit(limit).all()
    return trades


@router.get("/performance")
async def portfolio_performance(db: Session = Depends(get_db)):
    """Get portfolio performance summary."""
    latest = db.query(PortfolioSnapshot).order_by(
        PortfolioSnapshot.created_at.desc()
    ).first()

    if settings.dry_run:
        all_simulated = (
            db.query(CopiedTrade)
            .filter(CopiedTrade.is_simulated.is_(True))
            .all()
        )
        total_trades = len(all_simulated)
        settled = [t for t in all_simulated if t.status == "settled" and t.pnl is not None]
        winning = sum(1 for t in settled if t.pnl > 0)
        total_pnl = sum(t.pnl for t in settled)
        open_costs = sum(
            t.cost for t in all_simulated if t.status not in ("settled", "cancelled")
        )
        balance = settings.paper_balance_initial + total_pnl - open_costs
    else:
        total_trades = db.query(CopiedTrade).filter(CopiedTrade.is_simulated.is_(False)).count()
        winning = db.query(CopiedTrade).filter(
            CopiedTrade.is_simulated.is_(False), CopiedTrade.pnl > 0
        ).count()
        total_pnl = sum(
            t.pnl for t in db.query(CopiedTrade).filter(
                CopiedTrade.is_simulated.is_(False),
                CopiedTrade.status == "settled",
            ).all()
            if t.pnl is not None
        )
        try:
            balance = await get_kalshi_client().get_portfolio_balance()
        except Exception as exc:
            logger.warning("Could not fetch Kalshi balance: %s", exc)
            balance = latest.balance if latest else 0

    return {
        "mode": "paper" if settings.dry_run else "live",
        "balance": balance,
        "total_value": latest.total_value if latest else balance,
        "total_pnl": total_pnl,
        "total_trades": total_trades,
        "win_rate": winning / total_trades if total_trades > 0 else 0,
    }


@router.get("/traders")
def trader_pnl_attribution(db: Session = Depends(get_db)):
    """Return settled P&L grouped by the copied trader, sorted by total_pnl desc."""
    rows = (
        db.query(CopiedTrade, TrackedTrader)
        .join(TradeSignal, CopiedTrade.signal_id == TradeSignal.id)
        .join(TrackedTrader, TradeSignal.trader_id == TrackedTrader.id)
        .filter(CopiedTrade.status == "settled", CopiedTrade.pnl.isnot(None))
        .all()
    )

    by_trader: dict[str, dict] = {}
    for trade, trader in rows:
        entry = by_trader.setdefault(
            trader.kalshi_username,
            {
                "kalshi_username": trader.kalshi_username,
                "display_name": trader.display_name,
                "elephant_score": trader.elephant_score,
                "tier": trader.tier,
                "_trades": [],
            },
        )
        entry["_trades"].append(trade)

    result = []
    for info in by_trader.values():
        trades = info.pop("_trades")
        trade_count = len(trades)
        total_pnl = sum(t.pnl for t in trades)
        total_cost = sum(t.cost for t in trades)
        winners = sum(1 for t in trades if t.pnl > 0)
        result.append(
            {
                **info,
                "total_pnl": total_pnl,
                "win_rate": winners / trade_count,
                "trade_count": trade_count,
                "total_cost": total_cost,
                "roi": total_pnl / total_cost if total_cost > 0 else 0.0,
            }
        )

    result.sort(key=lambda x: x["total_pnl"], reverse=True)
    return result


@router.get("/snapshots")
def portfolio_history(limit: int = 100, db: Session = Depends(get_db)):
    """Get portfolio value history for charting."""
    snapshots = db.query(PortfolioSnapshot).order_by(
        PortfolioSnapshot.created_at.desc()
    ).limit(limit).all()
    return list(reversed(snapshots))
