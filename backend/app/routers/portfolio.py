"""Portfolio and copy-trading endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db import get_db
from app.models import CopiedTrade, PortfolioSnapshot

router = APIRouter()


@router.get("/trades")
def list_copied_trades(limit: int = 50, db: Session = Depends(get_db)):
    """List recently copied trades."""
    trades = db.query(CopiedTrade).order_by(
        CopiedTrade.created_at.desc()
    ).limit(limit).all()
    return trades


@router.get("/performance")
def portfolio_performance(db: Session = Depends(get_db)):
    """Get portfolio performance summary."""
    latest = db.query(PortfolioSnapshot).order_by(
        PortfolioSnapshot.created_at.desc()
    ).first()

    total_trades = db.query(CopiedTrade).count()
    winning = db.query(CopiedTrade).filter(CopiedTrade.pnl > 0).count()
    total_pnl = sum(
        t.pnl for t in db.query(CopiedTrade).filter(
            CopiedTrade.status == "settled"
        ).all()
    )

    return {
        "balance": latest.balance if latest else 0,
        "total_value": latest.total_value if latest else 0,
        "total_pnl": total_pnl,
        "total_trades": total_trades,
        "win_rate": winning / total_trades if total_trades > 0 else 0,
    }


@router.get("/snapshots")
def portfolio_history(limit: int = 100, db: Session = Depends(get_db)):
    """Get portfolio value history for charting."""
    snapshots = db.query(PortfolioSnapshot).order_by(
        PortfolioSnapshot.created_at.desc()
    ).limit(limit).all()
    return list(reversed(snapshots))
