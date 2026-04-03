"""Elephant — Kalshi copy-trading platform."""

import logging
import os
from contextlib import asynccontextmanager

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import SessionLocal
from app.models import CopiedTrade, PortfolioSnapshot
from app.routers import traders, markets, portfolio, signals
from app.services.kalshi_client import get_kalshi_client
from app.services.leaderboard_scraper import run_scrape
from app.services.orderbook_monitor import run_orderbook_monitor
from app.services.settlement_service import settle_open_trades
from app.services.signal_generator import expire_stale_signals

_ALEMBIC_INI = os.path.join(os.path.dirname(__file__), "..", "alembic.ini")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _expire_signals_job() -> None:
    """APScheduler wrapper: open a DB session, expire stale signals, close."""
    db = SessionLocal()
    try:
        expire_stale_signals(db)
    finally:
        db.close()


async def _settle_trades_job() -> None:
    """APScheduler wrapper: open a DB session, settle open trades, close."""
    db = SessionLocal()
    try:
        await settle_open_trades(db)
    finally:
        db.close()


async def _snapshot_portfolio_job() -> None:
    """APScheduler wrapper: capture a portfolio snapshot every 30 minutes."""
    db = SessionLocal()
    try:
        if settings.dry_run:
            # Paper trading: compute paper balance from simulated trades only.
            all_simulated = (
                db.query(CopiedTrade)
                .filter(CopiedTrade.is_simulated.is_(True))
                .all()
            )
            open_trades = [t for t in all_simulated if t.status not in ("settled", "cancelled")]
            settled_trades = [t for t in all_simulated if t.status == "settled" and t.pnl is not None]

            total_pnl = sum(t.pnl for t in settled_trades)
            open_costs = sum(t.cost for t in open_trades)
            # Available cash = initial capital + settled PnL - cost locked in open trades
            balance = settings.paper_balance_initial + total_pnl - open_costs
            positions_value = sum(t.contracts * t.price for t in open_trades)
        else:
            client = get_kalshi_client()
            balance = await client.get_portfolio_balance()

            open_trades = (
                db.query(CopiedTrade)
                .filter(
                    CopiedTrade.is_simulated.is_(False),
                    CopiedTrade.status.notin_(["settled", "cancelled"]),
                )
                .all()
            )
            positions_value = sum(t.contracts * t.price for t in open_trades)

            settled_trades = (
                db.query(CopiedTrade)
                .filter(
                    CopiedTrade.is_simulated.is_(False),
                    CopiedTrade.status == "settled",
                    CopiedTrade.pnl.isnot(None),
                )
                .all()
            )
            total_pnl = sum(t.pnl for t in settled_trades)

        profitable = sum(1 for t in settled_trades if t.pnl > 0)
        win_rate = profitable / len(settled_trades) if settled_trades else 0.0

        snapshot = PortfolioSnapshot(
            balance=balance,
            positions_value=positions_value,
            total_value=balance + positions_value,
            total_pnl=total_pnl,
            win_rate=win_rate,
        )
        db.add(snapshot)
        db.commit()
        logger.info(
            "Portfolio snapshot (%s): balance=%.2f positions=%.2f total_pnl=%.2f win_rate=%.2f",
            "paper" if settings.dry_run else "live",
            balance,
            positions_value,
            total_pnl,
            win_rate,
        )
    except Exception:
        logger.exception("_snapshot_portfolio_job failed")
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Apply any pending migrations on startup
    alembic_cfg = AlembicConfig(_ALEMBIC_INI)
    alembic_command.upgrade(alembic_cfg, "head")

    # Schedule leaderboard scrape every 6 hours
    scheduler.add_job(
        run_scrape,
        trigger="interval",
        hours=6,
        id="leaderboard_scrape",
        replace_existing=True,
    )
    # Expire stale pending signals every 5 minutes
    scheduler.add_job(
        _expire_signals_job,
        trigger="interval",
        minutes=5,
        id="expire_stale_signals",
        replace_existing=True,
    )
    # Settle open trades and reconcile PnL every 15 minutes
    scheduler.add_job(
        _settle_trades_job,
        trigger="interval",
        minutes=15,
        id="settle_open_trades",
        replace_existing=True,
    )
    # Snapshot portfolio state every 30 minutes
    scheduler.add_job(
        _snapshot_portfolio_job,
        trigger="interval",
        minutes=30,
        id="portfolio_snapshot",
        replace_existing=True,
    )
    # Start WebSocket order book monitor immediately as a background task
    scheduler.add_job(
        run_orderbook_monitor,
        trigger="date",
        id="orderbook_monitor",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "APScheduler started — leaderboard scrape every 6 hours, "
        "signal expiry every 5 minutes, trade settlement every 15 minutes, "
        "portfolio snapshot every 30 minutes, order book monitor running"
    )
    if settings.dry_run:
        logger.warning(
            "DRY RUN mode enabled (paper_balance_initial=%.2f). "
            "Orders will be simulated — no real trades will be placed. "
            "Set DRY_RUN=false to enable live trading.",
            settings.paper_balance_initial,
        )

    yield

    scheduler.shutdown(wait=False)
    logger.info("APScheduler shut down")


app = FastAPI(
    title="Elephant",
    description="Kalshi prediction market copy-trading platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(traders.router, prefix="/api/traders", tags=["traders"])
app.include_router(markets.router, prefix="/api/markets", tags=["markets"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["portfolio"])
app.include_router(signals.router, prefix="/api/signals", tags=["signals"])


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "elephant"}
