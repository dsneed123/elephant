"""Elephant — Kalshi copy-trading platform."""

import logging
import os
from contextlib import asynccontextmanager

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import SessionLocal
from app.routers import traders, markets, portfolio, signals
from app.services.leaderboard_scraper import run_scrape
from app.services.orderbook_monitor import run_orderbook_monitor
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
        "signal expiry every 5 minutes, order book monitor running"
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
