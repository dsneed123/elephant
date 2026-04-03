"""Elephant — Kalshi copy-trading platform."""

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import engine, Base
from app.routers import traders, markets, portfolio, signals
from app.services.leaderboard_scraper import run_scrape

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    Base.metadata.create_all(bind=engine)

    # Schedule leaderboard scrape every 6 hours
    scheduler.add_job(
        run_scrape,
        trigger="interval",
        hours=6,
        id="leaderboard_scrape",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("APScheduler started — leaderboard scrape scheduled every 6 hours")

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
