"""Elephant — Kalshi copy-trading platform."""

import logging
import os
from contextlib import asynccontextmanager

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.config import settings as app_settings
from app.limiter import limiter
from app.db import SessionLocal
from app.middleware.auth import APIKeyMiddleware
from app.websocket_manager import get_manager
from app.models import CopiedTrade, PortfolioSnapshot
from app.routers import traders, markets, portfolio, signals
from app.routers import settings as settings_router
from app.services.kalshi_client import get_kalshi_client, is_circuit_open
from app.services.leaderboard_scraper import run_scrape
from app.services.orderbook_monitor import get_monitor, run_orderbook_monitor
from app.services.execution_service import check_stop_losses
from app.services.settlement_service import settle_open_trades, poll_open_orders
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


async def _poll_open_orders_job() -> None:
    """APScheduler wrapper: open a DB session, poll pending order fill status, close."""
    db = SessionLocal()
    try:
        await poll_open_orders(db)
    except Exception:
        logger.exception("_poll_open_orders_job failed")
    finally:
        db.close()


async def _check_stop_losses_job() -> None:
    """APScheduler wrapper: open a DB session, check stop-losses, close."""
    db = SessionLocal()
    try:
        await check_stop_losses(db)
    except Exception:
        logger.exception("_check_stop_losses_job failed")
    finally:
        db.close()


async def _premarket_gap_scan_job() -> None:
    """APScheduler wrapper: scan watchlist for pre-market gaps before market open."""
    if not app_settings.watchlist_symbols:
        return
    from lib.stock_scanner import check_premarket_gaps
    from app.services.notification_service import notify_gap_alerts
    try:
        gaps = check_premarket_gaps(app_settings.watchlist_symbols)
        if gaps:
            logger.info("Pre-market gap scan: %d gap(s) detected", len(gaps))
            notify_gap_alerts(gaps)
        else:
            logger.info("Pre-market gap scan: no significant gaps")
    except Exception:
        logger.exception("_premarket_gap_scan_job failed")


async def _earnings_watch_job() -> None:
    """APScheduler wrapper: post weekly earnings watch embed on Monday mornings."""
    if not app_settings.watchlist_symbols:
        return
    from lib.stock_scanner import get_earnings_this_week
    from app.services.notification_service import notify_earnings_watch
    try:
        earnings = get_earnings_this_week(app_settings.watchlist_symbols)
        logger.info(
            "Earnings watch: %d watchlist stock(s) reporting this week",
            len(earnings),
        )
        notify_earnings_watch(earnings)
    except Exception:
        logger.exception("_earnings_watch_job failed")


async def _snapshot_portfolio_job() -> None:
    """APScheduler wrapper: capture a portfolio snapshot every 30 minutes."""
    db = SessionLocal()
    try:
        if app_settings.dry_run:
            # Paper trading: compute paper balance from simulated trades only.
            # stopped_out trades are closed with realised PnL — treat them like settled.
            all_simulated = (
                db.query(CopiedTrade)
                .filter(CopiedTrade.is_simulated.is_(True))
                .all()
            )
            open_trades = [
                t for t in all_simulated
                if t.status not in ("settled", "cancelled", "stopped_out")
            ]
            settled_trades = [
                t for t in all_simulated
                if t.pnl is not None and t.status in ("settled", "stopped_out")
            ]

            total_pnl = sum(t.pnl for t in settled_trades)
            open_costs = sum(t.cost for t in open_trades)
            # Available cash = initial capital + settled PnL - cost locked in open trades
            balance = app_settings.paper_balance_initial + total_pnl - open_costs
            positions_value = sum(t.contracts * t.price for t in open_trades)
        else:
            client = get_kalshi_client()
            balance = await client.get_portfolio_balance()

            open_trades = (
                db.query(CopiedTrade)
                .filter(
                    CopiedTrade.is_simulated.is_(False),
                    CopiedTrade.status.notin_(["settled", "cancelled", "stopped_out"]),
                )
                .all()
            )
            positions_value = sum(t.contracts * t.price for t in open_trades)

            settled_trades = (
                db.query(CopiedTrade)
                .filter(
                    CopiedTrade.is_simulated.is_(False),
                    CopiedTrade.status.in_(["settled", "stopped_out"]),
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
            "paper" if app_settings.dry_run else "live",
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
    # Poll pending order fill status every 2 minutes
    scheduler.add_job(
        _poll_open_orders_job,
        trigger="interval",
        minutes=2,
        id="poll_open_orders",
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
    # Check per-trade stop-losses every 5 minutes
    scheduler.add_job(
        _check_stop_losses_job,
        trigger="interval",
        minutes=5,
        id="check_stop_losses",
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
    # Pre-market gap scan at 09:00 on weekdays (before 09:30 market open)
    scheduler.add_job(
        _premarket_gap_scan_job,
        trigger="cron",
        day_of_week="mon-fri",
        hour=9,
        minute=0,
        id="premarket_gap_scan",
        replace_existing=True,
    )
    # Weekly earnings watch every Monday at 08:00
    scheduler.add_job(
        _earnings_watch_job,
        trigger="cron",
        day_of_week="mon",
        hour=8,
        minute=0,
        id="earnings_watch",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "APScheduler started — leaderboard scrape every 6 hours, "
        "order fill poll every 2 minutes, signal expiry every 5 minutes, "
        "stop-loss check every 5 minutes, trade settlement every 15 minutes, "
        "portfolio snapshot every 30 minutes, "
        "pre-market gap scan weekdays at 09:00, earnings watch Mondays at 08:00"
    )
    if app_settings.dry_run:
        logger.warning(
            "DRY RUN mode enabled (paper_balance_initial=%.2f). "
            "Orders will be simulated — no real trades will be placed. "
            "Set DRY_RUN=false to enable live trading.",
            app_settings.paper_balance_initial,
        )

    # Start WebSocket order book monitor as a proper asyncio task
    # (APScheduler cancels long-running coroutines; asyncio.create_task does not)
    import asyncio
    monitor_task = asyncio.create_task(run_orderbook_monitor())
    logger.info("Order book monitor started as background task")

    yield

    monitor_task.cancel()
    scheduler.shutdown(wait=False)
    logger.info("APScheduler shut down")


app = FastAPI(
    title="Elephant",
    description="Kalshi prediction market copy-trading platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(APIKeyMiddleware)


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(status_code=429, content={"error": "rate limit exceeded"})

app.include_router(traders.router, prefix="/api/traders", tags=["traders"])
app.include_router(markets.router, prefix="/api/markets", tags=["markets"])
app.include_router(portfolio.router, prefix="/api/portfolio", tags=["portfolio"])
app.include_router(signals.router, prefix="/api/signals", tags=["signals"])
app.include_router(settings_router.router, prefix="/api/settings", tags=["settings"])


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    manager = get_manager()
    await manager.connect(ws)
    try:
        while True:
            # Keep the connection alive; clients don't send messages to the server.
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "service": "elephant",
        "circuit_open": is_circuit_open(),
        **get_monitor().health_check(),
    }
