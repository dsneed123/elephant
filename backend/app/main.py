"""Elephant — Kalshi copy-trading platform."""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import engine, Base
from app.routers import traders, markets, portfolio, signals


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    Base.metadata.create_all(bind=engine)
    yield


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
