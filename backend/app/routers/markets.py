"""Market data endpoints — proxy to Kalshi API."""

from fastapi import APIRouter
import httpx

from app.config import settings

router = APIRouter()

KALSHI_BASE = settings.kalshi_base_url


@router.get("/")
async def list_markets(limit: int = 20, status: str = "open"):
    """List active Kalshi markets."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{KALSHI_BASE}/markets",
            params={"limit": limit, "status": status},
        )
        return resp.json()


@router.get("/{ticker}")
async def get_market(ticker: str):
    """Get a specific market by ticker."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{KALSHI_BASE}/markets/{ticker}")
        return resp.json()


@router.get("/{ticker}/orderbook")
async def get_orderbook(ticker: str):
    """Get order book for a market."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{KALSHI_BASE}/markets/{ticker}/orderbook")
        return resp.json()
