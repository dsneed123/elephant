"""Market data endpoints — proxy to Kalshi API."""

import asyncio
import logging
import time
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from app.services.kalshi_client import KalshiCircuitOpenError, get_kalshi_client

logger = logging.getLogger(__name__)

router = APIRouter()

_TTL_MARKETS = 300.0    # 5 minutes for market list / individual markets
_TTL_ORDERBOOK = 60.0   # 1 minute for orderbooks

_cache: dict[str, tuple[Any, float]] = {}
_cache_lock = asyncio.Lock()


def _cache_get(key: str) -> Any | None:
    entry = _cache.get(key)
    if entry is not None and time.monotonic() < entry[1]:
        return entry[0]
    return None


def _cache_set(key: str, value: Any, ttl: float) -> None:
    _cache[key] = (value, time.monotonic() + ttl)


@router.get("/")
async def list_markets(limit: int = 20, status: str = "open"):
    """List active Kalshi markets."""
    cache_key = f"markets:{limit}:{status}"
    async with _cache_lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
    try:
        data = await get_kalshi_client().list_markets(limit=limit, status=status)
    except KalshiCircuitOpenError:
        raise HTTPException(status_code=503, detail="Kalshi API circuit breaker is open")
    except httpx.TimeoutException:
        raise HTTPException(status_code=503, detail="Kalshi API timed out")
    except httpx.HTTPStatusError as exc:
        logger.warning("Kalshi list_markets error %d", exc.response.status_code)
        raise HTTPException(status_code=502, detail="Kalshi API returned an error")
    async with _cache_lock:
        _cache_set(cache_key, data, _TTL_MARKETS)
    return data


@router.get("/{ticker}")
async def get_market(ticker: str):
    """Get a specific market by ticker."""
    cache_key = f"market:{ticker}"
    async with _cache_lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
    try:
        data = await get_kalshi_client().get_market(ticker)
    except KalshiCircuitOpenError:
        raise HTTPException(status_code=503, detail="Kalshi API circuit breaker is open")
    except httpx.TimeoutException:
        raise HTTPException(status_code=503, detail="Kalshi API timed out")
    except httpx.HTTPStatusError as exc:
        logger.warning("Kalshi get_market error %d for %s", exc.response.status_code, ticker)
        raise HTTPException(status_code=502, detail="Kalshi API returned an error")
    async with _cache_lock:
        _cache_set(cache_key, data, _TTL_MARKETS)
    return data


@router.get("/{ticker}/orderbook")
async def get_orderbook(ticker: str):
    """Get order book for a market."""
    cache_key = f"orderbook:{ticker}"
    async with _cache_lock:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
    try:
        data = await get_kalshi_client().get_orderbook(ticker)
    except KalshiCircuitOpenError:
        raise HTTPException(status_code=503, detail="Kalshi API circuit breaker is open")
    except httpx.TimeoutException:
        raise HTTPException(status_code=503, detail="Kalshi API timed out")
    except httpx.HTTPStatusError as exc:
        logger.warning("Kalshi get_orderbook error %d for %s", exc.response.status_code, ticker)
        raise HTTPException(status_code=502, detail="Kalshi API returned an error")
    async with _cache_lock:
        _cache_set(cache_key, data, _TTL_ORDERBOOK)
    return data
