"""Real-time order book whale detection via Kalshi WebSocket."""

import asyncio
import base64
import json
import logging
import time
from pathlib import Path

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from app.config import settings
from app.services.signal_generator import WhaleEvent, process_whale_event

logger = logging.getLogger(__name__)

_BASE_BACKOFF = 1.0
_MAX_BACKOFF = 60.0


def _load_private_key():
    key_path = Path(settings.kalshi_private_key_path)
    if not key_path.exists():
        raise FileNotFoundError(f"Kalshi private key not found: {settings.kalshi_private_key_path}")
    pem = key_path.read_bytes()
    return serialization.load_pem_private_key(pem, password=None)


def _make_auth_headers(private_key) -> dict[str, str]:
    """Generate RSA-PSS auth headers for the WebSocket upgrade request."""
    timestamp_ms = str(int(time.time() * 1000))
    path = "/trade-api/ws/v2"
    message = (timestamp_ms + "GET" + path).encode()
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": settings.kalshi_api_key,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
    }


def _get_tracked_market_tickers() -> list[str]:
    """Query DB for all market tickers tracked by active traders."""
    from app.db import SessionLocal
    from app.models import TrackedTrader
    db = SessionLocal()
    try:
        traders = (
            db.query(TrackedTrader)
            .filter(TrackedTrader.is_active == True)  # noqa: E712
            .all()
        )
        tickers: set[str] = set()
        for trader in traders:
            if not trader.top_markets:
                continue
            try:
                markets = json.loads(trader.top_markets)
                tickers.update(markets)
            except (json.JSONDecodeError, TypeError):
                continue
        return list(tickers)
    finally:
        db.close()


def _detect_whale(msg: dict) -> WhaleEvent | None:
    """
    Inspect an orderbook_delta message and return a WhaleEvent if it meets
    the whale threshold, or None otherwise.

    Kalshi orderbook_delta msg fields:
      market_ticker  — market identifier
      side           — "yes" or "no"
      price          — price in cents (1–99)
      delta          — contracts added (positive) or removed (negative)

    Estimated USD value = delta * price / 100
    Only positive deltas (new orders being placed) are considered.
    """
    ticker = msg.get("market_ticker", "")
    side = msg.get("side", "")
    price = msg.get("price", 0)
    delta = msg.get("delta", 0)

    if not ticker or not side or delta <= 0:
        return None

    order_size_usd = delta * price / 100.0
    if order_size_usd < settings.whale_order_threshold:
        return None

    return WhaleEvent(
        market_ticker=ticker,
        side=side,
        action="buy",
        order_size=order_size_usd,
    )


async def _run_connection(tickers: list[str], private_key) -> None:
    """Open a single WebSocket connection, subscribe, and process messages."""
    headers = _make_auth_headers(private_key)
    url = settings.kalshi_ws_url

    logger.info("Connecting to Kalshi WebSocket: %s (%d markets)", url, len(tickers))
    async with websockets.connect(url, additional_headers=headers) as ws:
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": tickers,
            },
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info("Subscribed to orderbook_delta for %d markets", len(tickers))

        async for raw in ws:
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Received non-JSON WebSocket message; skipping")
                continue

            if envelope.get("type") != "orderbook_delta":
                continue

            msg = envelope.get("msg", {})
            event = _detect_whale(msg)
            if event is None:
                continue

            logger.info(
                "Whale detected: %s %s %s $%.2f",
                event.market_ticker,
                event.side,
                event.action,
                event.order_size,
            )

            from app.db import SessionLocal
            db = SessionLocal()
            try:
                process_whale_event(event, db)
            except Exception:
                logger.exception(
                    "Error processing whale event for %s", event.market_ticker
                )
            finally:
                db.close()


async def run_orderbook_monitor() -> None:
    """
    Long-running coroutine that monitors Kalshi order books for whale activity.

    Connects to the Kalshi WebSocket API, subscribes to orderbook_delta channels
    for all markets tracked by active traders, and calls process_whale_event()
    when a whale-sized order is detected.  Auto-reconnects with exponential
    backoff on disconnect or error.
    """
    backoff = _BASE_BACKOFF

    try:
        private_key = _load_private_key()
    except FileNotFoundError:
        logger.error(
            "Kalshi private key not found at %s — order book monitor disabled",
            settings.kalshi_private_key_path,
        )
        return

    while True:
        tickers = _get_tracked_market_tickers()
        if not tickers:
            logger.info(
                "No tracked markets found; retrying in %.0fs", backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
            continue

        try:
            await _run_connection(tickers, private_key)
            # Clean disconnect — reset backoff and reconnect promptly
            logger.info("WebSocket disconnected cleanly; reconnecting in %.0fs", backoff)
            backoff = _BASE_BACKOFF
        except Exception:
            logger.exception(
                "WebSocket connection error; reconnecting in %.0fs", backoff
            )

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _MAX_BACKOFF)
