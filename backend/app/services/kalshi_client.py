"""Kalshi authenticated HTTP client with RSA-PSS request signing."""

import asyncio
import base64
import logging
import time
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from app.config import settings

logger = logging.getLogger(__name__)


class _TokenBucket:
    """Simple token-bucket rate limiter."""

    def __init__(self, rate: float) -> None:
        self._rate = rate  # tokens per second
        self._tokens = rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


class KalshiClient:
    """Authenticated HTTP client for the Kalshi trade API.

    Auth scheme: RSA-PSS signatures over ``timestamp + METHOD + /path``.
    Headers added to every request:
      KALSHI-ACCESS-KEY        — API key from config
      KALSHI-ACCESS-TIMESTAMP  — milliseconds since epoch (str)
      KALSHI-ACCESS-SIGNATURE  — base64(RSA-PSS-SHA256(msg))
    """

    def __init__(self) -> None:
        self._base_url = settings.kalshi_base_url.rstrip("/")
        self._api_key = settings.kalshi_api_key
        self._private_key = self._load_private_key(settings.kalshi_private_key_path)

        # 10 writes/sec, 20 reads/sec
        self._write_bucket = _TokenBucket(rate=10.0)
        self._read_bucket = _TokenBucket(rate=20.0)

    # ------------------------------------------------------------------ #
    # Key loading & signing                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_private_key(path: str):
        key_path = Path(path)
        if not key_path.exists():
            raise FileNotFoundError(f"Kalshi private key not found: {path}")
        pem = key_path.read_bytes()
        return serialization.load_pem_private_key(pem, password=None)

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """Return base64-encoded RSA-PSS-SHA256 signature."""
        message = (timestamp_ms + method.upper() + path).encode()
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        timestamp_ms = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": self._sign(timestamp_ms, method, path),
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------ #
    # Low-level HTTP helpers                                               #
    # ------------------------------------------------------------------ #

    async def _get(self, path: str, **kwargs) -> dict:
        await self._read_bucket.acquire()
        url = self._base_url + path
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers=self._auth_headers("GET", path), **kwargs)
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, json: dict) -> dict:
        await self._write_bucket.acquire()
        url = self._base_url + path
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url, headers=self._auth_headers("POST", path), json=json
            )
        resp.raise_for_status()
        return resp.json()

    async def _delete(self, path: str) -> dict:
        await self._write_bucket.acquire()
        url = self._base_url + path
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.delete(url, headers=self._auth_headers("DELETE", path))
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # Public API methods                                                   #
    # ------------------------------------------------------------------ #

    async def get_portfolio_balance(self) -> float:
        """Return the available balance in cents (as a float)."""
        data = await self._get("/portfolio/balance")
        return float(data.get("balance", 0))

    async def place_order(
        self,
        ticker: str,
        side: str,
        count: int,
        price: int,
    ) -> dict:
        """Place a limit order. Returns the full order object (includes order_id)."""
        payload = {
            "ticker": ticker,
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": price if side == "yes" else 100 - price,
            "no_price": price if side == "no" else 100 - price,
            "action": "buy",
        }
        data = await self._post("/portfolio/orders", json=payload)
        return data.get("order", data)

    async def get_order(self, order_id: str) -> dict:
        """Fetch a single order by ID."""
        data = await self._get(f"/portfolio/orders/{order_id}")
        return data.get("order", data)

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order by ID."""
        data = await self._delete(f"/portfolio/orders/{order_id}")
        return data.get("order", data)


# Module-level singleton
kalshi_client = KalshiClient.__new__(KalshiClient)
_client_initialized = False


def get_kalshi_client() -> KalshiClient:
    """Return the module-level KalshiClient, initializing lazily."""
    global kalshi_client, _client_initialized
    if not _client_initialized:
        kalshi_client = KalshiClient()
        _client_initialized = True
    return kalshi_client
