"""Kalshi authenticated HTTP client with RSA-PSS request signing."""

import asyncio
import base64
import enum
import functools
import logging
import random
import time
from pathlib import Path

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from app.config import settings

logger = logging.getLogger(__name__)

_RETRY_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY = 1.0   # seconds
_RETRY_MAX_DELAY = 30.0   # seconds

# Circuit breaker parameters
_CB_FAILURE_THRESHOLD = 5    # consecutive failures within _CB_FAILURE_WINDOW
_CB_FAILURE_WINDOW = 60.0    # seconds
_CB_RECOVERY_TIMEOUT = 30.0  # seconds in OPEN state before trying HALF_OPEN


class KalshiCircuitOpenError(Exception):
    """Raised when the Kalshi API circuit breaker is open; no retries are attempted."""


class _CBState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class _CircuitBreaker:
    """Three-state circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED).

    CLOSED:    normal operation; failures are counted in a rolling time window.
    OPEN:      all calls fail immediately with KalshiCircuitOpenError for
               _CB_RECOVERY_TIMEOUT seconds.
    HALF_OPEN: a single probe call is allowed through; success closes the
               circuit, failure re-opens it.
    """

    def __init__(self) -> None:
        self._state = _CBState.CLOSED
        self._failure_times: list[float] = []
        self._opened_at: float = 0.0

    @property
    def is_open(self) -> bool:
        return self._state == _CBState.OPEN

    @property
    def state(self) -> str:
        return self._state.value

    def check(self) -> None:
        """Raise KalshiCircuitOpenError if the circuit should block the call."""
        if self._state == _CBState.CLOSED:
            return
        if self._state == _CBState.OPEN:
            if time.monotonic() - self._opened_at >= _CB_RECOVERY_TIMEOUT:
                self._state = _CBState.HALF_OPEN
                logger.info("Circuit breaker → HALF_OPEN (testing recovery)")
            else:
                raise KalshiCircuitOpenError(
                    "Kalshi API circuit breaker is OPEN; retry after %.0fs"
                    % (_CB_RECOVERY_TIMEOUT - (time.monotonic() - self._opened_at))
                )
        # HALF_OPEN: let the probe call through

    def record_success(self) -> None:
        if self._state == _CBState.HALF_OPEN:
            self._state = _CBState.CLOSED
            self._failure_times.clear()
            logger.info("Circuit breaker → CLOSED (recovery confirmed)")

    def record_failure(self) -> None:
        now = time.monotonic()
        if self._state == _CBState.HALF_OPEN:
            self._state = _CBState.OPEN
            self._opened_at = now
            logger.warning("Circuit breaker → OPEN (probe failed)")
            return
        # CLOSED: track failures in rolling window
        self._failure_times = [t for t in self._failure_times if now - t < _CB_FAILURE_WINDOW]
        self._failure_times.append(now)
        if len(self._failure_times) >= _CB_FAILURE_THRESHOLD:
            self._state = _CBState.OPEN
            self._opened_at = now
            logger.warning(
                "Circuit breaker → OPEN (%d failures within %.0fs window)",
                len(self._failure_times),
                _CB_FAILURE_WINDOW,
            )


def _with_retry(fn):
    """Decorator: retry on 429, 5xx, and network timeouts with exponential backoff + jitter.

    Rules:
    - Circuit breaker: if self._circuit_breaker is OPEN, raise KalshiCircuitOpenError
      immediately before each attempt (no retries while open).
    - 429: respect Retry-After header if present, else backoff. Max 3 retries.
    - 5xx: exponential backoff with jitter. Max 3 retries.
    - httpx.TimeoutException: exponential backoff with jitter. Max 3 retries.
    - Other 4xx: raise immediately (no retry, does not trip circuit breaker).
    """

    @functools.wraps(fn)
    async def wrapper(self, *args, **kwargs):
        cb = getattr(self, "_circuit_breaker", None)
        for attempt in range(_RETRY_MAX_ATTEMPTS + 1):
            if cb is not None:
                cb.check()  # raises KalshiCircuitOpenError if open
            try:
                result = await fn(self, *args, **kwargs)
                if cb is not None:
                    cb.record_success()
                return result
            except httpx.TimeoutException as exc:
                if cb is not None:
                    cb.record_failure()
                if attempt == _RETRY_MAX_ATTEMPTS:
                    raise
                delay = _backoff_delay(attempt)
                logger.warning(
                    "%s timed out (attempt %d/%d), retrying in %.1fs",
                    fn.__name__, attempt + 1, _RETRY_MAX_ATTEMPTS, delay,
                )
                await asyncio.sleep(delay)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 429:
                    if cb is not None:
                        cb.record_failure()
                    if attempt == _RETRY_MAX_ATTEMPTS:
                        raise
                    delay = _retry_after_delay(exc.response, attempt)
                    logger.warning(
                        "%s rate-limited 429 (attempt %d/%d), retrying in %.1fs",
                        fn.__name__, attempt + 1, _RETRY_MAX_ATTEMPTS, delay,
                    )
                    await asyncio.sleep(delay)
                elif 500 <= status < 600:
                    if cb is not None:
                        cb.record_failure()
                    if attempt == _RETRY_MAX_ATTEMPTS:
                        raise
                    delay = _backoff_delay(attempt)
                    logger.warning(
                        "%s server error %d (attempt %d/%d), retrying in %.1fs",
                        fn.__name__, status, attempt + 1, _RETRY_MAX_ATTEMPTS, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise  # 4xx client errors do not trip the circuit breaker

    return wrapper


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2^attempt))."""
    cap = min(_RETRY_MAX_DELAY, _RETRY_BASE_DELAY * (2 ** attempt))
    return random.uniform(0, cap)


def _retry_after_delay(response: httpx.Response, attempt: int) -> float:
    """Return delay from Retry-After header or fall back to exponential backoff."""
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return _backoff_delay(attempt)


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

        self._circuit_breaker = _CircuitBreaker()

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

    @_with_retry
    async def get_portfolio_balance(self) -> float:
        """Return the available balance in cents (as a float)."""
        data = await self._get("/portfolio/balance")
        return float(data.get("balance", 0))

    @_with_retry
    async def place_order(
        self,
        ticker: str,
        side: str,
        count: int,
        price: int,
        action: str = "buy",
    ) -> dict:
        """Place a limit order. Returns the full order object (includes order_id)."""
        payload = {
            "ticker": ticker,
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": price if side == "yes" else 100 - price,
            "no_price": price if side == "no" else 100 - price,
            "action": action,
        }
        data = await self._post("/portfolio/orders", json=payload)
        return data.get("order", data)

    @_with_retry
    async def get_order(self, order_id: str) -> dict:
        """Fetch a single order by ID."""
        data = await self._get(f"/portfolio/orders/{order_id}")
        return data.get("order", data)

    @_with_retry
    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order by ID."""
        data = await self._delete(f"/portfolio/orders/{order_id}")
        return data.get("order", data)

    @_with_retry
    async def get_market(self, ticker: str) -> dict:
        """Fetch market data by ticker. Used by settlement to check resolution in dry-run mode."""
        data = await self._get(f"/markets/{ticker}")
        return data.get("market", data)

    @_with_retry
    async def list_markets(self, limit: int = 20, status: str = "open") -> dict:
        """List active markets."""
        return await self._get("/markets", params={"limit": limit, "status": status})

    @_with_retry
    async def get_orderbook(self, ticker: str) -> dict:
        """Fetch order book for a market."""
        return await self._get(f"/markets/{ticker}/orderbook")


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


def is_circuit_open() -> bool:
    """Return True if the KalshiClient circuit breaker is currently OPEN."""
    if not _client_initialized:
        return False
    return kalshi_client._circuit_breaker.is_open
