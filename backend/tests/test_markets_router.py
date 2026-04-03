"""Tests for GET /api/markets/, GET /api/markets/{ticker}, GET /api/markets/{ticker}/orderbook."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.kalshi_client import KalshiCircuitOpenError
import app.routers.markets as markets_module


def _make_http_error(status_code: int) -> httpx.HTTPStatusError:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.headers = {}
    return httpx.HTTPStatusError("error", request=MagicMock(), response=response)


@pytest.fixture(autouse=True)
def clear_cache():
    """Reset the in-memory cache before and after each test."""
    markets_module._cache.clear()
    yield
    markets_module._cache.clear()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_kalshi():
    mock = MagicMock()
    mock.list_markets = AsyncMock(return_value={"markets": []})
    mock.get_market = AsyncMock(return_value={"ticker": "TEST", "status": "open"})
    mock.get_orderbook = AsyncMock(return_value={"orderbook": {"yes": [], "no": []}})
    with patch("app.routers.markets.get_kalshi_client", return_value=mock):
        yield mock


# ---------------------------------------------------------------------------
# GET /api/markets/
# ---------------------------------------------------------------------------

class TestListMarkets:
    def test_5xx_returns_502(self, client, mock_kalshi):
        mock_kalshi.list_markets.side_effect = _make_http_error(500)
        resp = client.get("/api/markets/")
        assert resp.status_code == 502

    def test_timeout_returns_503(self, client, mock_kalshi):
        mock_kalshi.list_markets.side_effect = httpx.TimeoutException("timed out")
        resp = client.get("/api/markets/")
        assert resp.status_code == 503

    def test_circuit_open_returns_503(self, client, mock_kalshi):
        mock_kalshi.list_markets.side_effect = KalshiCircuitOpenError("open")
        resp = client.get("/api/markets/")
        assert resp.status_code == 503

    def test_cache_hit_avoids_second_call(self, client, mock_kalshi):
        resp1 = client.get("/api/markets/")
        resp2 = client.get("/api/markets/")
        assert resp1.status_code == 200
        assert resp2.status_code == 200
        mock_kalshi.list_markets.assert_called_once()

    def test_cache_miss_after_expiry(self, client, mock_kalshi):
        client.get("/api/markets/")
        for key in list(markets_module._cache):
            data, _ = markets_module._cache[key]
            markets_module._cache[key] = (data, time.monotonic() - 1)
        client.get("/api/markets/")
        assert mock_kalshi.list_markets.call_count == 2


# ---------------------------------------------------------------------------
# GET /api/markets/{ticker}
# ---------------------------------------------------------------------------

class TestGetMarket:
    def test_5xx_returns_502(self, client, mock_kalshi):
        mock_kalshi.get_market.side_effect = _make_http_error(503)
        resp = client.get("/api/markets/TEST")
        assert resp.status_code == 502

    def test_timeout_returns_503(self, client, mock_kalshi):
        mock_kalshi.get_market.side_effect = httpx.TimeoutException("timed out")
        resp = client.get("/api/markets/TEST")
        assert resp.status_code == 503

    def test_circuit_open_returns_503(self, client, mock_kalshi):
        mock_kalshi.get_market.side_effect = KalshiCircuitOpenError("open")
        resp = client.get("/api/markets/TEST")
        assert resp.status_code == 503

    def test_cache_hit_avoids_second_call(self, client, mock_kalshi):
        resp1 = client.get("/api/markets/TEST")
        resp2 = client.get("/api/markets/TEST")
        assert resp1.status_code == 200
        assert resp2.status_code == 200
        mock_kalshi.get_market.assert_called_once()


# ---------------------------------------------------------------------------
# GET /api/markets/{ticker}/orderbook
# ---------------------------------------------------------------------------

class TestGetOrderbook:
    def test_5xx_returns_502(self, client, mock_kalshi):
        mock_kalshi.get_orderbook.side_effect = _make_http_error(500)
        resp = client.get("/api/markets/TEST/orderbook")
        assert resp.status_code == 502

    def test_timeout_returns_503(self, client, mock_kalshi):
        mock_kalshi.get_orderbook.side_effect = httpx.TimeoutException("timed out")
        resp = client.get("/api/markets/TEST/orderbook")
        assert resp.status_code == 503

    def test_circuit_open_returns_503(self, client, mock_kalshi):
        mock_kalshi.get_orderbook.side_effect = KalshiCircuitOpenError("open")
        resp = client.get("/api/markets/TEST/orderbook")
        assert resp.status_code == 503

    def test_cache_hit_avoids_second_call(self, client, mock_kalshi):
        resp1 = client.get("/api/markets/TEST/orderbook")
        resp2 = client.get("/api/markets/TEST/orderbook")
        assert resp1.status_code == 200
        assert resp2.status_code == 200
        mock_kalshi.get_orderbook.assert_called_once()
