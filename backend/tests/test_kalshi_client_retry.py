"""Tests for KalshiClient retry logic."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.services.kalshi_client import _backoff_delay, _retry_after_delay, _with_retry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_http_error(status_code: int, headers: dict | None = None) -> httpx.HTTPStatusError:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.headers = headers or {}
    return httpx.HTTPStatusError("error", request=MagicMock(), response=response)


def _make_retryable_method(side_effects):
    """Build a minimal object with a @_with_retry method driven by side_effects list."""

    class _Dummy:
        @_with_retry
        async def call(self):
            effect = side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect

    return _Dummy()


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# _backoff_delay
# ---------------------------------------------------------------------------

class TestBackoffDelay:
    def test_attempt_0_within_bounds(self):
        for _ in range(50):
            d = _backoff_delay(0)
            assert 0 <= d <= 1.0  # cap = min(30, 1 * 2^0) = 1

    def test_attempt_1_within_bounds(self):
        for _ in range(50):
            d = _backoff_delay(1)
            assert 0 <= d <= 2.0  # cap = min(30, 1 * 2^1) = 2

    def test_caps_at_max_delay(self):
        for _ in range(50):
            d = _backoff_delay(10)
            assert d <= 30.0


# ---------------------------------------------------------------------------
# _retry_after_delay
# ---------------------------------------------------------------------------

class TestRetryAfterDelay:
    def test_uses_header_when_present(self):
        response = MagicMock()
        response.headers = {"Retry-After": "5"}
        assert _retry_after_delay(response, 0) == pytest.approx(5.0)

    def test_falls_back_to_backoff_when_missing(self):
        response = MagicMock()
        response.headers = {}
        for _ in range(20):
            d = _retry_after_delay(response, 0)
            assert 0 <= d <= 1.0

    def test_falls_back_to_backoff_when_header_invalid(self):
        response = MagicMock()
        response.headers = {"Retry-After": "not-a-number"}
        for _ in range(20):
            d = _retry_after_delay(response, 0)
            assert 0 <= d <= 1.0


# ---------------------------------------------------------------------------
# Retry behaviour (sleep patched to avoid actual delays)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    mock = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", mock)
    return mock


class TestRetryOn429:
    def test_succeeds_after_one_429(self):
        obj = _make_retryable_method([_make_http_error(429), "ok"])
        result = run(obj.call())
        assert result == "ok"

    def test_raises_after_max_retries(self):
        errors = [_make_http_error(429)] * 4  # 3 retries + final raise
        obj = _make_retryable_method(errors)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            run(obj.call())
        assert exc_info.value.response.status_code == 429

    def test_respects_retry_after_header(self, no_sleep):
        error = _make_http_error(429, headers={"Retry-After": "7"})
        obj = _make_retryable_method([error, "ok"])
        run(obj.call())
        no_sleep.assert_awaited_once_with(pytest.approx(7.0))


class TestRetryOn5xx:
    def test_succeeds_after_one_503(self):
        obj = _make_retryable_method([_make_http_error(503), "ok"])
        result = run(obj.call())
        assert result == "ok"

    def test_raises_after_max_retries_on_500(self):
        errors = [_make_http_error(500)] * 4
        obj = _make_retryable_method(errors)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            run(obj.call())
        assert exc_info.value.response.status_code == 500


class TestRetryOnTimeout:
    def test_succeeds_after_timeout(self):
        obj = _make_retryable_method([httpx.TimeoutException("timeout"), "ok"])
        result = run(obj.call())
        assert result == "ok"

    def test_raises_after_max_retries(self):
        errors = [httpx.TimeoutException("timeout")] * 4
        obj = _make_retryable_method(errors)
        with pytest.raises(httpx.TimeoutException):
            run(obj.call())


class TestNoRetryOn4xx:
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    def test_raises_immediately_on_client_error(self, status, no_sleep):
        obj = _make_retryable_method([_make_http_error(status), "ok"])
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            run(obj.call())
        assert exc_info.value.response.status_code == status
        no_sleep.assert_not_awaited()


class TestRetryLogging:
    def test_logs_warning_on_retry(self, caplog):
        import logging
        obj = _make_retryable_method([_make_http_error(503), "ok"])
        with caplog.at_level(logging.WARNING, logger="app.services.kalshi_client"):
            run(obj.call())
        assert any("server error 503" in r.message for r in caplog.records)
