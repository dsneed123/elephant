"""Tests for APIKeyMiddleware on /api/* endpoints."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def client_with_key():
    """Client fixture with ELEPHANT_API_KEY configured."""
    with patch("app.middleware.auth.settings") as mock_settings:
        mock_settings.api_key = "test-secret-key"
        yield TestClient(app, raise_server_exceptions=False)


class TestDevMode:
    """When no API key is configured, all requests pass through."""

    def test_api_endpoint_passes_without_key(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_non_api_path_passes(self, client):
        # /docs is a non-/api/ path — should pass regardless
        resp = client.get("/docs")
        assert resp.status_code == 200


class TestAuthEnabled:
    """When ELEPHANT_API_KEY is set, /api/* requires a valid key."""

    def test_health_exempt_without_key(self, client_with_key):
        resp = client_with_key.get("/api/health")
        assert resp.status_code == 200

    def test_missing_key_returns_401(self, client_with_key):
        resp = client_with_key.get("/api/traders/")
        assert resp.status_code == 401
        assert resp.json()["detail"] == "Invalid or missing API key"

    def test_wrong_key_returns_401(self, client_with_key):
        resp = client_with_key.get("/api/traders/", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401

    def test_correct_key_passes(self, client_with_key):
        resp = client_with_key.get(
            "/api/health", headers={"X-API-Key": "test-secret-key"}
        )
        assert resp.status_code == 200

    def test_correct_key_on_protected_endpoint(self, client_with_key):
        resp = client_with_key.get(
            "/api/traders/", headers={"X-API-Key": "test-secret-key"}
        )
        # 200 or any non-401 means auth passed (endpoint may return other errors)
        assert resp.status_code != 401

    def test_post_without_key_returns_401(self, client_with_key):
        resp = client_with_key.post("/api/signals/execute/1")
        assert resp.status_code == 401

    def test_health_post_requires_key(self, client_with_key):
        # Only GET /api/health is exempt; other methods are not
        resp = client_with_key.post("/api/health")
        assert resp.status_code == 401
