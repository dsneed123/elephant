"""Tests for GET/PATCH /api/settings endpoints."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.settings import _load, _save, AppSettings, _SETTINGS_FILE, _STATE_DIR


@pytest.fixture(autouse=True)
def isolated_state_dir(tmp_path, monkeypatch):
    """Redirect _STATE_DIR and _SETTINGS_FILE to a temp directory for each test."""
    state = tmp_path / "state"
    settings_file = state / "settings.json"
    monkeypatch.setattr("app.routers.settings._STATE_DIR", state)
    monkeypatch.setattr("app.routers.settings._SETTINGS_FILE", settings_file)
    yield state, settings_file


@pytest.fixture
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# _load helpers
# ---------------------------------------------------------------------------

class TestLoad:
    def test_returns_env_defaults_when_no_file(self, isolated_state_dir):
        _, settings_file = isolated_state_dir
        assert not settings_file.exists()
        s = _load()
        assert isinstance(s, AppSettings)
        assert s.max_exposure_pct > 0
        assert s.paper_trading_mode in (True, False)

    def test_reads_from_file_when_present(self, isolated_state_dir):
        state, settings_file = isolated_state_dir
        state.mkdir(parents=True, exist_ok=True)
        data = {
            "max_exposure_pct": 0.15,
            "max_daily_loss_pct": 0.05,
            "stop_loss_pct": 0.10,
            "min_confidence_threshold": 0.80,
            "whale_order_threshold": 500.0,
            "paper_trading_mode": False,
            "paper_balance": 2000.0,
        }
        settings_file.write_text(json.dumps(data))
        s = _load()
        assert s.max_exposure_pct == 0.15
        assert s.paper_balance == 2000.0
        assert s.paper_trading_mode is False

    def test_falls_back_to_defaults_on_corrupt_file(self, isolated_state_dir):
        state, settings_file = isolated_state_dir
        state.mkdir(parents=True, exist_ok=True)
        settings_file.write_text("{not valid json")
        s = _load()
        assert isinstance(s, AppSettings)


# ---------------------------------------------------------------------------
# GET /api/settings
# ---------------------------------------------------------------------------

class TestGetSettings:
    def test_returns_200_with_all_fields(self, client):
        resp = client.get("/api/settings/")
        assert resp.status_code == 200
        data = resp.json()
        expected_keys = {
            "max_exposure_pct",
            "max_daily_loss_pct",
            "stop_loss_pct",
            "min_confidence_threshold",
            "whale_order_threshold",
            "paper_trading_mode",
            "paper_balance",
        }
        assert expected_keys == set(data.keys())

    def test_numeric_fields_are_positive(self, client):
        data = client.get("/api/settings/").json()
        assert data["max_exposure_pct"] > 0
        assert data["whale_order_threshold"] > 0
        assert data["paper_balance"] > 0


# ---------------------------------------------------------------------------
# PATCH /api/settings
# ---------------------------------------------------------------------------

class TestPatchSettings:
    def test_partial_update(self, client):
        resp = client.patch("/api/settings/", json={"max_exposure_pct": 0.25})
        assert resp.status_code == 200
        assert resp.json()["max_exposure_pct"] == 0.25

    def test_persists_to_file(self, client, isolated_state_dir):
        _, settings_file = isolated_state_dir
        client.patch("/api/settings/", json={"paper_balance": 9999.0})
        assert settings_file.exists()
        saved = json.loads(settings_file.read_text())
        assert saved["paper_balance"] == 9999.0

    def test_toggle_paper_trading_mode(self, client):
        original = client.get("/api/settings/").json()["paper_trading_mode"]
        resp = client.patch("/api/settings/", json={"paper_trading_mode": not original})
        assert resp.status_code == 200
        assert resp.json()["paper_trading_mode"] is not original

    def test_rejects_exposure_pct_above_one(self, client):
        resp = client.patch("/api/settings/", json={"max_exposure_pct": 1.5})
        assert resp.status_code == 422

    def test_rejects_zero_exposure_pct(self, client):
        resp = client.patch("/api/settings/", json={"max_exposure_pct": 0.0})
        assert resp.status_code == 422

    def test_rejects_negative_whale_threshold(self, client):
        resp = client.patch("/api/settings/", json={"whale_order_threshold": -100.0})
        assert resp.status_code == 422

    def test_rejects_zero_paper_balance(self, client):
        resp = client.patch("/api/settings/", json={"paper_balance": 0.0})
        assert resp.status_code == 422

    def test_empty_patch_returns_current(self, client):
        before = client.get("/api/settings/").json()
        resp = client.patch("/api/settings/", json={})
        assert resp.status_code == 200
        assert resp.json() == before
