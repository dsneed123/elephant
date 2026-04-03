"""Runtime settings endpoint — reads/writes state/settings.json."""

import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel, field_validator

from app.config import settings as env_settings

logger = logging.getLogger(__name__)

_STATE_DIR = Path(__file__).parent.parent.parent / "state"
_SETTINGS_FILE = _STATE_DIR / "settings.json"

router = APIRouter()


class AppSettings(BaseModel):
    max_exposure_pct: float
    max_daily_loss_pct: float
    stop_loss_pct: float
    min_confidence_threshold: float
    whale_order_threshold: float
    paper_trading_mode: bool
    paper_balance: float


class SettingsPatch(BaseModel):
    max_exposure_pct: Optional[float] = None
    max_daily_loss_pct: Optional[float] = None
    stop_loss_pct: Optional[float] = None
    min_confidence_threshold: Optional[float] = None
    whale_order_threshold: Optional[float] = None
    paper_trading_mode: Optional[bool] = None
    paper_balance: Optional[float] = None

    @field_validator(
        "max_exposure_pct", "max_daily_loss_pct", "stop_loss_pct", "min_confidence_threshold"
    )
    @classmethod
    def validate_fraction(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 < v <= 1.0):
            raise ValueError("must be between 0 and 1 (exclusive of 0)")
        return v

    @field_validator("whale_order_threshold", "paper_balance")
    @classmethod
    def validate_positive(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v <= 0:
            raise ValueError("must be a positive number")
        return v


def _load() -> AppSettings:
    """Load settings from state file, falling back to env defaults."""
    if _SETTINGS_FILE.exists():
        try:
            data = json.loads(_SETTINGS_FILE.read_text())
            return AppSettings(**data)
        except Exception:
            logger.warning(
                "Failed to parse %s — falling back to env defaults", _SETTINGS_FILE
            )
    return AppSettings(
        max_exposure_pct=env_settings.max_total_exposure_pct,
        max_daily_loss_pct=env_settings.max_daily_loss_pct,
        stop_loss_pct=env_settings.stop_loss_pct,
        min_confidence_threshold=env_settings.min_signal_confidence,
        whale_order_threshold=env_settings.whale_order_threshold,
        paper_trading_mode=env_settings.dry_run,
        paper_balance=env_settings.paper_balance_initial,
    )


def _save(s: AppSettings) -> None:
    """Persist settings to state/settings.json."""
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    _SETTINGS_FILE.write_text(json.dumps(s.model_dump(), indent=2))


@router.get("/", response_model=AppSettings)
def get_settings():
    """Return current runtime settings."""
    return _load()


@router.patch("/", response_model=AppSettings)
def patch_settings(patch: SettingsPatch):
    """Update one or more runtime settings."""
    current = _load()
    updates = patch.model_dump(exclude_none=True)
    updated = current.model_copy(update=updates)
    _save(updated)
    return updated
