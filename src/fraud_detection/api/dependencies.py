"""FastAPI dependency providers — typed access to shared, startup-built state.

`AppState` is constructed once by `app.py`'s lifespan handler at
process startup (model load, Feast client construction) and stored on
`app.state`; every request pulls the same instance via `Depends`
rather than rebuilding anything per request.
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request

from fraud_detection.api.prediction_service import PredictionService
from fraud_detection.features.feast_store import FeastFeatureStore


@dataclass
class AppState:
    """Everything built once at startup and shared across requests.

    `prediction_service`/`model_version`/`feast_store` are `None` when
    startup couldn't load a Production model (see `app._load_app_state`)
    — the process still starts so `/health` stays green, but `/ready`
    reports not-ready and `/predict` returns 503 until it's fixed.
    """

    prediction_service: PredictionService | None
    model_version: str | None
    feast_store: FeastFeatureStore | None
    redis_host: str
    redis_port: int


def get_app_state(request: Request) -> AppState:
    state: AppState = request.app.state.fraud_detection
    return state


def get_prediction_service(
    state: AppState = Depends(get_app_state),  # noqa: B008 — FastAPI's own DI pattern
) -> PredictionService:
    """Raises 503 instead of returning `None` — routes never see an unready service."""
    if state.prediction_service is None:
        raise HTTPException(status_code=503, detail="Model not loaded; service is not ready")
    return state.prediction_service
