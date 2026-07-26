"""FastAPI application: startup wiring (lifespan) + router mount.

Loads the MLflow "Production" model, the feature order it was trained
with, and a `FeastFeatureStore` exactly once, at process startup —
never per request; a registry-backed "Production" stage exists
precisely so serving doesn't re-resolve/reload it on every call.

If startup loading fails (no model promoted yet, MLflow/Feast
unreachable, ...), the process still starts: `/health` stays green for
a liveness probe, but `/ready` reports not-ready and `/predict` returns
503 until an operator fixes the underlying issue and restarts. See
docs/decisions/0007-fastapi-inference-service.md.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow.sklearn
from fastapi import FastAPI

from fraud_detection.api.dependencies import AppState
from fraud_detection.api.prediction_service import PredictionService
from fraud_detection.api.routers import router
from fraud_detection.common.logger import get_logger
from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.models.model_registry import (
    DEFAULT_MODEL_NAME,
    load_feature_names,
    resolve_production_model,
)
from fraud_detection.models.training import DEFAULT_TRACKING_URI
from fraud_detection.monitoring.prediction_log import DEFAULT_LOG_PATH

logger = get_logger(__name__)

DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379

_NOT_READY_STATE = AppState(
    prediction_service=None,
    model_version=None,
    feast_store=None,
    redis_host=DEFAULT_REDIS_HOST,
    redis_port=DEFAULT_REDIS_PORT,
)


def load_app_state(
    model_name: str = DEFAULT_MODEL_NAME,
    tracking_uri: str = DEFAULT_TRACKING_URI,
    prediction_log_path: Path | str = DEFAULT_LOG_PATH,
) -> AppState:
    """Build everything the service needs once; never raises.

    Startup dependency loading is a boundary where the alternative to
    a broad `except Exception` is crash-looping the whole process for
    an MLflow/Feast/Redis outage that `/ready` is specifically meant to
    surface instead — see the module docstring.
    """
    try:
        mlflow.set_tracking_uri(tracking_uri)
        production = resolve_production_model(model_name)
        model = mlflow.sklearn.load_model(production.uri)
        feature_order = load_feature_names(production.run_id)
        feast_store = FeastFeatureStore(
            repo_path=DEFAULT_FEAST_REPO_PATH,
            offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
        )
        redis_host, redis_port = feast_store.online_store_address()
    except Exception as exc:  # noqa: BLE001 — startup boundary, see docstring
        logger.error(
            "model/feature-store load failed at startup; service starting not-ready",
            extra={"error": str(exc), "model_name": model_name},
        )
        return _NOT_READY_STATE

    logger.info(
        "model loaded", extra={"model_name": model_name, "model_version": production.version}
    )
    return AppState(
        prediction_service=PredictionService(
            model=model,
            model_version=production.version,
            feature_order=feature_order,
            feature_store=feast_store,
            prediction_log_path=prediction_log_path,
        ),
        model_version=production.version,
        feast_store=feast_store,
        redis_host=redis_host,
        redis_port=redis_port,
    )


def create_app(
    model_name: str = DEFAULT_MODEL_NAME,
    tracking_uri: str = DEFAULT_TRACKING_URI,
    prediction_log_path: Path | str = DEFAULT_LOG_PATH,
) -> FastAPI:
    """Build the FastAPI app. `model_name`/`tracking_uri`/`prediction_log_path`
    are overridable so tests can point startup at an isolated MLflow store
    and a tmp log file instead of the real ones — see tests/api/test_integration.py.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.fraud_detection = load_app_state(model_name, tracking_uri, prediction_log_path)
        yield

    app = FastAPI(
        title="Fraud Detection Inference API",
        description=(
            "Transaction -> Feast online features (Redis) -> MLflow Production model "
            "-> fraud probability."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
