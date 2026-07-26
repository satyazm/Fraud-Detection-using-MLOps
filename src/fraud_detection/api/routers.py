"""HTTP routes. Thin: parse request -> call PredictionService -> map errors to HTTP.

No business logic lives here — `PredictionService` (entity lookup,
feature retrieval, inference) and `AppState` (startup wiring) own that;
these handlers only translate between HTTP and the two.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from fraud_detection.api.dependencies import AppState, get_app_state, get_prediction_service
from fraud_detection.api.prediction_service import PredictionService
from fraud_detection.api.schemas import (
    HealthResponse,
    PredictionResponse,
    ReadinessChecks,
    ReadyResponse,
    TransactionRequest,
)
from fraud_detection.common.logger import get_logger
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.feature_store import FeatureStoreError
from fraud_detection.monitoring.metrics import (
    MODEL_FRAUD_PROBABILITY,
    MODEL_PREDICTIONS_TOTAL,
    PREDICTION_ERRORS_TOTAL,
    PREDICTION_LATENCY_SECONDS,
    PREDICTION_REQUESTS_TOTAL,
    REDIS_CONNECTION_STATUS,
    redis_reachable,
)

logger = get_logger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Liveness only: the process is up. Never checks dependencies — see `/ready`."""
    return HealthResponse()


@router.get("/ready", response_model=ReadyResponse)
def ready(state: AppState = Depends(get_app_state)) -> ReadyResponse:  # noqa: B008
    """Readiness: model loaded, Feast client constructed, Redis actually reachable."""
    checks = ReadinessChecks(
        model_loaded=state.prediction_service is not None,
        feast_reachable=state.feast_store is not None,
        redis_reachable=redis_reachable(state.redis_host, state.redis_port),
    )
    return ReadyResponse(
        ready=checks.model_loaded and checks.feast_reachable and checks.redis_reachable,
        checks=checks,
    )


@router.get("/metrics")
def metrics(state: AppState = Depends(get_app_state)) -> Response:  # noqa: B008
    """Prometheus scrape endpoint. `REDIS_CONNECTION_STATUS` is set fresh
    on every scrape (not a stale value from startup) via the same live
    check `/ready` uses — set here rather than via `Gauge.set_function`
    so it correctly reflects *this* app instance's `AppState` even when
    multiple app instances exist in one process (as the test suite does).
    """
    REDIS_CONNECTION_STATUS.set(1.0 if redis_reachable(state.redis_host, state.redis_port) else 0.0)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/predict", response_model=PredictionResponse)
def predict(
    transaction_request: TransactionRequest,
    prediction_service: PredictionService = Depends(get_prediction_service),  # noqa: B008
) -> PredictionResponse:
    request_id = uuid.uuid4().hex

    try:
        transaction = transaction_from_dict(transaction_request.model_dump())
    except InvalidTransactionError as exc:
        PREDICTION_REQUESTS_TOTAL.labels(outcome="invalid_request").inc()
        PREDICTION_ERRORS_TOTAL.labels(reason="invalid_request").inc()
        logger.error(
            "prediction request rejected: invalid transaction",
            extra={"request_id": request_id, "error": str(exc)},
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        response = prediction_service.predict(transaction)
    except FeatureStoreError as exc:
        PREDICTION_REQUESTS_TOTAL.labels(outcome="features_unavailable").inc()
        PREDICTION_ERRORS_TOTAL.labels(reason="features_unavailable").inc()
        logger.error(
            "prediction request failed: features unavailable",
            extra={"request_id": request_id, "error": str(exc)},
        )
        raise HTTPException(
            status_code=503, detail=f"Online features not available yet: {exc}"
        ) from exc

    PREDICTION_REQUESTS_TOTAL.labels(outcome="success").inc()
    PREDICTION_LATENCY_SECONDS.observe(response.latency_ms / 1000)
    MODEL_PREDICTIONS_TOTAL.labels(prediction=str(response.prediction)).inc()
    MODEL_FRAUD_PROBABILITY.observe(response.fraud_probability)

    logger.info(
        "prediction request served",
        extra={
            "request_id": request_id,
            "prediction": response.prediction,
            "fraud_probability": response.fraud_probability,
            "model_version": response.model_version,
            "latency_ms": response.latency_ms,
        },
    )
    return response
