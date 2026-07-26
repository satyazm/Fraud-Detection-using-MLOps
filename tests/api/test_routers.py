"""API-level tests: TestClient + `dependency_overrides`.

No real MLflow/Feast/Redis needed — `load_app_state` is monkeypatched
to a not-ready `AppState` at startup (so the lifespan never touches
real MLflow), and individual tests override `get_app_state`/
`get_prediction_service` for the scenario they want to exercise.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fraud_detection.api.app import create_app
from fraud_detection.api.dependencies import AppState, get_app_state, get_prediction_service
from fraud_detection.api.schemas import PredictionResponse
from fraud_detection.features.feature_store import FeatureStoreError

VALID_TRANSACTION = {
    "step": 1,
    "type": "TRANSFER",
    "amount": 181.0,
    "nameOrig": "C1231006815",
    "oldbalanceOrg": 181.0,
    "newbalanceOrig": 0.0,
    "nameDest": "C1666544295",
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
}

_NOT_READY_STATE = AppState(
    prediction_service=None,
    model_version=None,
    feast_store=None,
    redis_host="localhost",
    redis_port=6379,
)


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setattr("fraud_detection.api.app.load_app_state", lambda *a, **k: _NOT_READY_STATE)
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_always_ok(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_reports_not_ready_by_default(client: TestClient) -> None:
    response = client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["ready"] is False
    assert body["checks"]["model_loaded"] is False
    assert body["checks"]["feast_reachable"] is False


def test_ready_reports_model_and_feast_loaded_when_overridden(client: TestClient) -> None:
    ready_state = AppState(
        prediction_service=object(),  # only truthiness matters to /ready
        model_version="1",
        feast_store=object(),
        redis_host="localhost",
        redis_port=6379,
    )
    client.app.dependency_overrides[get_app_state] = lambda: ready_state

    response = client.get("/ready")

    body = response.json()
    assert body["checks"]["model_loaded"] is True
    assert body["checks"]["feast_reachable"] is True
    # redis_reachable is a live socket check, deliberately not mockable
    # through AppState — it reflects whether Redis is actually up.


def test_predict_returns_503_when_model_not_loaded(client: TestClient) -> None:
    response = client.post("/predict", json=VALID_TRANSACTION)

    assert response.status_code == 503


class _UnusedService:
    """A ready-but-unreachable-in-practice stub: proves validation errors
    (422) are reported even when the service *is* ready, distinct from
    the not-ready-service case (503) covered above — `.predict()` must
    never actually be called for these two tests, since request
    validation should reject the payload before the route body runs."""

    def predict(self, transaction: object) -> PredictionResponse:
        raise AssertionError("predict() should not be called for an invalid request")


def test_predict_rejects_invalid_transaction_type(client: TestClient) -> None:
    client.app.dependency_overrides[get_prediction_service] = lambda: _UnusedService()

    response = client.post("/predict", json={**VALID_TRANSACTION, "type": "NOT_A_TYPE"})

    assert response.status_code == 422


def test_predict_rejects_missing_field(client: TestClient) -> None:
    client.app.dependency_overrides[get_prediction_service] = lambda: _UnusedService()
    incomplete = {k: v for k, v in VALID_TRANSACTION.items() if k != "amount"}

    response = client.post("/predict", json=incomplete)

    assert response.status_code == 422


def test_predict_returns_200_with_stubbed_service(client: TestClient) -> None:
    class _StubService:
        def predict(self, transaction: object) -> PredictionResponse:
            return PredictionResponse(
                prediction=1, fraud_probability=0.99, model_version="7", latency_ms=1.23
            )

    client.app.dependency_overrides[get_prediction_service] = lambda: _StubService()

    response = client.post("/predict", json=VALID_TRANSACTION)

    assert response.status_code == 200
    body = response.json()
    assert body["prediction"] == 1
    assert body["fraud_probability"] == 0.99
    assert body["model_version"] == "7"
    assert body["latency_ms"] == 1.23


def test_predict_returns_503_when_features_unavailable(client: TestClient) -> None:
    class _StubService:
        def predict(self, transaction: object) -> PredictionResponse:
            raise FeatureStoreError("no features yet")

    client.app.dependency_overrides[get_prediction_service] = lambda: _StubService()

    response = client.post("/predict", json=VALID_TRANSACTION)

    assert response.status_code == 503


def test_metrics_exposes_expected_metric_names(client: TestClient) -> None:
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    for name in (
        "prediction_requests_total",
        "prediction_latency_seconds",
        "prediction_errors_total",
        "model_predictions_total",
        "model_fraud_probability",
        "redis_connection_status",
    ):
        assert name in body


def test_metrics_reflects_a_successful_prediction(client: TestClient) -> None:
    class _StubService:
        def predict(self, transaction: object) -> PredictionResponse:
            return PredictionResponse(
                prediction=1, fraud_probability=0.9, model_version="1", latency_ms=5.0
            )

    client.app.dependency_overrides[get_prediction_service] = lambda: _StubService()
    client.post("/predict", json=VALID_TRANSACTION)

    body = client.get("/metrics").text

    assert 'prediction_requests_total{outcome="success"}' in body
    assert 'model_predictions_total{prediction="1"}' in body
