"""End-to-end integration test: real MLflow (isolated store) + real Feast/Redis.

Trains a tiny real model, registers + promotes it to "Production" in an
isolated MLflow file store (never the real mlruns/), writes its
engineered features into the real Redis online store via
`FeastFeatureStore`, then drives the actual FastAPI app through
`TestClient` end to end and cross-checks the response against calling
`PredictionService` directly for the same transaction — the API isn't a
second inference implementation, so the two must match exactly (not
`pytest.approx`), the same bar Milestone 5's own Feast tests hold
themselves to.

Needs real Redis (skips cleanly otherwise, like the rest of the
Feast-dependent suite); MLflow itself needs no server (file store).
"""

from __future__ import annotations

import uuid

import mlflow.sklearn
from fastapi.testclient import TestClient
from mlflow.tracking import MlflowClient

from fraud_detection.api.app import create_app
from fraud_detection.api.prediction_service import PredictionService
from fraud_detection.domain.schemas import transaction_from_dict, transaction_to_dict
from fraud_detection.features.entity_key import compute_entity_id
from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.features.registry import feature_names
from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.model_registry import load_feature_names, register_model
from fraud_detection.models.training import select_best_run, train_and_compare

from ..features.conftest import applied_feast_repo, requires_redis  # noqa: F401 — fixtures

pytestmark = requires_redis


def test_predict_end_to_end_matches_prediction_service_directly(
    processed_dir,
    mlflow_tracking_uri,
    sample_transactions_df,
    applied_feast_repo,  # noqa: F811 — pytest fixture, not a redefinition of the import above
    tmp_path,
):
    dataset = load_dataset(processed_dir)
    model_name = f"integration-test-model-{uuid.uuid4().hex[:8]}"
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="api-integration-test"
    )
    best = select_best_run(results)
    version = register_model(best.run_id, model_name=model_name)
    MlflowClient().transition_model_version_stage(
        name=model_name, version=version.version, stage="Production"
    )

    raw_row = sample_transactions_df.iloc[[0]].reset_index(drop=True)
    transaction = transaction_from_dict(raw_row.iloc[0].to_dict())
    featurized = FeaturePipeline().transform(raw_row)
    engineered_features = {name: featurized.iloc[0][name] for name in feature_names()}

    feast_store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH, offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH
    )
    entity_id = compute_entity_id(transaction)
    feast_store.write_online(entity_id, engineered_features)

    model = mlflow.sklearn.load_model(f"models:/{model_name}/{version.version}")
    feature_order = load_feature_names(best.run_id)
    direct_result = PredictionService(
        model=model,
        model_version=str(version.version),
        feature_order=feature_order,
        feature_store=feast_store,
        prediction_log_path=tmp_path / "direct_prediction_log.jsonl",
    ).predict(transaction)

    app = create_app(
        model_name=model_name,
        tracking_uri=mlflow_tracking_uri,
        prediction_log_path=tmp_path / "api_prediction_log.jsonl",
    )
    with TestClient(app) as client:
        ready = client.get("/ready").json()
        assert ready["checks"]["model_loaded"] is True
        assert ready["checks"]["redis_reachable"] is True

        response = client.post("/predict", json=transaction_to_dict(transaction))

    assert response.status_code == 200
    body = response.json()
    assert body["prediction"] == direct_result.prediction
    assert body["fraud_probability"] == direct_result.fraud_probability
    assert body["model_version"] == direct_result.model_version == str(version.version)
