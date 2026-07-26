"""Unit tests for PredictionService — pure Python, no FastAPI/Feast/Redis.

Uses `LocalFeatureStore` (the same `FeatureStore` protocol
`FeastFeatureStore` implements) so these tests exercise the real
entity-lookup/feature-merge/reindex logic without needing real infra.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from fraud_detection.api.prediction_service import PredictionService
from fraud_detection.data.preprocessing import preprocess
from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.entity_key import compute_entity_id
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.features.feature_store import FeatureStoreError, LocalFeatureStore
from fraud_detection.features.registry import feature_names as engineered_feature_names


class _StubModel:
    """Records the DataFrame it was called with; returns canned outputs."""

    def __init__(self, prediction: int, probability: float) -> None:
        self._prediction = prediction
        self._probability = probability
        self.received: pd.DataFrame | None = None

    def predict(self, x: pd.DataFrame) -> np.ndarray[Any, Any]:
        self.received = x
        return np.array([self._prediction])

    def predict_proba(self, x: pd.DataFrame) -> np.ndarray[Any, Any]:
        self.received = x
        return np.array([[1 - self._probability, self._probability]])


@pytest.fixture
def transaction_and_features(sample_transactions_df):
    raw_row = sample_transactions_df.iloc[[0]].reset_index(drop=True)
    transaction = transaction_from_dict(raw_row.iloc[0].to_dict())

    featurized = FeaturePipeline().transform(raw_row)
    engineered = {name: featurized.iloc[0][name] for name in engineered_feature_names()}

    return transaction, engineered


@pytest.fixture
def feature_order(sample_transactions_df) -> tuple[str, ...]:
    """The exact column set/order `models.dataset.load_dataset` would produce."""
    featurized = FeaturePipeline().transform(sample_transactions_df)
    processed = preprocess(featurized)
    return tuple(c for c in processed.columns if c != "isFraud")


def test_predict_returns_stub_model_output(transaction_and_features, feature_order, tmp_path):
    transaction, engineered = transaction_and_features
    entity_id = compute_entity_id(transaction)

    store = LocalFeatureStore(offline_path=tmp_path / "unused.parquet")
    store.write_online(entity_id, engineered)

    model = _StubModel(prediction=1, probability=0.87)
    service = PredictionService(
        model=model,
        model_version="3",
        feature_order=feature_order,
        feature_store=store,
        prediction_log_path=tmp_path / "prediction_log.jsonl",
    )

    response = service.predict(transaction)

    assert response.prediction == 1
    assert response.fraud_probability == pytest.approx(0.87)
    assert response.model_version == "3"
    assert response.latency_ms >= 0


def test_predict_builds_row_matching_training_columns(
    transaction_and_features, feature_order, tmp_path
):
    transaction, engineered = transaction_and_features
    entity_id = compute_entity_id(transaction)

    store = LocalFeatureStore(offline_path=tmp_path / "unused.parquet")
    store.write_online(entity_id, engineered)

    model = _StubModel(prediction=0, probability=0.01)
    service = PredictionService(
        model=model,
        model_version="3",
        feature_order=feature_order,
        feature_store=store,
        prediction_log_path=tmp_path / "prediction_log.jsonl",
    )

    service.predict(transaction)

    assert model.received is not None
    assert list(model.received.columns) == list(feature_order)
    assert model.received.select_dtypes(include="bool").empty


def test_predict_raises_when_features_not_in_feast(
    transaction_and_features, feature_order, tmp_path
):
    transaction, _engineered = transaction_and_features
    store = LocalFeatureStore(offline_path=tmp_path / "unused.parquet")

    service = PredictionService(
        model=_StubModel(0, 0.0),
        model_version="3",
        feature_order=feature_order,
        feature_store=store,
        prediction_log_path=tmp_path / "prediction_log.jsonl",
    )

    with pytest.raises(FeatureStoreError):
        service.predict(transaction)
