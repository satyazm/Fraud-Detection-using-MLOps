"""Tests for MLflow Model Registry integration."""

import mlflow
import pytest

from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.exceptions import ModelRegistryError
from fraud_detection.models.model_registry import register_model, resolve_latest_model_uri
from fraud_detection.models.training import train_and_compare


def test_register_and_resolve_latest_model_uri(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )

    version = register_model(results[0].run_id, model_name="test-model")
    resolved_uri = resolve_latest_model_uri("test-model")

    assert resolved_uri == f"models:/test-model/{version.version}"


def test_resolve_latest_model_uri_raises_when_nothing_registered(mlflow_tracking_uri):
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    with pytest.raises(ModelRegistryError):
        resolve_latest_model_uri("does-not-exist")
