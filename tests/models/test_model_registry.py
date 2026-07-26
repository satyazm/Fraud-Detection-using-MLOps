"""Tests for MLflow Model Registry integration."""

import mlflow
import pytest
from mlflow.tracking import MlflowClient

from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.exceptions import ModelRegistryError
from fraud_detection.models.model_registry import (
    load_feature_names,
    register_model,
    resolve_latest_model_uri,
    resolve_production_model,
)
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


def test_resolve_production_model_raises_when_nothing_promoted(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )
    register_model(results[0].run_id, model_name="test-model-unpromoted")

    with pytest.raises(ModelRegistryError):
        resolve_production_model("test-model-unpromoted")


def test_resolve_production_model_after_stage_transition(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )
    version = register_model(results[0].run_id, model_name="test-model-promoted")
    MlflowClient().transition_model_version_stage(
        name="test-model-promoted", version=version.version, stage="Production"
    )

    production = resolve_production_model("test-model-promoted")

    assert production.uri == f"models:/test-model-promoted/{version.version}"
    assert production.version == str(version.version)
    assert production.run_id == results[0].run_id


def test_load_feature_names_matches_dataset_columns(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )

    feature_names = load_feature_names(results[0].run_id)

    assert feature_names == dataset.feature_names


def test_load_feature_names_raises_for_unknown_run(mlflow_tracking_uri):
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    with pytest.raises(ModelRegistryError):
        load_feature_names("does-not-exist")
