"""Tests for train_and_compare: the core Milestone 3 training loop."""

from mlflow.tracking import MlflowClient

from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.training import MODEL_SPECS, select_best_run, train_and_compare


def test_train_and_compare_trains_every_spec(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)

    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )

    assert [r.model_name for r in results] == [spec.name for spec in MODEL_SPECS]
    for result in results:
        assert 0.0 <= result.validation_metrics.average_precision <= 1.0
        assert 0.0 <= result.test_metrics.average_precision <= 1.0


def test_train_and_compare_logs_feature_version_and_git_commit(processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)

    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )

    client = MlflowClient(tracking_uri=mlflow_tracking_uri)
    run = client.get_run(results[0].run_id)

    assert "feature_version" in run.data.params
    assert "git_commit" in run.data.params
    assert run.data.params["model_type"] == results[0].model_name


def test_select_best_run_picks_highest_validation_average_precision(
    processed_dir, mlflow_tracking_uri
):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )

    best = select_best_run(results)

    assert best.validation_metrics.average_precision == max(
        r.validation_metrics.average_precision for r in results
    )
