"""Tests for the model comparison report renderer."""

from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.reporting import plot_model_comparison, render_model_report
from fraud_detection.models.training import select_best_run, train_and_compare


def test_render_model_report_and_plot(tmp_path, processed_dir, mlflow_tracking_uri):
    dataset = load_dataset(processed_dir)
    results = train_and_compare(
        dataset, tracking_uri=mlflow_tracking_uri, experiment_name="test-experiment"
    )
    best = select_best_run(results)

    image_path = plot_model_comparison(results, output_path=tmp_path / "images" / "cmp.png")
    report_path = render_model_report(
        results,
        best,
        feature_version="abc123",
        git_commit="deadbeef",
        registered_model_name="test-model",
        registered_model_version="1",
        output_path=tmp_path / "model_report.md",
    )

    assert image_path.exists()
    assert report_path.exists()

    text = report_path.read_text()
    assert best.model_name in text
    assert "abc123" in text
    assert "deadbeef" in text
    assert "average_precision" in text
