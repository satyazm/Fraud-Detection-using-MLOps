"""Tests for evaluation metrics."""

import numpy as np

from fraud_detection.models.evaluation import evaluate_predictions, results_to_frame


def test_evaluate_predictions_perfect_classifier():
    y_true = np.array([0, 0, 1, 1])
    y_pred = np.array([0, 0, 1, 1])
    y_proba = np.array([0.1, 0.2, 0.9, 0.95])

    result = evaluate_predictions(y_true, y_pred, y_proba)

    assert result.accuracy == 1.0
    assert result.precision == 1.0
    assert result.recall == 1.0
    assert result.f1 == 1.0
    assert result.roc_auc == 1.0
    assert result.average_precision == 1.0
    assert (result.true_positives, result.true_negatives) == (2, 2)
    assert (result.false_positives, result.false_negatives) == (0, 0)


def test_evaluate_predictions_all_wrong():
    y_true = np.array([0, 0, 1, 1])
    y_pred = np.array([1, 1, 0, 0])
    y_proba = np.array([0.9, 0.8, 0.1, 0.2])

    result = evaluate_predictions(y_true, y_pred, y_proba)

    assert result.accuracy == 0.0
    assert result.precision == 0.0
    assert result.recall == 0.0


def test_evaluate_predictions_never_predicting_positive_does_not_raise():
    """precision_score/recall_score would warn+raise-adjacent on 0/0 without zero_division=0."""
    y_true = np.array([0, 0, 0, 1])
    y_pred = np.array([0, 0, 0, 0])
    y_proba = np.array([0.1, 0.2, 0.1, 0.3])

    result = evaluate_predictions(y_true, y_pred, y_proba)

    assert result.precision == 0.0
    assert result.recall == 0.0
    assert result.true_positives == 0
    assert result.false_negatives == 1


def test_as_metrics_dict_values_are_all_floats():
    y_true = np.array([0, 1])
    y_pred = np.array([0, 1])
    y_proba = np.array([0.1, 0.9])

    result = evaluate_predictions(y_true, y_pred, y_proba)
    metrics = result.as_metrics_dict()

    assert all(isinstance(v, float) for v in metrics.values())
    assert "average_precision" in metrics


def test_results_to_frame_has_one_row_per_model():
    y_true = np.array([0, 1])
    y_pred = np.array([0, 1])
    y_proba = np.array([0.1, 0.9])
    result = evaluate_predictions(y_true, y_pred, y_proba)

    frame = results_to_frame({"model_a": result, "model_b": result})

    assert list(frame.index) == ["model_a", "model_b"]
    assert "average_precision" in frame.columns
