"""Evaluation metrics for a binary fraud classifier.

Shared by training-time validation/test scoring and the standalone
`fraud-detection evaluate` command, so "how we score a model" has one
definition. Accuracy is reported for completeness but is close to
meaningless here (always predicting "not fraud" scores ~99.87%);
`average_precision` (PR-AUC) is the primary metric given the severe
class imbalance, since — unlike ROC-AUC — it isn't inflated by the
large number of easy true negatives.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


@dataclass(frozen=True)
class EvaluationResult:
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float
    average_precision: float
    true_negatives: int
    false_positives: int
    false_negatives: int
    true_positives: int

    def as_metrics_dict(self) -> dict[str, float]:
        """Scalar metrics only, suitable for `mlflow.log_metrics`."""
        return {
            "accuracy": self.accuracy,
            "precision": self.precision,
            "recall": self.recall,
            "f1": self.f1,
            "roc_auc": self.roc_auc,
            "average_precision": self.average_precision,
            "true_negatives": float(self.true_negatives),
            "false_positives": float(self.false_positives),
            "false_negatives": float(self.false_negatives),
            "true_positives": float(self.true_positives),
        }


def evaluate_predictions(
    y_true: pd.Series[Any] | np.ndarray[Any, Any],
    y_pred: pd.Series[Any] | np.ndarray[Any, Any],
    y_proba: pd.Series[Any] | np.ndarray[Any, Any],
) -> EvaluationResult:
    """Compute the fixed set of metrics used to compare and report on models.

    Args:
        y_true: Ground-truth labels (0/1).
        y_pred: Predicted labels (0/1) at the model's default threshold.
        y_proba: Predicted probability of the positive (fraud) class.
    """
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    return EvaluationResult(
        accuracy=float(accuracy_score(y_true, y_pred)),
        precision=float(precision_score(y_true, y_pred, zero_division=0)),
        recall=float(recall_score(y_true, y_pred, zero_division=0)),
        f1=float(f1_score(y_true, y_pred, zero_division=0)),
        roc_auc=float(roc_auc_score(y_true, y_proba)),
        average_precision=float(average_precision_score(y_true, y_proba)),
        true_negatives=int(tn),
        false_positives=int(fp),
        false_negatives=int(fn),
        true_positives=int(tp),
    )


def results_to_frame(results: dict[str, EvaluationResult]) -> pd.DataFrame:
    """Convenience: a comparison table, one row per model name -> result."""
    rows: dict[str, dict[str, Any]] = {
        name: result.as_metrics_dict() for name, result in results.items()
    }
    return pd.DataFrame.from_dict(rows, orient="index")
