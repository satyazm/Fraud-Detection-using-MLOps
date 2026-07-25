"""Train and compare candidate models, logging every run to MLflow.

Handles class imbalance via class weighting (`class_weight="balanced"`
for scikit-learn estimators, `scale_pos_weight` for XGBoost/LightGBM)
rather than resampling — cheap to compute, no synthetic rows, and
scales to the full ~6.4M-row dataset without a separate resampling
pass. Logistic Regression is wrapped in a `Pipeline` with a
`StandardScaler` fit only on the training split, so the fitted scaler
travels with the model artifact rather than living in the shared
feature pipeline (see docs/decisions/0003-shared-feature-pipeline.md —
that ADR deliberately scopes out fitted transforms; this is where one
lives instead, private to a single model).

XGBoost tolerates the raw `negative/positive` ratio as `scale_pos_weight`
fine on this dataset. LightGBM does not: empirically (see
docs/model_report.md), its leaf-wise growth over-corrects at the raw
~774x ratio here and floods predictions with false positives (PR-AUC
0.01 vs XGBoost's 0.998 at the same ratio). A sqrt-dampened weight
(`sqrt(negative/positive)`, a standard mitigation for this exact
failure mode) recovers most of the signal (PR-AUC ~0.51) without
touching XGBoost's already-good behavior.
"""

from __future__ import annotations

import math
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.base import BaseEstimator
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger
from fraud_detection.features.registry import feature_version as compute_feature_version
from fraud_detection.models.dataset import Dataset
from fraud_detection.models.evaluation import EvaluationResult, evaluate_predictions

logger = get_logger(__name__)

RANDOM_STATE = 42
DEFAULT_TRACKING_URI = f"file:{PROJECT_ROOT / 'mlruns'}"
DEFAULT_EXPERIMENT_NAME = "paysim-fraud-detection"

ModelBuilder = Callable[[float], BaseEstimator]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    build: ModelBuilder


@dataclass(frozen=True)
class TrainingRunResult:
    model_name: str
    run_id: str
    validation_metrics: EvaluationResult
    test_metrics: EvaluationResult


def _build_logistic_regression(_scale_pos_weight: float) -> BaseEstimator:
    return Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=200,
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )


def _build_random_forest(_scale_pos_weight: float) -> BaseEstimator:
    return RandomForestClassifier(
        n_estimators=100,
        max_depth=14,
        max_samples=0.3,
        class_weight="balanced",
        n_jobs=-1,
        random_state=RANDOM_STATE,
    )


def _build_xgboost(scale_pos_weight: float) -> BaseEstimator:
    return XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        tree_method="hist",
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )


def _build_lightgbm(scale_pos_weight: float) -> BaseEstimator:
    # sqrt-dampened, not the raw ratio — see module docstring.
    return LGBMClassifier(
        n_estimators=200,
        max_depth=-1,
        learning_rate=0.1,
        scale_pos_weight=math.sqrt(scale_pos_weight),
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbosity=-1,
    )


MODEL_SPECS: tuple[ModelSpec, ...] = (
    ModelSpec("logistic_regression", _build_logistic_regression),
    ModelSpec("random_forest", _build_random_forest),
    ModelSpec("xgboost", _build_xgboost),
    ModelSpec("lightgbm", _build_lightgbm),
)


def select_best_run(results: Sequence[TrainingRunResult]) -> TrainingRunResult:
    """Pick the run with the highest validation average_precision (PR-AUC).

    Selection uses the validation split, never the test split, so the
    reported test metrics stay an honest, unused-for-selection estimate.
    """
    return max(results, key=lambda r: r.validation_metrics.average_precision)


def train_and_compare(
    dataset: Dataset,
    model_specs: Sequence[ModelSpec] = MODEL_SPECS,
    tracking_uri: str = DEFAULT_TRACKING_URI,
    experiment_name: str = DEFAULT_EXPERIMENT_NAME,
) -> list[TrainingRunResult]:
    """Train every spec in `model_specs`, logging params/metrics/artifacts to MLflow."""
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    scale_pos_weight = _compute_scale_pos_weight(dataset.y_train)
    feature_version = compute_feature_version()
    git_commit = get_git_commit_hash()

    results: list[TrainingRunResult] = []
    for spec in model_specs:
        with mlflow.start_run(run_name=spec.name) as run:
            model = spec.build(scale_pos_weight)
            model.fit(dataset.x_train, dataset.y_train)

            validation_metrics = _predict_and_evaluate(
                model, dataset.x_validation, dataset.y_validation
            )
            test_metrics = _predict_and_evaluate(model, dataset.x_test, dataset.y_test)

            mlflow.log_param("model_type", spec.name)
            mlflow.log_param("feature_version", feature_version)
            mlflow.log_param("git_commit", git_commit)
            mlflow.log_params(_flatten_estimator_params(model))
            mlflow.log_metrics(
                {f"val_{k}": v for k, v in validation_metrics.as_metrics_dict().items()}
            )
            mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.as_metrics_dict().items()})
            mlflow.log_dict({"feature_names": list(dataset.feature_names)}, "feature_names.json")
            mlflow.sklearn.log_model(model, artifact_path="model")

            results.append(
                TrainingRunResult(
                    model_name=spec.name,
                    run_id=run.info.run_id,
                    validation_metrics=validation_metrics,
                    test_metrics=test_metrics,
                )
            )
            logger.info(
                "trained model",
                extra={
                    "model": spec.name,
                    "run_id": run.info.run_id,
                    "val_average_precision": validation_metrics.average_precision,
                    "val_roc_auc": validation_metrics.roc_auc,
                },
            )

    return results


def _predict_and_evaluate(
    model: BaseEstimator, x: pd.DataFrame, y: pd.Series[Any]
) -> EvaluationResult:
    predictions = model.predict(x)
    probabilities = model.predict_proba(x)[:, 1]
    return evaluate_predictions(y, predictions, probabilities)


def _compute_scale_pos_weight(y_train: pd.Series[Any]) -> float:
    positive = int((y_train == 1).sum())
    negative = int((y_train == 0).sum())
    return negative / positive if positive else 1.0


def _flatten_estimator_params(model: BaseEstimator) -> dict[str, str]:
    """MLflow params must be strings; drop non-scalar values (e.g. nested estimators)."""
    params = model.get_params(deep=True)
    return {
        k: str(v) for k, v in params.items() if not callable(v) and not hasattr(v, "get_params")
    }


def get_git_commit_hash() -> str:
    try:
        result = subprocess.run(  # noqa: S603 — fixed, non-user-controlled argv
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return "unknown"
