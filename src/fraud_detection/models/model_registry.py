"""MLflow Model Registry integration.

Named `model_registry` (not `registry`) to avoid confusion with
`fraud_detection.features.registry`, which is a different concept (a
static definition of what features exist, not a versioned model store).
"""

from __future__ import annotations

from dataclasses import dataclass

import mlflow
import mlflow.artifacts
from mlflow.entities.model_registry import ModelVersion
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from fraud_detection.common.logger import get_logger
from fraud_detection.models.exceptions import ModelRegistryError

logger = get_logger(__name__)

DEFAULT_MODEL_NAME = "fraud-detection-classifier"


def register_model(run_id: str, model_name: str = DEFAULT_MODEL_NAME) -> ModelVersion:
    """Register the model artifact logged under `run_id` in the MLflow Model Registry."""
    model_uri = f"runs:/{run_id}/model"
    version = mlflow.register_model(model_uri=model_uri, name=model_name)
    logger.info(
        "registered model",
        extra={"model_name": model_name, "version": version.version, "run_id": run_id},
    )
    return version


def resolve_latest_model_uri(model_name: str = DEFAULT_MODEL_NAME) -> str:
    """Return `models:/{model_name}/{version}` for the highest registered version.

    Raises:
        ModelRegistryError: If no versions of `model_name` are registered.
    """
    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
    except MlflowException as exc:
        raise ModelRegistryError(f"Could not query registry for '{model_name}': {exc}") from exc

    if not versions:
        raise ModelRegistryError(f"No registered versions found for model '{model_name}'")

    latest = max(versions, key=lambda v: int(v.version))
    return f"models:/{model_name}/{latest.version}"


@dataclass(frozen=True)
class ProductionModel:
    """Identifies the registered model version currently in the "Production" stage."""

    uri: str
    version: str
    run_id: str


def resolve_production_model(model_name: str = DEFAULT_MODEL_NAME) -> ProductionModel:
    """Return the `model_name` version currently in the MLflow "Production" stage.

    Serving (Milestone 6) deliberately resolves by stage, not "latest
    version" (`resolve_latest_model_uri`, used by `evaluate`): a freshly
    trained model is registered but not automatically promoted, so a
    human/CI step must move it to "Production" before it's servable —
    the same gate a real deployment would want before any model starts
    scoring live traffic.

    Raises:
        ModelRegistryError: If no version of `model_name` is in the
            Production stage.
    """
    client = MlflowClient()
    try:
        versions = client.get_latest_versions(model_name, stages=["Production"])
    except MlflowException as exc:
        raise ModelRegistryError(f"Could not query registry for '{model_name}': {exc}") from exc

    if not versions:
        raise ModelRegistryError(f"No version of '{model_name}' is in the Production stage")

    version = versions[0]
    return ProductionModel(
        uri=f"models:/{model_name}/{version.version}",
        version=str(version.version),
        run_id=version.run_id,
    )


def load_feature_names(run_id: str) -> tuple[str, ...]:
    """Return the ordered feature columns the training run at `run_id` used.

    Reads the `feature_names.json` artifact `training.train_and_compare`
    already logs for every run, rather than hardcoding a second copy of
    the column list here — the serving layer's input dataframe must
    match this order/set exactly, or the model silently scores garbage.

    Raises:
        ModelRegistryError: If the artifact can't be loaded.
    """
    try:
        data = mlflow.artifacts.load_dict(f"runs:/{run_id}/feature_names.json")
    except (MlflowException, OSError) as exc:
        raise ModelRegistryError(
            f"Could not load feature_names.json for run {run_id}: {exc}"
        ) from exc
    return tuple(data["feature_names"])
