"""MLflow Model Registry integration.

Named `model_registry` (not `registry`) to avoid confusion with
`fraud_detection.features.registry`, which is a different concept (a
static definition of what features exist, not a versioned model store).
"""

from __future__ import annotations

import mlflow
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
