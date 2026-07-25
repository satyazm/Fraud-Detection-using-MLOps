"""Exceptions raised by the model training/evaluation/registry layer."""


class ModelError(Exception):
    """Base class for all model-layer errors."""


class ModelTrainingError(ModelError):
    """Raised when a model can't be trained because its inputs are invalid."""


class ModelRegistryError(ModelError):
    """Raised when a model can't be registered in or loaded from MLflow."""
