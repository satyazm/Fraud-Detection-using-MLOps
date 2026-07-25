"""Exceptions raised by the data ingestion/validation/preprocessing pipeline.

Kept separate from `fraud_detection.domain.exceptions`: these are
dataset-level infrastructure errors (a whole CSV failing schema
validation), not single-entity business-rule violations.
"""


class DataError(Exception):
    """Base class for all data-layer errors."""


class DataIngestionError(DataError):
    """Raised when the raw dataset can't be loaded or fails schema validation."""


class DataValidationError(DataError):
    """Raised when a data-quality check fails in a way that should halt the pipeline."""


class DataSplitError(DataError):
    """Raised when train/validation/test splitting can't be performed safely."""
