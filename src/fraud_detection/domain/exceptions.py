"""Domain-level exceptions.

Raised by business logic that is independent of any particular
transport (Kafka, HTTP) or storage technology.
"""


class DomainError(Exception):
    """Base class for all domain-layer errors."""


class InvalidTransactionError(DomainError):
    """Raised when a transaction payload violates the domain contract."""


class ModelNotReadyError(DomainError):
    """Raised when a prediction is requested before a model is loaded."""
