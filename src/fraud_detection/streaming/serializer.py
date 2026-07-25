"""JSON (de)serialization between Kafka message bytes and the domain
`Transaction` entity.

Deliberately thin: both directions delegate to
`fraud_detection.domain.schemas`, the same module `data.ingestion` and
`models` already go through. Producer and consumer therefore share
exactly one definition of "what a transaction is" — the domain entity —
rather than a second, Kafka-specific schema. See
docs/decisions/0005-reuse-domain-entity-for-kafka-messages.md.
"""

from __future__ import annotations

import json

from fraud_detection.domain.entities import Transaction
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.domain.schemas import transaction_from_dict, transaction_to_dict


def serialize_transaction(transaction: Transaction) -> bytes:
    """Encode a Transaction as the JSON bytes published to Kafka."""
    return json.dumps(transaction_to_dict(transaction)).encode("utf-8")


def deserialize_transaction(payload: bytes) -> Transaction:
    """Decode a Kafka message value back into a Transaction.

    Raises:
        InvalidTransactionError: If `payload` isn't valid JSON, or the
            decoded object doesn't satisfy the domain contract.
    """
    try:
        data = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidTransactionError(f"Invalid transaction payload: {exc}") from exc

    if not isinstance(data, dict):
        raise InvalidTransactionError(f"Expected a JSON object, got {type(data).__name__}")

    return transaction_from_dict(data)
