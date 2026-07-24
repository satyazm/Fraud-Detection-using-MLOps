"""(De)serialization between domain entities and plain dicts/JSON.

Kept separate from `entities.py` so entities stay free of serialization
concerns; this module is the only place that needs to know about wire
formats (PaySim rows, Kafka JSON payloads, API bodies). The dict shape
matches `data/contracts/transaction_schema.json`.
"""

from __future__ import annotations

from typing import Any

from fraud_detection.domain.entities import Transaction, TransactionType
from fraud_detection.domain.exceptions import InvalidTransactionError


def transaction_from_dict(data: dict[str, Any]) -> Transaction:
    """Build a Transaction from a raw dict (PaySim row or Kafka payload).

    Raises:
        InvalidTransactionError: If a required field is missing or has
            a value that doesn't satisfy the domain contract.
    """
    try:
        return Transaction(
            step=int(data["step"]),
            type=TransactionType(data["type"]),
            amount=float(data["amount"]),
            name_orig=str(data["nameOrig"]),
            oldbalance_org=float(data["oldbalanceOrg"]),
            newbalance_orig=float(data["newbalanceOrig"]),
            name_dest=str(data["nameDest"]),
            oldbalance_dest=float(data["oldbalanceDest"]),
            newbalance_dest=float(data["newbalanceDest"]),
            is_flagged_fraud=bool(data.get("isFlaggedFraud", 0)),
        )
    except (KeyError, ValueError, TypeError) as exc:
        raise InvalidTransactionError(f"Invalid transaction payload: {exc}") from exc


def transaction_to_dict(transaction: Transaction) -> dict[str, Any]:
    """Serialize a Transaction back to the PaySim/Kafka wire format."""
    return {
        "step": transaction.step,
        "type": transaction.type.value,
        "amount": transaction.amount,
        "nameOrig": transaction.name_orig,
        "oldbalanceOrg": transaction.oldbalance_org,
        "newbalanceOrig": transaction.newbalance_orig,
        "nameDest": transaction.name_dest,
        "oldbalanceDest": transaction.oldbalance_dest,
        "newbalanceDest": transaction.newbalance_dest,
        "isFlaggedFraud": int(transaction.is_flagged_fraud),
    }
