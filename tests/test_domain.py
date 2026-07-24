"""Tests for domain entities and (de)serialization."""

import pytest

from fraud_detection.domain.entities import TransactionType
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.domain.schemas import transaction_from_dict, transaction_to_dict

RAW_TRANSACTION = {
    "step": 1,
    "type": "PAYMENT",
    "amount": 9839.64,
    "nameOrig": "C1231006815",
    "oldbalanceOrg": 170136.0,
    "newbalanceOrig": 160296.36,
    "nameDest": "M1979787155",
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
    "isFlaggedFraud": 0,
}


def test_transaction_from_dict_round_trips_to_same_dict():
    transaction = transaction_from_dict(RAW_TRANSACTION)

    assert transaction.type == TransactionType.PAYMENT
    assert transaction_to_dict(transaction) == RAW_TRANSACTION


def test_transaction_from_dict_raises_for_missing_field():
    incomplete = dict(RAW_TRANSACTION)
    del incomplete["amount"]

    with pytest.raises(InvalidTransactionError):
        transaction_from_dict(incomplete)


def test_transaction_from_dict_raises_for_invalid_type():
    invalid = dict(RAW_TRANSACTION, type="NOT_A_TYPE")

    with pytest.raises(InvalidTransactionError):
        transaction_from_dict(invalid)


def test_transaction_is_immutable():
    transaction = transaction_from_dict(RAW_TRANSACTION)

    with pytest.raises(AttributeError):
        transaction.amount = 0.0  # type: ignore[misc]
