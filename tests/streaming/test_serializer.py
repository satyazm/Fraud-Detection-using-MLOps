"""Tests for Kafka message (de)serialization."""

import json

import pytest

from fraud_detection.domain.entities import Transaction, TransactionType
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.streaming.serializer import deserialize_transaction, serialize_transaction

SAMPLE_TRANSACTION = Transaction(
    step=1,
    type=TransactionType.PAYMENT,
    amount=9839.64,
    name_orig="C1231006815",
    oldbalance_org=170136.0,
    newbalance_orig=160296.36,
    name_dest="M1979787155",
    oldbalance_dest=0.0,
    newbalance_dest=0.0,
)


def test_serialize_then_deserialize_round_trips():
    payload = serialize_transaction(SAMPLE_TRANSACTION)
    result = deserialize_transaction(payload)

    assert result == SAMPLE_TRANSACTION


def test_serialize_produces_the_paysim_wire_shape():
    payload = serialize_transaction(SAMPLE_TRANSACTION)
    decoded = json.loads(payload)

    assert decoded["type"] == "PAYMENT"
    assert decoded["nameOrig"] == "C1231006815"
    assert decoded["nameDest"] == "M1979787155"


def test_deserialize_raises_for_invalid_json():
    with pytest.raises(InvalidTransactionError):
        deserialize_transaction(b"not json")


def test_deserialize_raises_for_json_that_is_not_an_object():
    with pytest.raises(InvalidTransactionError):
        deserialize_transaction(b"[1, 2, 3]")


def test_deserialize_raises_for_missing_required_field():
    payload = b'{"step": 1, "type": "PAYMENT"}'

    with pytest.raises(InvalidTransactionError):
        deserialize_transaction(payload)
