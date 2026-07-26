"""Unit tests for monitoring.prediction_log — no real infra needed."""

from __future__ import annotations

import pytest

from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.monitoring.prediction_log import (
    PredictionLogError,
    append_prediction,
    load_predictions,
)

_TRANSACTION_DICT = {
    "step": 1,
    "type": "TRANSFER",
    "amount": 181.0,
    "nameOrig": "C1231006815",
    "oldbalanceOrg": 181.0,
    "newbalanceOrig": 0.0,
    "nameDest": "C1666544295",
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
}


def test_load_predictions_raises_when_nothing_logged(tmp_path):
    with pytest.raises(PredictionLogError):
        load_predictions(tmp_path / "does-not-exist.jsonl")


def test_append_then_load_round_trips(tmp_path):
    log_path = tmp_path / "prediction_log.jsonl"
    transaction = transaction_from_dict(_TRANSACTION_DICT)

    append_prediction(
        transaction, prediction=0, fraud_probability=0.01, model_version="1", log_path=log_path
    )
    append_prediction(
        transaction, prediction=1, fraud_probability=0.98, model_version="1", log_path=log_path
    )

    df = load_predictions(log_path)

    assert len(df) == 2
    assert list(df["prediction"]) == [0, 1]
    assert df["amount"].iloc[0] == 181.0
    assert df["type"].iloc[0] == "TRANSFER"
    assert "logged_at" in df.columns


def test_append_creates_parent_directory(tmp_path):
    log_path = tmp_path / "nested" / "dir" / "prediction_log.jsonl"
    transaction = transaction_from_dict(_TRANSACTION_DICT)

    append_prediction(
        transaction, prediction=0, fraud_probability=0.01, model_version="1", log_path=log_path
    )

    assert log_path.exists()
