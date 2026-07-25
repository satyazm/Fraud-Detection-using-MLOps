"""Shared fixtures for the test suite."""

from __future__ import annotations

import pandas as pd
import pytest

_ROW_COUNT = 60
_FRAUD_COUNT = 15  # generous margin so stratified splits never starve a class in tests


@pytest.fixture
def sample_transactions_df() -> pd.DataFrame:
    """A small synthetic dataframe with PaySim's exact schema.

    Values are made up, not sampled from the real dataset — sufficient
    for exercising ingestion/validation/preprocessing/split logic
    without needing the actual PaySim CSV on disk.
    """
    types = (["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"] * _ROW_COUNT)[:_ROW_COUNT]
    fraud_flags = [1] * _FRAUD_COUNT + [0] * (_ROW_COUNT - _FRAUD_COUNT)

    return pd.DataFrame(
        {
            "step": list(range(1, _ROW_COUNT + 1)),
            "type": types,
            "amount": [100.0 + i for i in range(_ROW_COUNT)],
            "nameOrig": [f"C{i}" for i in range(_ROW_COUNT)],
            "oldbalanceOrg": [1000.0] * _ROW_COUNT,
            "newbalanceOrig": [900.0] * _ROW_COUNT,
            "nameDest": [f"M{i}" for i in range(_ROW_COUNT)],
            "oldbalanceDest": [0.0] * _ROW_COUNT,
            "newbalanceDest": [100.0] * _ROW_COUNT,
            "isFraud": fraud_flags,
            "isFlaggedFraud": [0] * _ROW_COUNT,
        }
    )
