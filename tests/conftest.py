"""Shared fixtures for the test suite."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from fraud_detection.data.preprocessing import preprocess
from fraud_detection.data.split import stratified_split
from fraud_detection.features.feature_pipeline import FeaturePipeline

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


@pytest.fixture
def processed_dir(tmp_path: Path, sample_transactions_df) -> Path:
    """A tiny train/validation/test parquet trio, built the same way
    `fraud-detection preprocess` builds the real ones (features ->
    preprocess -> stratified split), just on the small synthetic
    fixture instead of the full PaySim CSV. Used by both tests/models/
    (training/registry) and tests/api/ (serving) — both need a real,
    tiny processed dataset to train a real model against.
    """
    featurized = FeaturePipeline().transform(sample_transactions_df)
    processed = preprocess(featurized)
    split = stratified_split(processed, random_state=0)

    directory = tmp_path / "processed"
    directory.mkdir()
    for name, subset in split._asdict().items():
        subset.to_parquet(directory / f"{name}.parquet", index=False)

    return directory


@pytest.fixture
def mlflow_tracking_uri(tmp_path: Path) -> str:
    """An isolated, per-test MLflow file store — never the real mlruns/."""
    return f"file:{tmp_path / 'mlruns'}"
