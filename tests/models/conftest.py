"""Shared fixtures for model-layer tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from fraud_detection.data.preprocessing import preprocess
from fraud_detection.data.split import stratified_split
from fraud_detection.features.feature_pipeline import FeaturePipeline


@pytest.fixture
def processed_dir(tmp_path: Path, sample_transactions_df) -> Path:
    """A tiny train/validation/test parquet trio, built the same way
    `fraud-detection preprocess` builds the real ones (features ->
    preprocess -> stratified split), just on the small synthetic
    fixture instead of the full PaySim CSV.
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
