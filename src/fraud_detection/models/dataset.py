"""Loads processed PaySim splits into X/y ready for model training.

Expects `data/processed/{train,validation,test}.parquet` as produced by
`fraud-detection preprocess` — which already runs the shared
`FeaturePipeline` before dropping identifiers/encoding `type` (see
docs/decisions/0003-shared-feature-pipeline.md), so every column in
these files except the label is a usable feature. No feature logic
lives here; this module only assembles what preprocessing already
produced.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.models.exceptions import ModelTrainingError

TARGET_COLUMN = "isFraud"
DEFAULT_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


@dataclass(frozen=True)
class Dataset:
    x_train: pd.DataFrame
    y_train: pd.Series[Any]
    x_validation: pd.DataFrame
    y_validation: pd.Series[Any]
    x_test: pd.DataFrame
    y_test: pd.Series[Any]
    feature_names: tuple[str, ...]


def load_dataset(processed_dir: Path | str = DEFAULT_PROCESSED_DIR) -> Dataset:
    """Load train/validation/test splits and separate features from the label.

    Raises:
        ModelTrainingError: If any split file is missing (run
            `fraud-detection preprocess` first) or the label column
            isn't present.
    """
    directory = Path(processed_dir)
    train_df = _read_split(directory / "train.parquet")
    validation_df = _read_split(directory / "validation.parquet")
    test_df = _read_split(directory / "test.parquet")

    for name, df in (("train", train_df), ("validation", validation_df), ("test", test_df)):
        if TARGET_COLUMN not in df.columns:
            raise ModelTrainingError(f"'{TARGET_COLUMN}' column missing from {name} split")

    feature_names = tuple(c for c in train_df.columns if c != TARGET_COLUMN)

    return Dataset(
        x_train=_numeric_features(train_df, feature_names),
        y_train=train_df[TARGET_COLUMN],
        x_validation=_numeric_features(validation_df, feature_names),
        y_validation=validation_df[TARGET_COLUMN],
        x_test=_numeric_features(test_df, feature_names),
        y_test=test_df[TARGET_COLUMN],
        feature_names=feature_names,
    )


def _read_split(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise ModelTrainingError(
            f"Processed split not found at {path}; run `fraud-detection preprocess` first"
        )
    return pd.read_parquet(path)


def _numeric_features(df: pd.DataFrame, feature_names: tuple[str, ...]) -> pd.DataFrame:
    """Select feature columns and normalize dtypes models can consume.

    `pandas.get_dummies` (used by `data.preprocessing`) produces `bool`
    columns; cast those to `int64` so every estimator we train sees
    plain numeric input regardless of which dtype the parquet file
    happened to store.
    """
    features = df[list(feature_names)]
    bool_columns = features.select_dtypes(include="bool").columns
    if len(bool_columns) > 0:
        features = features.astype(dict.fromkeys(bool_columns, "int64"))
    return features
