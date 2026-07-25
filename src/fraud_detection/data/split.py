"""Stratified train/validation/test splitting.

Fraud is a small fraction of PaySim transactions, so splits are
stratified on `isFraud` to keep that ratio consistent across train,
validation, and test rather than letting a naive random split starve
one of them of fraud examples.
"""

from __future__ import annotations

from typing import NamedTuple

import pandas as pd
from sklearn.model_selection import train_test_split

from fraud_detection.data.exceptions import DataSplitError

TARGET_COLUMN = "isFraud"


class DatasetSplit(NamedTuple):
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame


def stratified_split(
    df: pd.DataFrame,
    train_size: float = 0.7,
    validation_size: float = 0.15,
    random_state: int = 42,
) -> DatasetSplit:
    """Split `df` into stratified train/validation/test sets.

    `test_size` is implied as `1 - train_size - validation_size`.

    Raises:
        DataSplitError: If `TARGET_COLUMN` is missing, or the requested
            sizes don't leave room for a non-empty test split.
    """
    if TARGET_COLUMN not in df.columns:
        raise DataSplitError(f"Cannot stratify: '{TARGET_COLUMN}' column not present")
    if not 0 < train_size < 1:
        raise DataSplitError(f"train_size must be between 0 and 1, got {train_size}")

    remainder_size = 1 - train_size
    if not 0 < validation_size < remainder_size:
        raise DataSplitError(
            f"validation_size ({validation_size}) must be > 0 and leave room for a "
            f"non-empty test split (remainder after train_size is {remainder_size})"
        )

    train_df, remainder_df = train_test_split(
        df,
        train_size=train_size,
        stratify=df[TARGET_COLUMN],
        random_state=random_state,
    )

    validation_fraction_of_remainder = validation_size / remainder_size
    validation_df, test_df = train_test_split(
        remainder_df,
        train_size=validation_fraction_of_remainder,
        stratify=remainder_df[TARGET_COLUMN],
        random_state=random_state,
    )

    return DatasetSplit(train=train_df, validation=validation_df, test=test_df)
