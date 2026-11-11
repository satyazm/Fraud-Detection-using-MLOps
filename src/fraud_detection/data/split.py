"""Time-based train/validation/test splitting.

Transactions are ordered in time (PaySim's `step` is simulated hours),
and production always scores *future* transactions. A random split
puts near-identical, same-account, same-period rows on both sides and
inflates test metrics, so we train on the earliest `step`s, validate on
the next, and test on the latest. Whole steps are never divided across
splits.
"""

from __future__ import annotations

from typing import NamedTuple

import pandas as pd

from fraud_detection.data.exceptions import DataSplitError

TARGET_COLUMN = "isFraud"
TIME_COLUMN = "step"


class DatasetSplit(NamedTuple):
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame


def temporal_split(
    df: pd.DataFrame,
    train_size: float = 0.7,
    validation_size: float = 0.15,
) -> DatasetSplit:
    """Split `df` chronologically by `step` into train/validation/test.

    Cutoffs fall on `step` boundaries, so row fractions are approximate
    (exact when steps are equally populated). `test_size` is implied as
    `1 - train_size - validation_size`.

    Raises:
        DataSplitError: If `step`/`isFraud` is missing, the sizes are
            invalid, or a split ends up empty or without both classes.
    """
    for column in (TIME_COLUMN, TARGET_COLUMN):
        if column not in df.columns:
            raise DataSplitError(f"Cannot split: '{column}' column not present")
    if not 0 < train_size < 1:
        raise DataSplitError(f"train_size must be between 0 and 1, got {train_size}")
    if not 0 < validation_size < 1 - train_size:
        raise DataSplitError(
            f"validation_size ({validation_size}) must be > 0 and leave room for a "
            f"non-empty test split (remainder after train_size is {1 - train_size})"
        )

    steps = df[TIME_COLUMN]
    cumulative = steps.value_counts().sort_index().cumsum() / len(df)
    train_end = cumulative.index[int((cumulative >= train_size).to_numpy().argmax())]
    validation_end = cumulative.index[
        int((cumulative >= train_size + validation_size).to_numpy().argmax())
    ]

    split = DatasetSplit(
        train=df[steps <= train_end],
        validation=df[(steps > train_end) & (steps <= validation_end)],
        test=df[steps > validation_end],
    )
    for name, subset in split._asdict().items():
        classes = subset[TARGET_COLUMN].nunique()
        if subset.empty or classes < 2:
            raise DataSplitError(
                f"{name} split has {len(subset)} rows and {classes} class(es); "
                "need both classes present"
            )
    return split
