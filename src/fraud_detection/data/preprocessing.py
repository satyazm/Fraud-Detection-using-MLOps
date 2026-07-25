"""Preprocessing for the PaySim dataset.

Deliberately minimal for Phase 2: drop identifier columns that don't
generalize and one-hot encode the categorical `type` column. Feature
engineering proper (Phase 3) builds on top of this output rather than
replacing it. Raw data is never mutated in place — `preprocess` always
returns a new dataframe.
"""

from __future__ import annotations

import pandas as pd

from fraud_detection.common.logger import get_logger

logger = get_logger(__name__)

# High-cardinality identifiers: not predictive on their own and don't
# generalize to transactions/accounts unseen during training.
COLUMNS_TO_DROP: tuple[str, ...] = ("nameOrig", "nameDest")

CATEGORICAL_COLUMN = "type"


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned copy of `df`; never mutates the input."""
    processed = df.copy()
    processed = _drop_identifier_columns(processed)
    processed = _encode_transaction_type(processed)

    logger.info(
        "preprocessing complete",
        extra={"rows": len(processed), "columns": len(processed.columns)},
    )
    return processed


def _drop_identifier_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = [c for c in COLUMNS_TO_DROP if c in df.columns]
    return df.drop(columns=columns)


def _encode_transaction_type(df: pd.DataFrame) -> pd.DataFrame:
    dummies = pd.get_dummies(df[CATEGORICAL_COLUMN], prefix=CATEGORICAL_COLUMN)
    return pd.concat([df.drop(columns=[CATEGORICAL_COLUMN]), dummies], axis=1)
