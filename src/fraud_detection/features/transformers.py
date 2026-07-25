"""Pure, stateless feature transformers for PaySim transactions.

Each transformer takes a dataframe with the raw Transaction schema
(`step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`,
`nameDest`, `oldbalanceDest`, `newbalanceDest`, optionally
`isFlaggedFraud`) — the same shape `fraud_detection.data.ingestion` and
`fraud_detection.domain.schemas.transaction_to_dict` both produce — and
returns a new dataframe with additional engineered columns. Existing
columns are never dropped or mutated.

Every feature here is a deterministic function of a single row: no
means/stds/encoders are fit from data. That's what makes it safe for
`FeaturePipeline` (see `feature_pipeline.py`) to run the exact same
function over a batch of six million training rows or over one live
transaction at inference time.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

# Avoids division by zero for accounts with a zero starting balance,
# without distorting the ratio for any account that actually has money.
_AMOUNT_EPSILON = 1.0

Transformer = Callable[[pd.DataFrame], pd.DataFrame]


def add_balance_error_features(df: pd.DataFrame) -> pd.DataFrame:
    """How far each account's stated balances deviate from `amount`.

    PaySim's simulated balances are internally consistent for
    legitimate transactions; a nonzero error is a well-known fraud
    signal in this dataset.
    """
    return df.assign(
        orig_balance_error=df["oldbalanceOrg"] - df["amount"] - df["newbalanceOrig"],
        dest_balance_error=df["oldbalanceDest"] + df["amount"] - df["newbalanceDest"],
    )


def add_balance_delta_features(df: pd.DataFrame) -> pd.DataFrame:
    """Raw change in each account's balance across the transaction."""
    return df.assign(
        orig_balance_delta=df["newbalanceOrig"] - df["oldbalanceOrg"],
        dest_balance_delta=df["newbalanceDest"] - df["oldbalanceDest"],
    )


def add_balance_flag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Binary flags for two account-balance patterns common in fraud."""
    return df.assign(
        is_orig_balance_depleted=((df["oldbalanceOrg"] > 0) & (df["newbalanceOrig"] == 0)).astype(
            int
        ),
        is_dest_balance_untouched=(
            (df["oldbalanceDest"] == 0) & (df["newbalanceDest"] == 0) & (df["amount"] > 0)
        ).astype(int),
    )


def add_amount_ratio_feature(df: pd.DataFrame) -> pd.DataFrame:
    """Transaction size relative to the sender's starting balance."""
    return df.assign(
        amount_to_orig_balance_ratio=df["amount"] / (df["oldbalanceOrg"] + _AMOUNT_EPSILON)
    )


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """`step` is simulated hours elapsed; derive hour-of-day (0-23)."""
    return df.assign(hour_of_day=df["step"] % 24)


def add_merchant_flag_feature(df: pd.DataFrame) -> pd.DataFrame:
    """PaySim prefixes merchant account ids with 'M', customers with 'C'."""
    return df.assign(is_dest_merchant=df["nameDest"].str.startswith("M").astype(int))


DEFAULT_TRANSFORMERS: tuple[Transformer, ...] = (
    add_balance_error_features,
    add_balance_delta_features,
    add_balance_flag_features,
    add_amount_ratio_feature,
    add_temporal_features,
    add_merchant_flag_feature,
)
