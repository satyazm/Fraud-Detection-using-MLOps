"""Tests for stratified train/validation/test splitting."""

import pytest

from fraud_detection.data.exceptions import DataSplitError
from fraud_detection.data.split import temporal_split


def test_temporal_split_preserves_total_row_count(sample_transactions_df):
    split = temporal_split(sample_transactions_df)

    total = len(split.train) + len(split.validation) + len(split.test)
    assert total == len(sample_transactions_df)


def test_temporal_split_gives_every_split_fraud_examples(sample_transactions_df):
    split = temporal_split(sample_transactions_df)

    for subset in (split.train, split.validation, split.test):
        assert subset["isFraud"].sum() > 0


def test_temporal_split_raises_without_target_column(sample_transactions_df):
    df = sample_transactions_df.drop(columns=["isFraud"])

    with pytest.raises(DataSplitError):
        temporal_split(df)


def test_temporal_split_raises_for_invalid_sizes(sample_transactions_df):
    with pytest.raises(DataSplitError):
        temporal_split(sample_transactions_df, train_size=0.9, validation_size=0.2)


def test_temporal_split_orders_splits_chronologically(sample_transactions_df):
    split = temporal_split(sample_transactions_df)

    assert split.train["step"].max() < split.validation["step"].min()
    assert split.validation["step"].max() < split.test["step"].min()
