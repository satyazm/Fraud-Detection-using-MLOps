"""Tests for the local FeatureStore implementation."""

import pytest

from fraud_detection.features.feature_store import FeatureStoreError, LocalFeatureStore


def test_write_and_read_offline_batch_round_trips(tmp_path, sample_transactions_df):
    store = LocalFeatureStore(offline_path=tmp_path / "features.parquet")

    store.write_offline_batch(sample_transactions_df)
    result = store.read_offline_batch()

    assert len(result) == len(sample_transactions_df)
    assert set(result.columns) == set(sample_transactions_df.columns)


def test_read_offline_batch_raises_when_nothing_written(tmp_path):
    store = LocalFeatureStore(offline_path=tmp_path / "missing.parquet")

    with pytest.raises(FeatureStoreError):
        store.read_offline_batch()


def test_write_and_read_online_round_trips(tmp_path):
    store = LocalFeatureStore(offline_path=tmp_path / "features.parquet")

    store.write_online("txn-1", {"amount_to_orig_balance_ratio": 0.5})
    result = store.read_online("txn-1")

    assert result == {"amount_to_orig_balance_ratio": 0.5}


def test_read_online_raises_for_unknown_entity(tmp_path):
    store = LocalFeatureStore(offline_path=tmp_path / "features.parquet")

    with pytest.raises(FeatureStoreError):
        store.read_online("does-not-exist")
