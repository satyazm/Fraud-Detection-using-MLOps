"""Tests for building the Feast offline source parquet."""

import pandas as pd

from fraud_detection.features.entity_key import compute_entity_ids
from fraud_detection.features.feast_prep import build_offline_source
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.features.registry import feature_names


def test_build_offline_source_has_expected_columns(tmp_path, sample_transactions_df):
    featurized = FeaturePipeline().transform(sample_transactions_df)
    output_path = tmp_path / "offline.parquet"

    build_offline_source(featurized, output_path=output_path)
    result = pd.read_parquet(output_path)

    assert list(result.columns) == ["transaction_id", "event_timestamp", *feature_names()]
    assert len(result) == len(sample_transactions_df)


def test_build_offline_source_entity_ids_match_entity_key_module(tmp_path, sample_transactions_df):
    featurized = FeaturePipeline().transform(sample_transactions_df)
    output_path = tmp_path / "offline.parquet"

    build_offline_source(featurized, output_path=output_path)
    result = pd.read_parquet(output_path)

    expected_ids = compute_entity_ids(featurized)
    assert list(result["transaction_id"]) == list(expected_ids)


def test_build_offline_source_feature_values_match_transform_output(
    tmp_path, sample_transactions_df
):
    featurized = FeaturePipeline().transform(sample_transactions_df)
    output_path = tmp_path / "offline.parquet"

    build_offline_source(featurized, output_path=output_path)
    result = pd.read_parquet(output_path)

    for name in feature_names():
        assert list(result[name]) == list(featurized[name])
