"""Tests for FeaturePipeline: the shared offline/online feature entry point."""

from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.features.registry import feature_names


def test_transform_adds_all_registered_features(sample_transactions_df):
    pipeline = FeaturePipeline()

    result = pipeline.transform(sample_transactions_df)

    for name in feature_names():
        assert name in result.columns
    assert len(result) == len(sample_transactions_df)


def test_transform_preserves_original_columns(sample_transactions_df):
    pipeline = FeaturePipeline()
    original_columns = set(sample_transactions_df.columns)

    result = pipeline.transform(sample_transactions_df)

    assert original_columns.issubset(set(result.columns))


def test_transform_one_matches_transform_for_the_same_row(sample_transactions_df):
    """The whole point of transform_one: it must never disagree with transform."""
    pipeline = FeaturePipeline()
    row_dict = sample_transactions_df.iloc[0].drop(labels=["isFraud"]).to_dict()
    transaction = transaction_from_dict(row_dict)

    online_features = pipeline.transform_one(transaction)
    batch_row = pipeline.transform(sample_transactions_df).iloc[0]

    for name in feature_names():
        assert online_features[name] == batch_row[name]
