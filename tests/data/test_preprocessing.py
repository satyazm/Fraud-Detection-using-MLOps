"""Tests for the preprocessing pipeline."""

from fraud_detection.data.preprocessing import preprocess


def test_preprocess_drops_identifier_columns(sample_transactions_df):
    processed = preprocess(sample_transactions_df)

    assert "nameOrig" not in processed.columns
    assert "nameDest" not in processed.columns


def test_preprocess_one_hot_encodes_transaction_type(sample_transactions_df):
    processed = preprocess(sample_transactions_df)

    assert "type" not in processed.columns
    assert any(col.startswith("type_") for col in processed.columns)


def test_preprocess_preserves_row_count(sample_transactions_df):
    processed = preprocess(sample_transactions_df)

    assert len(processed) == len(sample_transactions_df)


def test_preprocess_does_not_mutate_input(sample_transactions_df):
    original_columns = list(sample_transactions_df.columns)

    preprocess(sample_transactions_df)

    assert list(sample_transactions_df.columns) == original_columns
