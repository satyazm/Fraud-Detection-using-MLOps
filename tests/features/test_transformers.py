"""Tests for individual feature transformers."""

from fraud_detection.features.transformers import (
    DEFAULT_TRANSFORMERS,
    add_amount_ratio_feature,
    add_balance_delta_features,
    add_balance_error_features,
    add_balance_flag_features,
    add_merchant_flag_feature,
    add_temporal_features,
)


def test_add_balance_error_features_computes_expected_values(sample_transactions_df):
    result = add_balance_error_features(sample_transactions_df)

    row = result.iloc[0]
    expected_orig = row["oldbalanceOrg"] - row["amount"] - row["newbalanceOrig"]
    expected_dest = row["oldbalanceDest"] + row["amount"] - row["newbalanceDest"]
    assert row["orig_balance_error"] == expected_orig
    assert row["dest_balance_error"] == expected_dest


def test_add_balance_delta_features_computes_expected_values(sample_transactions_df):
    result = add_balance_delta_features(sample_transactions_df)

    row = result.iloc[0]
    assert row["orig_balance_delta"] == row["newbalanceOrig"] - row["oldbalanceOrg"]
    assert row["dest_balance_delta"] == row["newbalanceDest"] - row["oldbalanceDest"]


def test_add_balance_flag_features_flags_depleted_origin(sample_transactions_df):
    df = sample_transactions_df.copy()
    df.loc[0, "oldbalanceOrg"] = 500.0
    df.loc[0, "newbalanceOrig"] = 0.0

    result = add_balance_flag_features(df)

    assert result.loc[0, "is_orig_balance_depleted"] == 1


def test_add_balance_flag_features_flags_untouched_destination(sample_transactions_df):
    df = sample_transactions_df.copy()
    df.loc[0, "oldbalanceDest"] = 0.0
    df.loc[0, "newbalanceDest"] = 0.0
    df.loc[0, "amount"] = 100.0

    result = add_balance_flag_features(df)

    assert result.loc[0, "is_dest_balance_untouched"] == 1


def test_add_amount_ratio_feature_never_divides_by_zero(sample_transactions_df):
    df = sample_transactions_df.copy()
    df.loc[0, "oldbalanceOrg"] = 0.0

    result = add_amount_ratio_feature(df)

    assert result.loc[0, "amount_to_orig_balance_ratio"] == df.loc[0, "amount"] / 1.0


def test_add_temporal_features_wraps_step_into_hour_of_day(sample_transactions_df):
    df = sample_transactions_df.copy()
    df.loc[0, "step"] = 25

    result = add_temporal_features(df)

    assert result.loc[0, "hour_of_day"] == 1


def test_add_merchant_flag_feature_detects_m_prefix(sample_transactions_df):
    result = add_merchant_flag_feature(sample_transactions_df)

    # the fixture uses "M{i}" for every nameDest value
    assert result["is_dest_merchant"].eq(1).all()


def test_transformers_never_mutate_input(sample_transactions_df):
    original_columns = list(sample_transactions_df.columns)

    for transformer in DEFAULT_TRANSFORMERS:
        transformer(sample_transactions_df)

    assert list(sample_transactions_df.columns) == original_columns
