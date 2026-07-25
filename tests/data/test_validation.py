"""Tests for data quality checks."""

import pandas as pd
import pytest

from fraud_detection.data.exceptions import DataValidationError
from fraud_detection.data.validation import assert_trainable, run_data_quality_checks


def test_run_data_quality_checks_reports_fraud_percentage(sample_transactions_df):
    report = run_data_quality_checks(sample_transactions_df)

    assert report.row_count == len(sample_transactions_df)
    assert report.fraud_count == 15
    assert report.fraud_percentage == pytest.approx(25.0)


def test_run_data_quality_checks_detects_missing_values(sample_transactions_df):
    with_missing = sample_transactions_df.copy()
    with_missing.loc[0, "amount"] = None

    report = run_data_quality_checks(with_missing)

    assert report.missing_values.get("amount") == 1


def test_run_data_quality_checks_detects_duplicates(sample_transactions_df):
    with_dupes = pd.concat(
        [sample_transactions_df, sample_transactions_df.iloc[[0]]], ignore_index=True
    )

    report = run_data_quality_checks(with_dupes)

    assert report.duplicate_row_count >= 1


def test_run_data_quality_checks_reports_transaction_type_counts(sample_transactions_df):
    report = run_data_quality_checks(sample_transactions_df)

    assert set(report.transaction_type_counts) == {
        "PAYMENT",
        "TRANSFER",
        "CASH_OUT",
        "CASH_IN",
        "DEBIT",
    }
    assert sum(report.transaction_type_counts.values()) == len(sample_transactions_df)


def test_assert_trainable_raises_when_no_fraud(sample_transactions_df):
    no_fraud = sample_transactions_df.copy()
    no_fraud["isFraud"] = 0

    report = run_data_quality_checks(no_fraud)

    with pytest.raises(DataValidationError):
        assert_trainable(report)


def test_assert_trainable_passes_for_valid_report(sample_transactions_df):
    report = run_data_quality_checks(sample_transactions_df)

    assert_trainable(report)  # should not raise
