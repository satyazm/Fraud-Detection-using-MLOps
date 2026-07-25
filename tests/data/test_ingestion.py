"""Tests for PaySim CSV ingestion and schema validation."""

import pytest

from fraud_detection.data.exceptions import DataIngestionError
from fraud_detection.data.ingestion import load_paysim_csv


def test_load_paysim_csv_raises_for_missing_file(tmp_path):
    with pytest.raises(DataIngestionError):
        load_paysim_csv(tmp_path / "does-not-exist.csv")


def test_load_paysim_csv_raises_for_missing_columns(tmp_path, sample_transactions_df):
    incomplete = sample_transactions_df.drop(columns=["amount"])
    csv_path = tmp_path / "incomplete.csv"
    incomplete.to_csv(csv_path, index=False)

    with pytest.raises(DataIngestionError):
        load_paysim_csv(csv_path)


def test_load_paysim_csv_raises_for_unexpected_transaction_type(tmp_path, sample_transactions_df):
    bad = sample_transactions_df.copy()
    bad.loc[0, "type"] = "NOT_A_TYPE"
    csv_path = tmp_path / "bad_type.csv"
    bad.to_csv(csv_path, index=False)

    with pytest.raises(DataIngestionError):
        load_paysim_csv(csv_path)


def test_load_paysim_csv_raises_for_empty_file(tmp_path):
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("")

    with pytest.raises(DataIngestionError):
        load_paysim_csv(csv_path)


def test_load_paysim_csv_succeeds_for_valid_file(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "valid.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    df = load_paysim_csv(csv_path)

    assert len(df) == len(sample_transactions_df)
    assert set(df.columns) == set(sample_transactions_df.columns)
