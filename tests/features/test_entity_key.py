"""Tests for deterministic entity key derivation."""

from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.entity_key import compute_entity_id, compute_entity_ids


def _row_dict(df, index: int) -> dict:
    return df.iloc[index].drop(labels=["isFraud"]).to_dict()


def test_compute_entity_id_is_deterministic(sample_transactions_df):
    transaction = transaction_from_dict(_row_dict(sample_transactions_df, 0))

    first = compute_entity_id(transaction)
    second = compute_entity_id(transaction)

    assert first == second
    assert len(first) == 16


def test_compute_entity_id_differs_for_different_transactions(sample_transactions_df):
    id_a = compute_entity_id(transaction_from_dict(_row_dict(sample_transactions_df, 0)))
    id_b = compute_entity_id(transaction_from_dict(_row_dict(sample_transactions_df, 1)))

    assert id_a != id_b


def test_compute_entity_ids_matches_compute_entity_id_per_row(sample_transactions_df):
    """The vectorized (DataFrame) path must never disagree with the
    single-transaction path — that's the whole correctness guarantee
    (see entity_key.py's module docstring)."""
    ids = compute_entity_ids(sample_transactions_df)

    for i in range(len(sample_transactions_df)):
        expected = compute_entity_id(transaction_from_dict(_row_dict(sample_transactions_df, i)))
        assert ids.iloc[i] == expected
