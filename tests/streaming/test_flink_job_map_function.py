"""Unit tests for `_ComputeAndPushFeatures.map()`'s per-record fault isolation.

Deliberately not part of test_flink_job.py's integration test (which
needs a real JVM/Kafka/Redis) — `_ComputeAndPushFeatures` is plain
Python, so its `map()` logic is testable directly by constructing it
and calling `open()`/`map()` by hand with a fake `FeastFeatureStore`.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.entity_key import compute_entity_id
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.streaming.flink_job import _ComputeAndPushFeatures
from fraud_detection.streaming.serializer import serialize_transaction


class _FailingStore:
    """Stands in for FeastFeatureStore, simulating a write_online failure
    (e.g. a transient Redis error) the same way the real one would raise."""

    def write_online(self, entity_id: str, features: dict) -> None:
        raise ConnectionError("simulated Redis blip")


@pytest.fixture
def message_value(sample_transactions_df: pd.DataFrame) -> str:
    row = sample_transactions_df.iloc[0].drop(labels=["isFraud"]).to_dict()
    transaction = transaction_from_dict(row)
    return serialize_transaction(transaction).decode("utf-8")


def test_map_skips_and_logs_on_write_online_failure(message_value: str) -> None:
    """A real bug: this used to propagate uncaught and kill the whole
    Flink job. It must now be isolated to this one record."""
    fn = _ComputeAndPushFeatures(repo_path="unused", offline_source_path="unused")
    fn._pipeline = FeaturePipeline()
    fn._store = _FailingStore()

    result = fn.map(message_value)

    assert result.startswith("SKIPPED processing error for entity_id=")
    assert "simulated Redis blip" in result


def test_map_still_returns_ok_on_success(message_value: str, sample_transactions_df) -> None:
    written: dict = {}

    class _RecordingStore:
        def write_online(self, entity_id: str, features: dict) -> None:
            written["entity_id"] = entity_id
            written["features"] = features

    fn = _ComputeAndPushFeatures(repo_path="unused", offline_source_path="unused")
    fn._pipeline = FeaturePipeline()
    fn._store = _RecordingStore()

    row = sample_transactions_df.iloc[0].drop(labels=["isFraud"]).to_dict()
    transaction = transaction_from_dict(row)
    expected_entity_id = compute_entity_id(transaction)

    result = fn.map(message_value)

    assert result == f"OK entity_id={expected_entity_id} name_orig={transaction.name_orig}"
    assert written["entity_id"] == expected_entity_id
    assert written["features"]  # non-empty — the pipeline actually ran
