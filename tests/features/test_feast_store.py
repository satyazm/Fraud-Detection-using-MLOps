"""Tests for FeastFeatureStore — needs a real Feast repo schema + Redis."""

from __future__ import annotations

import uuid

import pandas as pd
import pytest

from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feature_store import FeatureStoreError
from fraud_detection.features.registry import FEATURE_REGISTRY, feature_names

from .conftest import requires_redis

pytestmark = requires_redis


def _sample_feature_values() -> dict[str, float | int]:
    return {d.name: (1 if d.dtype == "int64" else 1.5) for d in FEATURE_REGISTRY}


@pytest.fixture
def store(applied_feast_repo) -> FeastFeatureStore:
    return FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
    )


def test_write_online_then_read_online_round_trips(store):
    entity_id = f"test-{uuid.uuid4().hex[:12]}"
    features = _sample_feature_values()

    store.write_online(entity_id, features)
    result = store.read_online(entity_id)

    for name in feature_names():
        assert result[name] == pytest.approx(features[name])


def test_read_online_raises_for_unknown_entity(store):
    with pytest.raises(FeatureStoreError):
        store.read_online(f"does-not-exist-{uuid.uuid4().hex[:12]}")


def test_write_offline_batch_then_read_offline_batch_round_trips(tmp_path):
    store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=tmp_path / "offline.parquet",
    )
    df = pd.DataFrame(
        {
            "transaction_id": ["a"],
            "event_timestamp": [pd.Timestamp.now(tz="UTC")],
            **{name: [value] for name, value in _sample_feature_values().items()},
        }
    )

    store.write_offline_batch(df)
    result = store.read_offline_batch()

    assert len(result) == 1
    assert set(feature_names()).issubset(result.columns)


def test_read_offline_batch_raises_when_nothing_written(tmp_path):
    store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=tmp_path / "missing.parquet",
    )

    with pytest.raises(FeatureStoreError):
        store.read_offline_batch()
