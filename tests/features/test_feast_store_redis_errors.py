"""Regression tests for `FeastFeatureStore` behavior that doesn't need a
live Redis — found via manual end-to-end verification of the Milestone 6
API against a real model and a real Docker container.

Deliberately not gated by `tests.features.conftest.requires_redis`
(unlike the rest of `test_feast_store.py`): both tests here either need
Redis to be *unreachable*, or only need Feast's already-loaded config
(no connection at all), so they must run in exactly the CI environment
(no Redis) that skips the rest of that file.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH, apply_feast_definitions
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feature_store import FeatureStoreError


def test_read_online_wraps_redis_connection_error():
    """`/predict` returned a bare 500 (not the 503 `api.routers.predict`
    maps `FeatureStoreError` to) the first time this was tried against
    a real model with Redis intentionally down."""
    apply_feast_definitions(DEFAULT_FEAST_REPO_PATH)
    store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
    )
    store._client = MagicMock()
    store._client.get_online_features.side_effect = RedisConnectionError("Connection refused.")

    with pytest.raises(FeatureStoreError, match="Could not reach the online store"):
        store.read_online("some-entity-id")


def test_online_store_address_matches_configured_connection_string():
    """`/ready` reported `redis_reachable: false` inside the real Docker
    container even though Feast could reach Redis fine — the API had a
    second, hardcoded "localhost:6379" for its own live-reachability
    probe that didn't match Feast's actual configured "redis:6379"
    inside the container. `online_store_address()` exists so there's
    exactly one place that knows where Redis is."""
    apply_feast_definitions(DEFAULT_FEAST_REPO_PATH)
    store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
    )

    host, port = store.online_store_address()

    assert (host, port) == ("localhost", 6379)
