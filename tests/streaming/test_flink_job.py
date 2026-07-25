"""Integration test for the PyFlink streaming job: Kafka -> features -> Feast/Redis.

Skipped automatically unless Kafka, Redis, a JVM, and the Flink Kafka
connector JAR are all available — run `docker compose up kafka redis`,
set JAVA_HOME, and `make flink-jar` to exercise this locally (see
README). Deliberately just one end-to-end test: each PyFlink job here
launches its own embedded mini-cluster (real JVM startup overhead), so
this stays a single, comprehensive check rather than many small ones.
"""

from __future__ import annotations

import shutil
import socket
import uuid

import pytest

from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.features.entity_key import compute_entity_id
from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH, apply_feast_definitions
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feature_pipeline import FeaturePipeline
from fraud_detection.features.registry import feature_names
from fraud_detection.streaming.flink_job import DEFAULT_KAFKA_CONNECTOR_JAR, run_flink_worker
from fraud_detection.streaming.producer import produce_transactions


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=1.0):
            return True
    except OSError:
        return False


def _redis_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 6379), timeout=1.0):
            return True
    except OSError:
        return False


def _java_available() -> bool:
    return shutil.which("java") is not None


pytestmark = pytest.mark.skipif(
    not (
        _kafka_reachable()
        and _redis_reachable()
        and _java_available()
        and DEFAULT_KAFKA_CONNECTOR_JAR.exists()
    ),
    reason=(
        "Needs Kafka + Redis + a JVM + the Flink Kafka connector JAR "
        "(`docker compose up kafka redis`, JAVA_HOME set, `make flink-jar`)"
    ),
)


def test_flink_worker_pushes_features_matching_transform_one(tmp_path, sample_transactions_df):
    apply_feast_definitions(DEFAULT_FEAST_REPO_PATH)

    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    topic = f"test-flink-{uuid.uuid4().hex[:8]}"
    group_id = f"test-flink-group-{uuid.uuid4().hex[:8]}"

    sent = produce_transactions(
        raw_path=csv_path,
        topic=topic,
        bootstrap_servers="localhost:9092",
        rate_per_second=0,
        limit=None,
    )
    assert sent == len(sample_transactions_df)

    run_flink_worker(
        topic=topic,
        bootstrap_servers="localhost:9092",
        group_id=group_id,
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
        kafka_connector_jar=DEFAULT_KAFKA_CONNECTOR_JAR,
        bounded=True,
    )

    store = FeastFeatureStore(
        repo_path=DEFAULT_FEAST_REPO_PATH,
        offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
    )

    row = sample_transactions_df.iloc[0].drop(labels=["isFraud"]).to_dict()
    transaction = transaction_from_dict(row)
    entity_id = compute_entity_id(transaction)
    expected = FeaturePipeline().transform_one(transaction)

    actual = store.read_online(entity_id)
    for name in feature_names():
        assert actual[name] == expected[name]
