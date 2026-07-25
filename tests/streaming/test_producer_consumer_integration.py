"""Integration tests against a real Kafka broker.

Skipped automatically if Kafka isn't reachable at localhost:9092 — run
`docker compose up kafka` first (see README) to exercise these
locally. Each test uses a fresh, uuid-suffixed topic and consumer
group so runs never collide with each other or with manual testing.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from fraud_detection.streaming.consumer import consume_transactions
from fraud_detection.streaming.producer import produce_transactions

BOOTSTRAP_SERVERS = "localhost:9092"


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=1.0):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _kafka_reachable(),
    reason="Kafka not reachable at localhost:9092 (run `docker compose up kafka`)",
)


def test_produce_then_consume_round_trip(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    topic = f"test-transactions-{uuid.uuid4().hex[:8]}"

    sent = produce_transactions(
        raw_path=csv_path,
        topic=topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        rate_per_second=0,
        limit=None,
    )
    assert sent == len(sample_transactions_df)

    processed = consume_transactions(
        topic=topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"test-group-{uuid.uuid4().hex[:8]}",
        max_messages=sent,
        poll_timeout_seconds=1.0,
    )
    assert processed == sent


def test_produce_respects_limit(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    topic = f"test-transactions-{uuid.uuid4().hex[:8]}"

    sent = produce_transactions(
        raw_path=csv_path,
        topic=topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        rate_per_second=0,
        limit=10,
    )

    assert sent == 10
