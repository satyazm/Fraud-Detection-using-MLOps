"""Shared fixtures for Feast-backed feature tests."""

from __future__ import annotations

import socket

import pytest

from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH, apply_feast_definitions


def _redis_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 6379), timeout=1.0):
            return True
    except OSError:
        return False


requires_redis = pytest.mark.skipif(
    not _redis_reachable(),
    reason="Redis not reachable at localhost:6379 (run `docker compose up redis`)",
)


@pytest.fixture(scope="module")
def applied_feast_repo() -> None:
    """Register the real feast_repo's schema before online tests run.

    Idempotent (`feast apply` is safe to rerun), and uses the real repo
    rather than a synthetic one — same "shared real infra, isolate by
    unique ids" pattern tests/streaming/ already uses for Kafka.
    """
    apply_feast_definitions(DEFAULT_FEAST_REPO_PATH)
