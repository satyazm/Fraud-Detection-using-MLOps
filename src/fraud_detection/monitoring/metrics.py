"""Prometheus metric definitions for the FastAPI inference service.

Single source of truth for every custom metric the API exposes at
`/metrics` (`api/routers.py`) — nothing else needs to know
`prometheus_client`'s API, the same way `features/registry.py` is the
one place feature *definitions* live. Metric objects are module-level
singletons (created once at import time): `prometheus_client`'s default
registry raises on duplicate registration, so these must never be
constructed inside a function that can run more than once per process
(e.g. `api.app.create_app`, which tests call repeatedly).
"""

from __future__ import annotations

import socket

from prometheus_client import Counter, Gauge, Histogram

PREDICTION_REQUESTS_TOTAL = Counter(
    "prediction_requests_total",
    "Total /predict requests received, by outcome",
    ["outcome"],  # success | invalid_request | features_unavailable
)
PREDICTION_LATENCY_SECONDS = Histogram(
    "prediction_latency_seconds", "End-to-end /predict request latency, in seconds"
)
PREDICTION_ERRORS_TOTAL = Counter(
    "prediction_errors_total",
    "Total /predict requests that resulted in an error, by reason",
    ["reason"],  # invalid_request | features_unavailable
)
MODEL_PREDICTIONS_TOTAL = Counter(
    "model_predictions_total", "Total model predictions, by predicted class", ["prediction"]
)
MODEL_FRAUD_PROBABILITY = Histogram(
    "model_fraud_probability",
    "Predicted fraud probability distribution",
    buckets=(0.0, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0),
)
REDIS_CONNECTION_STATUS = Gauge(
    "redis_connection_status", "1 if Redis (Feast's online store) is reachable, 0 otherwise"
)


def redis_reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    """Live TCP probe — the one implementation `/ready` and `/metrics`
    (via `REDIS_CONNECTION_STATUS`) both use, instead of each keeping
    its own that can silently drift out of sync (see ADR-0007's
    `FeastFeatureStore.online_store_address` for exactly that failure
    mode happening once already)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
