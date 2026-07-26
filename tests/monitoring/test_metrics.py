"""Unit tests for monitoring.metrics — no real infra needed."""

from __future__ import annotations

from fraud_detection.monitoring.metrics import (
    MODEL_FRAUD_PROBABILITY,
    MODEL_PREDICTIONS_TOTAL,
    PREDICTION_ERRORS_TOTAL,
    PREDICTION_LATENCY_SECONDS,
    PREDICTION_REQUESTS_TOTAL,
    redis_reachable,
)


def test_redis_reachable_true_for_a_real_listening_socket():
    import socket

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", 0))
    server.listen(1)
    _host, port = server.getsockname()

    try:
        assert redis_reachable("localhost", port, timeout=0.5) is True
    finally:
        server.close()


def test_redis_reachable_false_for_a_closed_port():
    import socket

    # Bind to an ephemeral port, then close it immediately — nothing is
    # listening there anymore, a more reliable "definitely closed" port
    # than guessing a fixed unused number.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", 0))
    _host, port = server.getsockname()
    server.close()

    assert redis_reachable("localhost", port, timeout=0.5) is False


def test_prediction_requests_total_increments_per_outcome():
    # prometheus_client's Counter has no public "current value" getter;
    # `._value.get()` is the commonly used (if private) way to read it
    # back synchronously for a delta assertion, without going through
    # the full text-exposition format.
    before = PREDICTION_REQUESTS_TOTAL.labels(outcome="success")._value.get()

    PREDICTION_REQUESTS_TOTAL.labels(outcome="success").inc()

    after = PREDICTION_REQUESTS_TOTAL.labels(outcome="success")._value.get()
    assert after == before + 1


def test_prediction_errors_total_increments_per_reason():
    before = PREDICTION_ERRORS_TOTAL.labels(reason="invalid_request")._value.get()

    PREDICTION_ERRORS_TOTAL.labels(reason="invalid_request").inc()

    after = PREDICTION_ERRORS_TOTAL.labels(reason="invalid_request")._value.get()
    assert after == before + 1


def test_model_predictions_total_increments_per_class():
    before = MODEL_PREDICTIONS_TOTAL.labels(prediction="1")._value.get()

    MODEL_PREDICTIONS_TOTAL.labels(prediction="1").inc()

    after = MODEL_PREDICTIONS_TOTAL.labels(prediction="1")._value.get()
    assert after == before + 1


def test_latency_and_probability_histograms_accept_observations():
    # Histograms don't expose a simple "last value" — just confirm
    # observing doesn't raise and the sample count goes up.
    before = PREDICTION_LATENCY_SECONDS._sum.get()
    PREDICTION_LATENCY_SECONDS.observe(0.042)
    assert PREDICTION_LATENCY_SECONDS._sum.get() == before + 0.042

    before_p = MODEL_FRAUD_PROBABILITY._sum.get()
    MODEL_FRAUD_PROBABILITY.observe(0.5)
    assert MODEL_FRAUD_PROBABILITY._sum.get() == before_p + 0.5
