"""Tests for the CLI: placeholder subcommands and the data/model/streaming pipeline."""

import shutil
import socket
import uuid

import pytest

from fraud_detection.cli import _cmd_api, build_parser, main
from fraud_detection.streaming.flink_job import DEFAULT_KAFKA_CONNECTOR_JAR


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


requires_kafka = pytest.mark.skipif(
    not _kafka_reachable(),
    reason="Kafka not reachable at localhost:9092 (run `docker compose up kafka`)",
)

requires_redis = pytest.mark.skipif(
    not _redis_reachable(),
    reason="Redis not reachable at localhost:6379 (run `docker compose up redis`)",
)

requires_flink_stack = pytest.mark.skipif(
    not (
        _kafka_reachable()
        and _redis_reachable()
        and shutil.which("java") is not None
        and DEFAULT_KAFKA_CONNECTOR_JAR.exists()
    ),
    reason=(
        "Needs Kafka + Redis + a JVM + the Flink Kafka connector JAR "
        "(`docker compose up kafka redis`, JAVA_HOME set, `make flink-jar`)"
    ),
)


def test_missing_command_is_a_usage_error():
    with pytest.raises(SystemExit):
        main([])


def test_ingest_command_succeeds_for_valid_csv(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    assert main(["ingest", "--raw-path", str(csv_path)]) == 0


def test_ingest_command_fails_for_missing_csv(tmp_path):
    assert main(["ingest", "--raw-path", str(tmp_path / "missing.csv")]) == 1


def test_validate_command_writes_report_and_images(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    report_path = tmp_path / "data_report.md"
    images_dir = tmp_path / "images"

    exit_code = main(
        [
            "validate",
            "--raw-path",
            str(csv_path),
            "--report-path",
            str(report_path),
            "--images-dir",
            str(images_dir),
        ]
    )

    assert exit_code == 0
    assert report_path.exists()
    assert "Fraud transactions" in report_path.read_text()
    assert (images_dir / "fraud_distribution.png").exists()
    assert (images_dir / "transaction_type_distribution.png").exists()
    assert (images_dir / "amount_histogram.png").exists()
    assert (images_dir / "correlation_heatmap.png").exists()


def test_preprocess_command_writes_parquet_splits(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    output_dir = tmp_path / "processed"

    exit_code = main(
        [
            "preprocess",
            "--raw-path",
            str(csv_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "train.parquet").exists()
    assert (output_dir / "validation.parquet").exists()
    assert (output_dir / "test.parquet").exists()


def test_preprocess_command_fails_when_no_fraud_present(tmp_path, sample_transactions_df):
    no_fraud = sample_transactions_df.copy()
    no_fraud["isFraud"] = 0
    csv_path = tmp_path / "no_fraud.csv"
    no_fraud.to_csv(csv_path, index=False)

    exit_code = main(["preprocess", "--raw-path", str(csv_path), "--output-dir", str(tmp_path)])

    assert exit_code == 1


def test_train_then_evaluate_end_to_end(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    processed_dir = tmp_path / "processed"
    tracking_uri = f"file:{tmp_path / 'mlruns'}"
    report_path = tmp_path / "model_report.md"
    images_dir = tmp_path / "images"

    preprocess_exit = main(
        ["preprocess", "--raw-path", str(csv_path), "--output-dir", str(processed_dir)]
    )
    assert preprocess_exit == 0

    train_exit = main(
        [
            "train",
            "--processed-dir",
            str(processed_dir),
            "--tracking-uri",
            tracking_uri,
            "--experiment-name",
            "cli-test",
            "--registry-name",
            "cli-test-model",
            "--report-path",
            str(report_path),
            "--images-dir",
            str(images_dir),
        ]
    )
    assert train_exit == 0
    assert report_path.exists()
    assert "Best model" in report_path.read_text()
    assert (images_dir / "model_comparison.png").exists()

    evaluate_exit = main(
        [
            "evaluate",
            "--processed-dir",
            str(processed_dir),
            "--tracking-uri",
            tracking_uri,
            "--registry-name",
            "cli-test-model",
        ]
    )
    assert evaluate_exit == 0


def test_evaluate_command_fails_when_nothing_registered(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    processed_dir = tmp_path / "processed"
    tracking_uri = f"file:{tmp_path / 'mlruns'}"

    main(["preprocess", "--raw-path", str(csv_path), "--output-dir", str(processed_dir)])

    exit_code = main(
        [
            "evaluate",
            "--processed-dir",
            str(processed_dir),
            "--tracking-uri",
            tracking_uri,
            "--registry-name",
            "no-such-model",
        ]
    )

    assert exit_code == 1


@requires_kafka
def test_producer_then_consumer_commands_end_to_end(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    topic = f"cli-test-transactions-{uuid.uuid4().hex[:8]}"

    producer_exit = main(
        [
            "producer",
            "--raw-path",
            str(csv_path),
            "--topic",
            topic,
            "--rate",
            "0",
            "--limit",
            "10",
        ]
    )
    assert producer_exit == 0

    consumer_exit = main(
        [
            "consumer",
            "--topic",
            topic,
            "--group-id",
            f"cli-test-group-{uuid.uuid4().hex[:8]}",
            "--max-messages",
            "10",
        ]
    )
    assert consumer_exit == 0


@requires_redis
def test_feast_apply_command():
    from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH

    assert main(["feast-apply", "--repo-path", str(DEFAULT_FEAST_REPO_PATH)]) == 0


@requires_redis
def test_materialize_command_builds_offline_source_and_registers(sample_transactions_df, tmp_path):
    from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
    from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH

    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    exit_code = main(
        [
            "materialize",
            "--raw-path",
            str(csv_path),
            "--repo-path",
            str(DEFAULT_FEAST_REPO_PATH),
            "--sample-size",
            str(len(sample_transactions_df)),
        ]
    )

    assert exit_code == 0
    assert DEFAULT_OFFLINE_SOURCE_PATH.exists()


@requires_flink_stack
def test_flink_worker_command_end_to_end(sample_transactions_df, tmp_path):
    from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH, apply_feast_definitions

    apply_feast_definitions(DEFAULT_FEAST_REPO_PATH)

    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    topic = f"cli-test-flink-{uuid.uuid4().hex[:8]}"

    producer_exit = main(
        [
            "producer",
            "--raw-path",
            str(csv_path),
            "--topic",
            topic,
            "--rate",
            "0",
            "--limit",
            "10",
        ]
    )
    assert producer_exit == 0

    flink_exit = main(
        [
            "flink-worker",
            "--topic",
            topic,
            "--group-id",
            f"cli-test-flink-group-{uuid.uuid4().hex[:8]}",
            "--repo-path",
            str(DEFAULT_FEAST_REPO_PATH),
            "--bounded",
        ]
    )
    assert flink_exit == 0


def test_api_command_parses_arguments():
    # Not run via main(): _cmd_api calls uvicorn.run(), which blocks
    # serving forever — the CLI/HTTP wiring is covered live by
    # tests/api/test_routers.py and test_integration.py instead. This
    # only checks argparse wires the flags to the right defaults/handler.
    parser = build_parser()

    args = parser.parse_args(["api", "--host", "127.0.0.1", "--port", "9001"])

    assert args.host == "127.0.0.1"
    assert args.port == 9001
    assert args.func is _cmd_api


def test_api_command_default_host_and_port():
    parser = build_parser()

    args = parser.parse_args(["api"])

    assert args.host == "0.0.0.0"
    assert args.port == 8000


def test_ready_command_returns_nonzero_when_unreachable():
    exit_code = main(["ready", "--host", "127.0.0.1", "--port", "1", "--timeout", "1"])

    assert exit_code == 1


def test_drift_report_command_fails_cleanly_when_nothing_logged(tmp_path):
    exit_code = main(
        [
            "drift-report",
            "--log-path",
            str(tmp_path / "no-such-log.jsonl"),
            "--output-path",
            str(tmp_path / "report.html"),
        ]
    )

    assert exit_code == 1
    assert not (tmp_path / "report.html").exists()


def test_drift_report_command_end_to_end(tmp_path, sample_transactions_df):
    from fraud_detection.domain.schemas import transaction_from_dict
    from fraud_detection.monitoring.prediction_log import append_prediction

    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    log_path = tmp_path / "prediction_log.jsonl"
    for _, row in sample_transactions_df.head(10).iterrows():
        transaction = transaction_from_dict(row.to_dict())
        append_prediction(transaction, 0, 0.01, "1", log_path=log_path)

    output_path = tmp_path / "report.html"
    exit_code = main(
        [
            "drift-report",
            "--raw-path",
            str(csv_path),
            "--reference-sample-size",
            "50",
            "--log-path",
            str(log_path),
            "--output-path",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
