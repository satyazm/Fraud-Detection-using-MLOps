"""Tests for the CLI: placeholder subcommands and the data/model/streaming pipeline."""

import socket
import uuid

import pytest

from fraud_detection.cli import main


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=1.0):
            return True
    except OSError:
        return False


requires_kafka = pytest.mark.skipif(
    not _kafka_reachable(),
    reason="Kafka not reachable at localhost:9092 (run `docker compose up kafka`)",
)


def test_placeholder_commands_exit_cleanly():
    assert main(["api"]) == 0


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
