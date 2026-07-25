"""Tests for the CLI: placeholder subcommands and the Phase 2 data pipeline."""

import pytest

from fraud_detection.cli import main


@pytest.mark.parametrize("command", ["train", "producer", "consumer", "api"])
def test_placeholder_commands_exit_cleanly(command):
    assert main([command]) == 0


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
