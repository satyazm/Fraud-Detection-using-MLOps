"""Unit tests for monitoring.drift — synthetic data, no real PaySim CSV needed.

`load_reference_sample` is tested separately against a small tmp CSV
(same "synthetic CSV, not the real 470MB PaySim file" convention
`tests/test_cli.py` already uses) — `generate_drift_report` itself
only needs two DataFrames, so it's tested directly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fraud_detection.monitoring.drift import (
    DRIFT_COLUMNS,
    generate_drift_report,
    load_reference_sample,
)


def _synthetic_frame(n: int, amount_mean: float, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "amount": rng.normal(amount_mean, 20, n),
            "type": rng.choice(["TRANSFER", "CASH_OUT", "PAYMENT"], n),
            "oldbalanceOrg": rng.normal(1000, 200, n),
            "newbalanceOrig": rng.normal(800, 200, n),
        }
    )


def test_generate_drift_report_detects_a_real_shift(tmp_path):
    reference = _synthetic_frame(300, amount_mean=100, seed=0)
    current = _synthetic_frame(300, amount_mean=100, seed=0)
    current["amount"] = current["amount"] * 5  # unambiguous shift

    summary = generate_drift_report(reference, current, output_path=tmp_path / "report.html")

    assert summary.total_columns == len(DRIFT_COLUMNS)
    assert summary.drifted_columns >= 1
    assert summary.report_path.exists()
    assert "html" in summary.report_path.read_text()[:200].lower()


def test_generate_drift_report_no_drift_for_identical_distributions(tmp_path):
    reference = _synthetic_frame(300, amount_mean=100, seed=1)
    current = _synthetic_frame(300, amount_mean=100, seed=1)  # identical

    summary = generate_drift_report(reference, current, output_path=tmp_path / "report.html")

    assert summary.drifted_columns == 0
    assert summary.drift_share == 0.0


def test_load_reference_sample_selects_drift_columns_from_csv(tmp_path, sample_transactions_df):
    csv_path = tmp_path / "sample.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    reference = load_reference_sample(raw_path=csv_path, sample_size=10)

    assert list(reference.columns) == list(DRIFT_COLUMNS)
    assert len(reference) == 10


def test_generate_drift_report_raises_for_missing_column(tmp_path):
    reference = _synthetic_frame(50, amount_mean=100, seed=2).drop(columns=["type"])
    current = _synthetic_frame(50, amount_mean=100, seed=2)

    with pytest.raises(KeyError):
        generate_drift_report(reference, current, output_path=tmp_path / "report.html")
