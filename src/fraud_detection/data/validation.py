"""Data quality checks for the raw PaySim dataset.

Computes the numbers that go into `docs/data_report.md`; rendering
that report (markdown + plots) lives in `reporting.py` so this module
stays free of matplotlib/file-I/O concerns and is easy to unit test.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fraud_detection.data.exceptions import DataValidationError

TARGET_COLUMN = "isFraud"
TRANSACTION_TYPE_COLUMN = "type"
NUMERICAL_COLUMNS: tuple[str, ...] = (
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
)


@dataclass(frozen=True)
class DataQualityReport:
    row_count: int
    column_count: int
    missing_values: dict[str, int]
    duplicate_row_count: int
    fraud_count: int
    fraud_percentage: float
    transaction_type_counts: dict[str, int]
    numerical_summary: dict[str, dict[str, float]]
    negative_amount_count: int


def run_data_quality_checks(df: pd.DataFrame) -> DataQualityReport:
    """Compute missing values, duplicates, class balance, and descriptive stats."""
    row_count = len(df)
    fraud_count = int(df[TARGET_COLUMN].sum()) if TARGET_COLUMN in df.columns else 0

    return DataQualityReport(
        row_count=row_count,
        column_count=df.shape[1],
        missing_values={str(col): int(n) for col, n in df.isna().sum().items() if n > 0},
        duplicate_row_count=int(df.duplicated().sum()),
        fraud_count=fraud_count,
        fraud_percentage=round(100 * fraud_count / row_count, 4) if row_count else 0.0,
        transaction_type_counts={
            str(k): int(v) for k, v in df[TRANSACTION_TYPE_COLUMN].value_counts().items()
        },
        numerical_summary={
            col: {str(k): float(v) for k, v in df[col].describe().items()}
            for col in NUMERICAL_COLUMNS
            if col in df.columns
        },
        negative_amount_count=int((df["amount"] < 0).sum()) if "amount" in df.columns else 0,
    )


def assert_trainable(report: DataQualityReport) -> None:
    """Raise if the dataset can't reasonably be used to train/evaluate a classifier."""
    if report.row_count == 0:
        raise DataValidationError("Dataset is empty")
    if report.fraud_count == 0:
        raise DataValidationError(
            "Dataset contains zero fraud examples; cannot train or evaluate a classifier"
        )
