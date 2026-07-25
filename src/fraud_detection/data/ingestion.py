"""PaySim CSV ingestion.

Loads the raw PaySim transaction log and validates its schema before
anything downstream (validation, preprocessing, splitting) touches it.
Raw data itself is never modified — this module only reads it.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger
from fraud_detection.data.exceptions import DataIngestionError

logger = get_logger(__name__)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)

# float64 columns tolerate an int64 read (e.g. a balance column that
# happens to contain only whole numbers in a given file).
EXPECTED_DTYPES: dict[str, str] = {
    "step": "int64",
    "type": "object",
    "amount": "float64",
    "nameOrig": "object",
    "oldbalanceOrg": "float64",
    "newbalanceOrig": "float64",
    "nameDest": "object",
    "oldbalanceDest": "float64",
    "newbalanceDest": "float64",
    "isFraud": "int64",
    "isFlaggedFraud": "int64",
}

VALID_TRANSACTION_TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}

# Canonical filename of the PaySim dataset as distributed on Kaggle
# (ealaxi/paysim1). Override via the `path` argument for other files.
DEFAULT_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "PS_20174660362_1_log.csv"


def load_paysim_csv(path: Path | str | None = None) -> pd.DataFrame:
    """Load and schema-validate the raw PaySim CSV.

    Args:
        path: Path to the CSV file. Defaults to the canonical PaySim
            filename under `data/raw/`.

    Returns:
        The raw dataframe, unmodified, once it has passed schema checks.

    Raises:
        DataIngestionError: If the file is missing, empty, unparsable,
            or its schema (missing columns, wrong types, unexpected
            transaction types) doesn't match what the pipeline expects.
    """
    csv_path = Path(path) if path else DEFAULT_RAW_PATH

    if not csv_path.exists():
        raise DataIngestionError(f"PaySim CSV not found at {csv_path}")

    try:
        df = pd.read_csv(csv_path)
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
        raise DataIngestionError(f"Failed to parse CSV at {csv_path}: {exc}") from exc

    if df.empty:
        raise DataIngestionError(f"PaySim CSV at {csv_path} contains no rows")

    _validate_columns(df)
    _validate_dtypes(df)
    _validate_transaction_types(df)

    logger.info("loaded PaySim dataset", extra={"path": str(csv_path), "rows": len(df)})
    return df


def _validate_columns(df: pd.DataFrame) -> None:
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise DataIngestionError(f"Missing required columns: {sorted(missing)}")


def _validate_dtypes(df: pd.DataFrame) -> None:
    mismatched = {}
    for column, expected_dtype in EXPECTED_DTYPES.items():
        actual_dtype = str(df[column].dtype)
        if not _dtype_compatible(actual_dtype, expected_dtype):
            mismatched[column] = f"expected {expected_dtype}, got {actual_dtype}"
    if mismatched:
        raise DataIngestionError(f"Column dtype mismatches: {mismatched}")


def _dtype_compatible(actual: str, expected: str) -> bool:
    if expected == "float64":
        return actual in {"float64", "int64"}
    return actual == expected


def _validate_transaction_types(df: pd.DataFrame) -> None:
    unexpected = set(df["type"].unique()) - VALID_TRANSACTION_TYPES
    if unexpected:
        raise DataIngestionError(f"Unexpected transaction types: {sorted(unexpected)}")
