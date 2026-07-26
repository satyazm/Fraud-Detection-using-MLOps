"""Append-only log of real `/predict` requests and their results.

This is the "live production data" `monitoring.drift`'s Evidently
report compares against the training reference — not a synthetic
stand-in. Deliberately a plain JSONL file (append-friendly, no schema
migration story, human-readable), not a database: this project already
uses a file for Feast's offline store (`feast_prep.py`) for the same
"good enough for local/dev scale, one call to change later" reasons.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.domain.entities import Transaction
from fraud_detection.domain.schemas import transaction_to_dict

DEFAULT_LOG_PATH = PROJECT_ROOT / "data" / "monitoring" / "prediction_log.jsonl"


class PredictionLogError(Exception):
    """Raised when the prediction log can't be read (e.g. nothing logged yet)."""


def append_prediction(
    transaction: Transaction,
    prediction: int,
    fraud_probability: float,
    model_version: str,
    log_path: Path | str = DEFAULT_LOG_PATH,
) -> None:
    """Append one served prediction. Never raises on a write failure at
    the caller — `api.routers.predict` treats this as best-effort
    telemetry, not something that should fail a request that already
    succeeded."""
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        **transaction_to_dict(transaction),
        "prediction": prediction,
        "fraud_probability": fraud_probability,
        "model_version": model_version,
        "logged_at": datetime.now(UTC).isoformat(),
    }
    with path.open("a") as f:
        f.write(json.dumps(row) + "\n")


def load_predictions(log_path: Path | str = DEFAULT_LOG_PATH) -> pd.DataFrame:
    """Raises:
    PredictionLogError: If nothing has been logged yet — a drift report
        against zero rows of "live" data isn't a real drift report.
    """
    path = Path(log_path)
    if not path.exists():
        raise PredictionLogError(
            f"No predictions logged yet at {path}; send some /predict requests first"
        )

    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        raise PredictionLogError(f"Prediction log at {path} is empty")

    return pd.DataFrame(rows)
