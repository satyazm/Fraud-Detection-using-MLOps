"""Data drift monitoring (Evidently AI): training reference vs. live production data.

Compares the raw PaySim feature distributions the model was trained on
against `monitoring.prediction_log`'s real record of what `/predict`
has actually been asked to score — not a synthetic proxy. Deliberately
scoped to the columns the milestone asked for (`amount`, `type`,
`oldbalanceOrg`, `newbalanceOrig`): every one is a raw column both
`data.ingestion.load_paysim_csv` and `prediction_log.append_prediction`
already produce, so there's no schema to reconcile between the two
sources.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger
from fraud_detection.data.ingestion import DEFAULT_RAW_PATH, load_paysim_csv

logger = get_logger(__name__)

DEFAULT_REPORT_PATH = PROJECT_ROOT / "docs" / "drift_report.html"
DEFAULT_REFERENCE_SAMPLE_SIZE = 5000
DRIFT_COLUMNS: tuple[str, ...] = ("amount", "type", "oldbalanceOrg", "newbalanceOrig")


class DriftReportError(Exception):
    """Raised when a drift report can't be built (e.g. Evidently's output
    didn't include the summary metric this module extracts)."""


@dataclass(frozen=True)
class DriftSummary:
    drifted_columns: int
    total_columns: int
    drift_share: float
    report_path: Path


def load_reference_sample(
    raw_path: Path | str = DEFAULT_RAW_PATH,
    sample_size: int = DEFAULT_REFERENCE_SAMPLE_SIZE,
) -> pd.DataFrame:
    """The training-time distribution: a sample of the raw PaySim CSV,
    restricted to `DRIFT_COLUMNS` — the same raw columns training data
    and live requests share.
    """
    df = load_paysim_csv(raw_path)
    return df[list(DRIFT_COLUMNS)].head(sample_size)


def generate_drift_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    output_path: Path | str = DEFAULT_REPORT_PATH,
) -> DriftSummary:
    """Build and save an Evidently data-drift HTML report over `DRIFT_COLUMNS`.

    Raises whatever `evidently`/`pandas` raise for genuinely malformed
    input (e.g. a missing column) — deliberately not caught here; the
    CLI layer decides how to present that.
    """
    definition = DataDefinition()
    reference_dataset = Dataset.from_pandas(
        reference[list(DRIFT_COLUMNS)], data_definition=definition
    )
    current_dataset = Dataset.from_pandas(current[list(DRIFT_COLUMNS)], data_definition=definition)

    report = Report(metrics=[DataDriftPreset()])
    result = report.run(reference_data=reference_dataset, current_data=current_dataset)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    result.save_html(str(path))

    summary = _extract_summary(result, path)
    logger.info(
        "drift report generated",
        extra={
            "report_path": str(path),
            "drifted_columns": summary.drifted_columns,
            "total_columns": summary.total_columns,
            "drift_share": summary.drift_share,
        },
    )
    return summary


def _extract_summary(result: Any, report_path: Path) -> DriftSummary:
    data = result.dict()
    for metric in data["metrics"]:
        if metric["metric_name"].startswith("DriftedColumnsCount"):
            value = metric["value"]
            return DriftSummary(
                drifted_columns=int(value["count"]),
                total_columns=len(DRIFT_COLUMNS),
                drift_share=float(value["share"]),
                report_path=report_path,
            )
    raise DriftReportError("Evidently report did not include a DriftedColumnsCount metric")
