"""Command-line entry point for the fraud detection platform.

`ingest`/`validate`/`preprocess` run the real Phase 2 data pipeline.
`train`/`producer`/`consumer`/`api` remain placeholders — each logs
which later phase implements it — so the full operational surface is
visible early.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger
from fraud_detection.data.exceptions import DataError
from fraud_detection.data.ingestion import DEFAULT_RAW_PATH, load_paysim_csv
from fraud_detection.data.preprocessing import preprocess
from fraud_detection.data.reporting import (
    DEFAULT_IMAGES_DIR,
    DEFAULT_REPORT_PATH,
    generate_plots,
    render_markdown_report,
)
from fraud_detection.data.split import stratified_split
from fraud_detection.data.validation import assert_trainable, run_data_quality_checks

logger = get_logger("fraud_detection.cli")

DEFAULT_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

_PLACEHOLDER_PLANNED_IN = {
    "train": "Phase 3 (Feature Engineering & Baseline Model)",
    "producer": "Phase 4 (Kafka Streaming)",
    "consumer": "Phase 4 (Kafka Streaming)",
    "api": "Phase 6 (FastAPI Serving)",
}


def _cmd_ingest(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    logger.info("ingestion succeeded", extra={"rows": len(df), "columns": len(df.columns)})
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    report = run_data_quality_checks(df)
    image_paths = generate_plots(df, output_dir=args.images_dir)
    report_path = render_markdown_report(report, output_path=args.report_path)

    logger.info(
        "validation report generated",
        extra={
            "report_path": str(report_path),
            "image_count": len(image_paths),
            "fraud_percentage": report.fraud_percentage,
        },
    )
    return 0


def _cmd_preprocess(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    report = run_data_quality_checks(df)
    try:
        assert_trainable(report)
    except DataError as exc:
        logger.error("dataset failed trainability check", extra={"error": str(exc)})
        return 1

    processed_df = preprocess(df)
    split = stratified_split(processed_df)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for split_name, subset in split._asdict().items():
        split_path = output_dir / f"{split_name}.parquet"
        subset.to_parquet(split_path, index=False)
        logger.info(
            "saved processed split",
            extra={"split": split_name, "rows": len(subset), "path": str(split_path)},
        )

    return 0


def _run_placeholder(command: str) -> int:
    logger.info(
        "command not yet implemented",
        extra={"command": command, "planned_in": _PLACEHOLDER_PLANNED_IN[command]},
    )
    return 0


def _placeholder_handler(command: str) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        return _run_placeholder(command)

    return handler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fraud-detection", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser(
        "ingest", help="Load and schema-validate the raw PaySim CSV"
    )
    ingest_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    ingest_parser.set_defaults(func=_cmd_ingest)

    validate_parser = subparsers.add_parser(
        "validate", help="Generate the data quality report and diagnostic plots"
    )
    validate_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    validate_parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    validate_parser.add_argument("--images-dir", type=Path, default=DEFAULT_IMAGES_DIR)
    validate_parser.set_defaults(func=_cmd_validate)

    preprocess_parser = subparsers.add_parser(
        "preprocess", help="Preprocess, stratified-split, and save the dataset"
    )
    preprocess_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    preprocess_parser.add_argument("--output-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    preprocess_parser.set_defaults(func=_cmd_preprocess)

    for name, planned_in in _PLACEHOLDER_PLANNED_IN.items():
        placeholder_parser = subparsers.add_parser(
            name, help=f"[placeholder, arrives in {planned_in}]"
        )
        placeholder_parser.set_defaults(func=_placeholder_handler(name))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result: int = args.func(args)
    return result


if __name__ == "__main__":
    sys.exit(main())
