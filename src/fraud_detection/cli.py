"""Command-line entry point for the fraud detection platform.

Phase 1 only wires up the operational surface (train / producer /
consumer / api) as placeholders, so the expected commands are visible
early. Each is implemented for real in the phase noted below.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from fraud_detection.common.logger import get_logger

logger = get_logger("fraud_detection.cli")

_PLANNED_IN = {
    "train": "Phase 2 (Data Understanding & Baseline ML)",
    "producer": "Phase 4 (Streaming Ingestion)",
    "consumer": "Phase 4 (Streaming Ingestion)",
    "api": "Phase 5 (Model Serving)",
}


def _run_placeholder(command: str) -> int:
    logger.info(
        "command not yet implemented",
        extra={"command": command, "planned_in": _PLANNED_IN[command]},
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fraud-detection", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, planned_in in _PLANNED_IN.items():
        subparsers.add_parser(name, help=f"[placeholder, arrives in {planned_in}]")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return _run_placeholder(args.command)


if __name__ == "__main__":
    sys.exit(main())
