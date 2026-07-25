"""Feast repo administration: apply + materialize.

Shells out to the `feast` CLI — the same pattern
`fraud_detection.models.training.get_git_commit_hash` uses for `git` —
rather than reimplementing Feast's own repo-scanning/definition-
discovery or materialization logic ourselves.
"""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger

logger = get_logger(__name__)

DEFAULT_FEAST_REPO_PATH = PROJECT_ROOT / "feast_repo"


class FeastOperationError(Exception):
    """Raised when a `feast` CLI operation (apply/materialize) fails."""


def apply_feast_definitions(repo_path: Path | str = DEFAULT_FEAST_REPO_PATH) -> None:
    """Register the entities/feature views defined in `repo_path` (`feast apply`)."""
    _run_feast_command(["apply"], repo_path)


def materialize_feast_features(
    start: datetime,
    end: datetime,
    repo_path: Path | str = DEFAULT_FEAST_REPO_PATH,
) -> None:
    """Push offline features into the online store for [start, end) (`feast materialize`)."""
    _run_feast_command(["materialize", start.isoformat(), end.isoformat()], repo_path)


def _run_feast_command(args: list[str], repo_path: Path | str) -> None:
    try:
        result = subprocess.run(  # noqa: S603 — fixed argv, no user input
            ["feast", *args],
            cwd=str(repo_path),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise FeastOperationError(f"Could not run `feast {' '.join(args)}`: {exc}") from exc

    if result.returncode != 0:
        raise FeastOperationError(
            f"`feast {' '.join(args)}` failed (exit {result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}"
        )

    logger.info(
        "feast command succeeded",
        extra={"command": " ".join(args), "repo_path": str(repo_path)},
    )
