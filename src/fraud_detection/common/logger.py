"""Structured (JSON) logging setup.

Wraps the standard library's `logging.config.dictConfig`, driven by
`configs/logging.yaml`, so every component in the platform emits
consistently formatted, machine-parseable log records.
"""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOGGING_CONFIG_PATH = PROJECT_ROOT / "configs" / "logging.yaml"

_configured = False


def setup_logging(config_path: Path | str | None = None) -> None:
    """Configure logging for the process from a YAML dictConfig file.

    Falls back to `logging.basicConfig` if the config file is missing,
    so the platform never fails to start solely because logging config
    is absent.
    """
    global _configured

    path = Path(config_path) if config_path else DEFAULT_LOGGING_CONFIG_PATH

    if not path.exists():
        logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__).warning(
            "Logging config not found at %s; falling back to basicConfig", path
        )
        _configured = True
        return

    with path.open("r") as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger, initializing logging on first use."""
    if not _configured:
        setup_logging()
    return logging.getLogger(name)
