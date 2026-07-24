"""YAML-based, environment-aware configuration loading.

Configuration lives under `configs/` as plain YAML so it can be reviewed,
diffed, and overridden per-environment without touching code:

    configs/base.yaml   shared defaults for every environment
    configs/dev.yaml    overrides applied on top of base.yaml for "dev"
    configs/prod.yaml   overrides applied on top of base.yaml for "prod"

The active environment is selected via `load_config(env=...)`, falling
back to the APP_ENV environment variable, then "dev".
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = PROJECT_ROOT / "configs"
BASE_CONFIG_NAME = "base.yaml"
DEFAULT_ENV = "dev"


class ConfigError(Exception):
    """Raised when configuration cannot be loaded or parsed."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    try:
        with path.open("r") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML config at {path}: {exc}") from exc


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `overlay` onto `base`, without mutating either."""
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(env: str | None = None, config_dir: Path | str | None = None) -> dict[str, Any]:
    """Load `base.yaml` merged with the `{env}.yaml` overlay.

    Args:
        env: Environment name matching `configs/{env}.yaml` (e.g. "dev",
            "prod"). Defaults to the APP_ENV environment variable, then
            "dev". Missing overlay files are silently skipped so `env`
            can be an ad-hoc name without a dedicated YAML file.
        config_dir: Directory containing config YAML files. Defaults to
            `configs/` at the project root.

    Returns:
        The merged configuration dict, with `environment` set to `env`
        unless the config itself already defines one.

    Raises:
        ConfigError: If `base.yaml` is missing or any present file is
            not valid YAML.
    """
    directory = Path(config_dir) if config_dir else CONFIG_DIR
    env = env or os.environ.get("APP_ENV", DEFAULT_ENV)

    config = _read_yaml(directory / BASE_CONFIG_NAME)

    env_path = directory / f"{env}.yaml"
    if env_path.exists():
        config = _deep_merge(config, _read_yaml(env_path))

    config["environment"] = config.get("environment", env)
    return config
