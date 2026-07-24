"""Example tests verifying the base + environment-overlay config loader."""

import pytest

from fraud_detection.common.config import ConfigError, load_config


def test_load_config_dev_merges_base_and_overlay():
    config = load_config("dev")

    assert config["project"]["name"] == "fraud-detection-platform"
    assert config["environment"] == "development"
    assert config["logging"]["level"] == "DEBUG"
    assert config["debug"] is True


def test_load_config_prod_overrides_logging_level():
    config = load_config("prod")

    assert config["environment"] == "production"
    assert config["logging"]["level"] == "INFO"
    assert config["debug"] is False


def test_load_config_unknown_env_falls_back_to_base_only():
    config = load_config("does-not-exist")

    assert config["project"]["name"] == "fraud-detection-platform"
    assert config["environment"] == "does-not-exist"


def test_load_config_raises_for_missing_base_file(tmp_path):
    with pytest.raises(ConfigError):
        load_config("dev", config_dir=tmp_path)
