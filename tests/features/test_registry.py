"""Tests for the feature registry."""

import pandas as pd
import pytest

from fraud_detection.features.registry import (
    FEATURE_REGISTRY,
    FeatureRegistryError,
    feature_names,
    validate_features_present,
)


def test_feature_names_matches_registry_length():
    assert len(feature_names()) == len(FEATURE_REGISTRY)


def test_feature_names_are_unique():
    names = feature_names()
    assert len(names) == len(set(names))


def test_validate_features_present_passes_when_all_columns_exist():
    df = pd.DataFrame({name: [0] for name in feature_names()})

    validate_features_present(df)  # should not raise


def test_validate_features_present_raises_for_missing_column():
    df = pd.DataFrame({name: [0] for name in feature_names()[:-1]})

    with pytest.raises(FeatureRegistryError):
        validate_features_present(df)
