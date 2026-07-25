"""Tests for loading processed splits into X/y."""

import pandas as pd
import pytest

from fraud_detection.models.dataset import load_dataset
from fraud_detection.models.exceptions import ModelTrainingError


def test_load_dataset_separates_features_and_label(processed_dir):
    dataset = load_dataset(processed_dir)

    assert "isFraud" not in dataset.feature_names
    assert "isFraud" not in dataset.x_train.columns
    assert len(dataset.x_train) == len(dataset.y_train)
    assert len(dataset.x_validation) == len(dataset.y_validation)
    assert len(dataset.x_test) == len(dataset.y_test)


def test_load_dataset_features_are_all_numeric(processed_dir):
    dataset = load_dataset(processed_dir)

    assert all(pd.api.types.is_numeric_dtype(dtype) for dtype in dataset.x_train.dtypes)


def test_load_dataset_raises_when_split_missing(tmp_path):
    with pytest.raises(ModelTrainingError):
        load_dataset(tmp_path)
