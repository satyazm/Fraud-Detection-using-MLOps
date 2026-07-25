"""Feature computation pipeline — the one place feature logic lives.

`transform()` is the batch/offline entry point (training,
`fraud-detection preprocess`-style CLI commands). `transform_one()` is
the online entry point: it builds a one-row dataframe from a domain
`Transaction` and calls `transform()` internally, so there is exactly
one implementation of every feature — not two that can drift apart.
The same `transform_one()` call is what a future Kafka/Flink consumer
(Milestone 4) will use per-event, with no new feature code required.

Pipeline order in a full training run:
    ingestion.load_paysim_csv()
        -> FeaturePipeline.transform()          (this module: add engineered columns)
        -> data.preprocessing.preprocess()      (drop identifiers, encode `type`)
        -> data.split.stratified_split()
`FeaturePipeline` must run before `data.preprocessing.preprocess()`
because some features (e.g. `is_dest_merchant`) read `nameDest`, which
preprocessing drops.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from fraud_detection.common.logger import get_logger
from fraud_detection.domain.entities import Transaction
from fraud_detection.domain.schemas import transaction_to_dict
from fraud_detection.features.registry import validate_features_present
from fraud_detection.features.transformers import DEFAULT_TRANSFORMERS, Transformer

logger = get_logger(__name__)


class FeaturePipeline:
    """Applies an ordered sequence of transformers to raw transaction rows."""

    def __init__(self, transformers: Sequence[Transformer] = DEFAULT_TRANSFORMERS) -> None:
        self._transformers = tuple(transformers)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Batch/offline entry point: add engineered feature columns to `df`.

        `df` must have the raw PaySim/Transaction schema. Original
        columns are preserved; engineered columns are added.

        Raises:
            FeatureRegistryError: If the result is missing any feature
                declared in `registry.FEATURE_REGISTRY`.
        """
        result = df.copy()
        for transformer in self._transformers:
            result = transformer(result)

        validate_features_present(result)
        logger.info(
            "computed features",
            extra={"rows": len(result), "transformer_count": len(self._transformers)},
        )
        return result

    def transform_one(self, transaction: Transaction) -> dict[str, Any]:
        """Online/streaming entry point: compute features for one Transaction.

        Delegates to `transform()` via a one-row dataframe, so online
        and offline feature values can never diverge.
        """
        row = pd.DataFrame([transaction_to_dict(transaction)])
        features_df = self.transform(row)
        result: dict[str, Any] = features_df.iloc[0].to_dict()
        return result


DEFAULT_FEATURE_PIPELINE = FeaturePipeline()
