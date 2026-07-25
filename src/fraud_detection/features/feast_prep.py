"""Builds the offline FileSource parquet Feast's `transaction_features`
FeatureView reads from.

Takes the output of `FeaturePipeline.transform()` (still has the raw
identity columns `entity_key.compute_entity_ids` needs, plus the
engineered features) and produces exactly the columns Feast requires:
the entity join key, `event_timestamp`, and the registered features —
nothing else, and nothing recomputed. PaySim has no real timestamps,
so `event_timestamp` is synthetic, derived from `step` (simulated
hours elapsed); that's a known, documented simplification (see
docs/decisions/0006-feast-redis-flink.md), not a claim of real event
time.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger
from fraud_detection.features.entity_key import compute_entity_ids
from fraud_detection.features.registry import feature_names

logger = get_logger(__name__)

DEFAULT_OFFLINE_SOURCE_PATH = PROJECT_ROOT / "data" / "feast" / "transaction_features.parquet"
DEFAULT_BASE_TIMESTAMP = datetime(2024, 1, 1, tzinfo=UTC)
ENTITY_JOIN_KEY = "transaction_id"
TIMESTAMP_FIELD = "event_timestamp"


def build_offline_source(
    featurized_df: pd.DataFrame,
    output_path: Path | str = DEFAULT_OFFLINE_SOURCE_PATH,
    base_timestamp: datetime = DEFAULT_BASE_TIMESTAMP,
) -> Path:
    """Write the Feast offline-source parquet and return its path.

    Args:
        featurized_df: Output of `FeaturePipeline.transform()` — raw
            PaySim columns plus the registered engineered features.
        output_path: Where to write the parquet file.
        base_timestamp: `event_timestamp` = base_timestamp + step hours.
    """
    entity_ids = compute_entity_ids(featurized_df)
    event_timestamps = featurized_df["step"].apply(
        lambda step: base_timestamp + timedelta(hours=int(step))
    )

    result = featurized_df[list(feature_names())].copy()
    result.insert(0, TIMESTAMP_FIELD, event_timestamps)
    result.insert(0, ENTITY_JOIN_KEY, entity_ids)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(path, index=False)
    logger.info(
        "wrote feast offline source",
        extra={"path": str(path), "rows": len(result)},
    )
    return path
