"""Feast entity and feature view definitions.

Feature names/dtypes come directly from
`fraud_detection.features.registry.FEATURE_REGISTRY` — the same
registry `FeaturePipeline.transform()` validates its own output
against — so this file cannot silently define a feature the pipeline
doesn't actually produce, or vice versa. "Reuse the existing feature
engineering instead of redefining features" (Milestone 5) means this,
literally: there is no second feature list anywhere in this repo.

The `transaction` entity's join key is a derived id (see
`fraud_detection.features.entity_key`), not a real PaySim column — see
docs/decisions/0006-feast-redis-flink.md for why.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64

from fraud_detection.features.feast_prep import (
    DEFAULT_OFFLINE_SOURCE_PATH,
    ENTITY_JOIN_KEY,
    TIMESTAMP_FIELD,
)
from fraud_detection.features.registry import FEATURE_REGISTRY

_DTYPE_MAP = {"float64": Float64, "int64": Int64}

transaction = Entity(
    name="transaction",
    join_keys=[ENTITY_JOIN_KEY],
    value_type=ValueType.STRING,
    description="A PaySim transaction, keyed by a derived id (entity_key.compute_entity_id).",
)

transaction_source = FileSource(
    name="transaction_features_source",
    path=str(DEFAULT_OFFLINE_SOURCE_PATH),
    timestamp_field=TIMESTAMP_FIELD,
)

transaction_features = FeatureView(
    name="transaction_features",
    entities=[transaction],
    ttl=timedelta(days=3650),
    schema=[
        Field(name=definition.name, dtype=_DTYPE_MAP[definition.dtype])
        for definition in FEATURE_REGISTRY
    ],
    online=True,
    source=transaction_source,
)
