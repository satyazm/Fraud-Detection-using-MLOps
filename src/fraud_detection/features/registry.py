"""Single source of truth for engineered feature metadata.

Every column `transformers.py` adds must have a matching entry here.
`FeaturePipeline.transform()` validates its output against this
registry on every run, so a transformer/registry drift (a feature added
without documenting it, or a registry entry with nothing producing it)
fails fast instead of silently shipping. This is also what a Feast
`FeatureView` will eventually be generated from (Milestone 5) — one
definition of "what a feature is," not two. See
docs/decisions/0003-shared-feature-pipeline.md.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd


class FeatureRegistryError(Exception):
    """Raised when a pipeline's output doesn't match the feature registry."""


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    dtype: str
    description: str
    source_columns: tuple[str, ...]


FEATURE_REGISTRY: tuple[FeatureDefinition, ...] = (
    FeatureDefinition(
        name="orig_balance_error",
        dtype="float64",
        description=(
            "oldbalanceOrg - amount - newbalanceOrig; nonzero indicates an "
            "inconsistent origin balance."
        ),
        source_columns=("oldbalanceOrg", "amount", "newbalanceOrig"),
    ),
    FeatureDefinition(
        name="dest_balance_error",
        dtype="float64",
        description=(
            "oldbalanceDest + amount - newbalanceDest; nonzero indicates an "
            "inconsistent destination balance."
        ),
        source_columns=("oldbalanceDest", "amount", "newbalanceDest"),
    ),
    FeatureDefinition(
        name="orig_balance_delta",
        dtype="float64",
        description="newbalanceOrig - oldbalanceOrg.",
        source_columns=("newbalanceOrig", "oldbalanceOrg"),
    ),
    FeatureDefinition(
        name="dest_balance_delta",
        dtype="float64",
        description="newbalanceDest - oldbalanceDest.",
        source_columns=("newbalanceDest", "oldbalanceDest"),
    ),
    FeatureDefinition(
        name="is_orig_balance_depleted",
        dtype="int64",
        description=(
            "1 if the origin account had a positive balance that was fully "
            "drained by this transaction."
        ),
        source_columns=("oldbalanceOrg", "newbalanceOrig"),
    ),
    FeatureDefinition(
        name="is_dest_balance_untouched",
        dtype="int64",
        description=(
            "1 if the destination account shows a zero balance both before and "
            "after a nonzero transaction — a common signature of mule/fraud "
            "accounts in PaySim."
        ),
        source_columns=("oldbalanceDest", "newbalanceDest", "amount"),
    ),
    FeatureDefinition(
        name="amount_to_orig_balance_ratio",
        dtype="float64",
        description="amount divided by the origin account's balance before the transaction.",
        source_columns=("amount", "oldbalanceOrg"),
    ),
    FeatureDefinition(
        name="hour_of_day",
        dtype="int64",
        description="step modulo 24; PaySim's `step` is simulated hours elapsed.",
        source_columns=("step",),
    ),
    FeatureDefinition(
        name="is_dest_merchant",
        dtype="int64",
        description="1 if the destination account id has PaySim's merchant prefix ('M').",
        source_columns=("nameDest",),
    ),
)


def feature_names() -> tuple[str, ...]:
    return tuple(definition.name for definition in FEATURE_REGISTRY)


def validate_features_present(df: pd.DataFrame) -> None:
    """Raise if `df` is missing any column the registry says should exist."""
    missing = [name for name in feature_names() if name not in df.columns]
    if missing:
        raise FeatureRegistryError(f"Pipeline output is missing registered features: {missing}")


def feature_version() -> str:
    """Stable short hash identifying the current set of registered features.

    Changes whenever a feature is added, removed, or redefined (name,
    dtype, or source columns change). Logged alongside every MLflow
    training run so a run can always be traced back to the feature
    definitions that produced its training data.
    """
    payload = "|".join(f"{d.name}:{d.dtype}:{','.join(d.source_columns)}" for d in FEATURE_REGISTRY)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
