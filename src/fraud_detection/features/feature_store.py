"""Feature storage abstraction.

`FeatureStore` is the interface every backend implements.
`LocalFeatureStore` is a local, in-process implementation for now; a
Feast-backed store (Milestone 5, with Redis as its online store) will
satisfy the same `Protocol` later without any change to
`feature_pipeline.py` or `transformers.py` — this module depends on
those, never the reverse. See
docs/decisions/0003-shared-feature-pipeline.md.

Two operations, matching the offline/online split every feature store
(Feast included) makes:
- `write_offline_batch` / `read_offline_batch`: the bulk feature table
  used for training, backed by a file.
- `write_online` / `read_online`: point lookups for serving, keyed by
  an entity id the caller supplies (e.g. a Kafka message key). Key
  generation is deliberately not this module's concern.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import pandas as pd


class FeatureStoreError(Exception):
    """Raised when a feature store read or write can't be completed."""


class FeatureStore(Protocol):
    def write_offline_batch(self, df: pd.DataFrame) -> None: ...

    def read_offline_batch(self) -> pd.DataFrame: ...

    def write_online(self, entity_id: str, features: dict[str, Any]) -> None: ...

    def read_online(self, entity_id: str) -> dict[str, Any]: ...


class LocalFeatureStore:
    """Filesystem/in-memory `FeatureStore` for local dev and tests.

    Offline features are persisted to a parquet file; online features
    live in an in-memory dict. Enough to exercise the training/serving
    contract before Feast + Redis exist; not intended to survive a
    process restart for the online side.
    """

    def __init__(self, offline_path: Path | str) -> None:
        self._offline_path = Path(offline_path)
        self._online_cache: dict[str, dict[str, Any]] = {}

    def write_offline_batch(self, df: pd.DataFrame) -> None:
        self._offline_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self._offline_path, index=False)

    def read_offline_batch(self) -> pd.DataFrame:
        if not self._offline_path.exists():
            raise FeatureStoreError(f"No offline features written yet at {self._offline_path}")
        return pd.read_parquet(self._offline_path)

    def write_online(self, entity_id: str, features: dict[str, Any]) -> None:
        self._online_cache[entity_id] = features

    def read_online(self, entity_id: str) -> dict[str, Any]:
        try:
            return self._online_cache[entity_id]
        except KeyError as exc:
            raise FeatureStoreError(
                f"No online features cached for entity_id={entity_id!r}"
            ) from exc
