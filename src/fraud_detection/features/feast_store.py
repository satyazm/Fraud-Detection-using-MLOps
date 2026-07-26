"""Feast-backed implementation of the `FeatureStore` protocol.

Satisfies the exact same `fraud_detection.features.feature_store.FeatureStore`
`Protocol` that `LocalFeatureStore` does — ADR-0003 promised this back in
"before Milestone 3": "A Feast-backed implementation (Milestone 5)
satisfies the same Protocol, so feature_pipeline/transformers don't
change when Feast arrives." Nothing in `feature_pipeline.py`,
`transformers.py`, or `registry.py` changed for this milestone; this
file is the only new thing feature *computation* code needed to grow
a real online store.

Offline here means Feast's file-based offline store (see
`feast_prep.py` for how that parquet gets built); online means Feast's
Redis online store, written to either in bulk (`feast materialize`,
outside this class) or per-record (`write_online`, this class's job —
what the streaming worker calls).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from feast import FeatureStore as FeastClient
from redis.exceptions import RedisError

from fraud_detection.features.feature_store import FeatureStoreError
from fraud_detection.features.registry import feature_names

DEFAULT_FEATURE_VIEW = "transaction_features"
DEFAULT_ENTITY_JOIN_KEY = "transaction_id"


class FeastFeatureStore:
    """`FeatureStore`-protocol implementation backed by Feast + Redis."""

    def __init__(
        self,
        repo_path: Path | str,
        offline_source_path: Path | str,
        feature_view: str = DEFAULT_FEATURE_VIEW,
        entity_join_key: str = DEFAULT_ENTITY_JOIN_KEY,
    ) -> None:
        self._client = FeastClient(repo_path=str(repo_path))
        self._offline_source_path = Path(offline_source_path)
        self._feature_view = feature_view
        self._entity_join_key = entity_join_key
        self._feature_refs = [f"{feature_view}:{name}" for name in feature_names()]

    def online_store_address(self) -> tuple[str, int]:
        """Host/port Feast is actually configured to reach Redis at.

        Parsed from the same `feature_store.yaml` Feast itself loaded
        (`connection_string`, e.g. `"redis:6379"` or
        `"localhost:6379,db=0"`) — the single source of truth for where
        Redis is, rather than a second hardcoded host/port a caller
        could keep separately and let drift out of sync. That drift is
        exactly what happened before this existed: the API's `/ready`
        check hardcoded "localhost:6379" for its live Redis probe, which
        was wrong inside a container where Feast itself was correctly
        configured for "redis:6379" — `/ready` reported `redis_reachable:
        false` even though Feast could reach Redis fine.
        """
        connection_string = self._client.config.online_store.connection_string
        host, port = connection_string.split(",")[0].split(":")
        return host, int(port)

    def write_offline_batch(self, df: pd.DataFrame) -> None:
        """Persist `df` as this store's Feast FileSource.

        `df` must already have the entity join key, `event_timestamp`,
        and the registered feature columns — see `feast_prep.py`.
        """
        self._offline_source_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self._offline_source_path, index=False)

    def read_offline_batch(self) -> pd.DataFrame:
        if not self._offline_source_path.exists():
            raise FeatureStoreError(
                f"No offline features written yet at {self._offline_source_path}"
            )
        return pd.read_parquet(self._offline_source_path)

    def write_online(self, entity_id: str, features: dict[str, Any]) -> None:
        """Push one row of features directly into the Redis online store.

        This is Feast's push API, not `feast materialize` — it's what
        lets a streaming record become queryable via `read_online()`
        immediately, without waiting for a batch materialization run.
        """
        row = {
            self._entity_join_key: [entity_id],
            "event_timestamp": [pd.Timestamp.now(tz="UTC")],
            **{name: [value] for name, value in features.items()},
        }
        self._client.write_to_online_store(
            feature_view_name=self._feature_view, df=pd.DataFrame(row)
        )

    def read_online(self, entity_id: str) -> dict[str, Any]:
        """Raises `FeatureStoreError` both when Redis has no entry for
        `entity_id` and when Redis itself can't be reached — callers
        (e.g. the API's `PredictionService`) need exactly one exception
        type to handle "features aren't available right now", not two.
        """
        try:
            response = self._client.get_online_features(
                features=self._feature_refs,
                entity_rows=[{self._entity_join_key: entity_id}],
            ).to_dict()
        except RedisError as exc:
            raise FeatureStoreError(f"Could not reach the online store (Redis): {exc}") from exc

        values = {name: response[name][0] for name in feature_names()}
        if all(value is None for value in values.values()):
            raise FeatureStoreError(f"No online features found for entity_id={entity_id!r}")
        return values
