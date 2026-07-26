"""Business logic for scoring a transaction — kept out of the API routes.

Pipeline: `compute_entity_id` (Milestone 5's deterministic Feast key)
-> `FeatureStore.read_online()` (the 9 engineered features, never
recomputed here) -> `data.preprocessing.preprocess()` (the same raw
drop/one-hot-`type` encoding training data went through) -> reindex to
the exact column order MLflow logged for the serving model's training
run -> `model.predict`/`predict_proba`.

Deliberately has no FastAPI import: it's plain Python operating on a
domain `Transaction` and a `FeatureStore`-protocol object, so it's
unit-testable (e.g. with `features.feature_store.LocalFeatureStore`)
without an HTTP server or real Feast/Redis.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from fraud_detection.api.schemas import PredictionResponse
from fraud_detection.common.logger import get_logger
from fraud_detection.data.preprocessing import preprocess
from fraud_detection.domain.entities import Transaction
from fraud_detection.domain.schemas import transaction_to_dict
from fraud_detection.features.entity_key import compute_entity_id
from fraud_detection.features.feature_store import FeatureStore
from fraud_detection.monitoring.prediction_log import append_prediction

logger = get_logger(__name__)


class PredictionService:
    """Scores a `Transaction` against the loaded model, once per request.

    `prediction_log_path` has no default: every call site must decide
    where predictions get logged. A previous version defaulted it to
    the real `monitoring.prediction_log.DEFAULT_LOG_PATH`, and test
    runs silently polluted that real "live production data" file
    (confirmed: a stubbed 0.99-probability test prediction and a
    synthetic-fixture transaction both ended up in it) — requiring an
    explicit path makes that a `TypeError` at construction time
    instead of a data-quality bug discovered later.
    """

    def __init__(
        self,
        model: Any,
        model_version: str,
        feature_order: tuple[str, ...],
        feature_store: FeatureStore,
        prediction_log_path: Path | str,
    ) -> None:
        self._model = model
        self._model_version = model_version
        self._feature_order = feature_order
        self._feature_store = feature_store
        self._prediction_log_path = prediction_log_path

    def predict(self, transaction: Transaction) -> PredictionResponse:
        """Score `transaction`.

        Raises:
            FeatureStoreError: Feast has no engineered features for
                this transaction yet — it hasn't flowed through the
                Milestone 5 Kafka -> Flink -> Feast pipeline. This
                service never recomputes features as a fallback (that
                would duplicate `FeaturePipeline`), so a miss here is
                surfaced to the caller, not silently absorbed.
        """
        start = time.perf_counter()

        entity_id = compute_entity_id(transaction)
        engineered_features = self._feature_store.read_online(entity_id)
        row = self._build_feature_row(transaction, engineered_features)

        prediction = int(self._model.predict(row)[0])
        probability = float(self._model.predict_proba(row)[:, 1][0])
        latency_ms = (time.perf_counter() - start) * 1000

        logger.info(
            "prediction complete",
            extra={
                "entity_id": entity_id,
                "prediction": prediction,
                "fraud_probability": probability,
                "model_version": self._model_version,
                "latency_ms": latency_ms,
            },
        )

        response = PredictionResponse(
            prediction=prediction,
            fraud_probability=probability,
            model_version=self._model_version,
            latency_ms=latency_ms,
        )
        self._log_prediction(transaction, response)
        return response

    def _log_prediction(self, transaction: Transaction, response: PredictionResponse) -> None:
        """Best-effort: a request that already produced a response
        shouldn't fail because the drift-monitoring log couldn't be
        written."""
        try:
            append_prediction(
                transaction,
                response.prediction,
                response.fraud_probability,
                response.model_version,
                log_path=self._prediction_log_path,
            )
        except OSError as exc:
            logger.error("failed to append prediction log", extra={"error": str(exc)})

    def _build_feature_row(
        self, transaction: Transaction, engineered_features: dict[str, Any]
    ) -> pd.DataFrame:
        """Reconstruct the exact row shape the model was trained on.

        Raw fields come from `transaction` itself (input, not
        engineered); `type` is one-hot encoded via the same
        `preprocess()` training used; the 9 engineered features come
        from Feast. Reindexing to `self._feature_order` (fill_value=0)
        only fills in the one-hot `type_*` columns this single row
        didn't produce (exactly one `type` is ever "hot") — it does
        not invent any feature value.
        """
        raw_row = pd.DataFrame([transaction_to_dict(transaction)])
        encoded_row = preprocess(raw_row)
        for name, value in engineered_features.items():
            encoded_row[name] = value

        row = encoded_row.reindex(columns=list(self._feature_order), fill_value=0)

        # pandas.get_dummies (via preprocess()) produces bool columns;
        # models.dataset._numeric_features casts those to int64 before
        # training, so serving must match or the model sees a dtype it
        # never trained on.
        bool_columns = row.select_dtypes(include="bool").columns
        if len(bool_columns) > 0:
            row = row.astype(dict.fromkeys(bool_columns, "int64"))
        return row
