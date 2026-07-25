"""Deterministic entity key derivation for the Feast `transaction` entity.

PaySim has no natural transaction id (a real production system would
carry one from the payment processor). We derive one deterministically
from fields that together identify a transaction, so the *same*
transaction always gets the *same* key whether it's being materialized
from batch data or pushed from the live stream — that's what makes
offline-prepared rows and online-pushed rows addressable by the same
identity in `get_online_features()`. See
docs/decisions/0006-feast-redis-flink.md.

`compute_entity_ids` (the batch/DataFrame path) deliberately calls
`compute_entity_id` (the single-transaction path) per row rather than
reimplementing the hash — a second implementation is exactly how the
two could silently drift apart and break that identity guarantee.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

from fraud_detection.domain.entities import Transaction
from fraud_detection.domain.schemas import transaction_from_dict

# transaction_from_dict requires all of these, even though the hash
# payload itself only uses a subset (see compute_entity_id).
_RAW_COLUMNS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
)


def compute_entity_id(transaction: Transaction) -> str:
    """Stable id for a single Transaction, used as the Feast entity key."""
    payload = (
        f"{transaction.step}:{transaction.type.value}:{transaction.name_orig}:"
        f"{transaction.name_dest}:{transaction.amount}:{transaction.oldbalance_org}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def compute_entity_ids(df: pd.DataFrame) -> pd.Series[Any]:
    """Entity ids for every row of a raw/domain-shaped DataFrame.

    `df` must have the raw PaySim/Transaction schema (the same columns
    `data.ingestion` and `FeaturePipeline.transform()` both produce) —
    call this *before* `data.preprocessing.preprocess()` drops
    `nameOrig`/`nameDest`, which this needs.
    """
    raw_rows = df[list(_RAW_COLUMNS)].to_dict(orient="records")
    ids = [
        compute_entity_id(transaction_from_dict({str(k): v for k, v in row.items()}))
        for row in raw_rows
    ]
    return pd.Series(ids, index=df.index)
