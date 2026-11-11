"""Core domain entities.

Plain, immutable data objects — the business vocabulary of the platform.
Kafka payloads and API request bodies are translated to/from these at
the layer boundary (see `schemas.py`), so downstream code works with
`Transaction` objects rather than raw dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TransactionType(str, Enum):
    CASH_IN = "CASH_IN"
    CASH_OUT = "CASH_OUT"
    DEBIT = "DEBIT"
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"


@dataclass(frozen=True, slots=True)
class Transaction:
    """A single mobile-money transaction, as sourced from PaySim/Kafka."""

    step: int
    type: TransactionType
    amount: float
    name_orig: str
    oldbalance_org: float
    newbalance_orig: float
    name_dest: str
    oldbalance_dest: float
    newbalance_dest: float
    is_flagged_fraud: bool = False
