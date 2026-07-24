"""Core domain entities.

Plain, immutable data objects — the business vocabulary of the platform.
Kafka payloads, model inputs/outputs, and API request/response bodies all
get translated to/from these at the layer boundary (see `schemas.py`),
so downstream code works with `Transaction`/`Prediction` objects rather
than raw dicts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True, slots=True)
class Prediction:
    """A model's fraud score for a given transaction."""

    transaction_id: str
    fraud_probability: float
    model_version: str
    scored_at: datetime


class FraudDecision(str, Enum):
    """The business decision derived from a Prediction."""

    ALLOW = "ALLOW"
    REVIEW = "REVIEW"
    BLOCK = "BLOCK"
