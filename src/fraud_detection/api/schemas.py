"""Pydantic request/response models for the FastAPI boundary.

`TransactionRequest` mirrors the wire shape `domain.schemas.transaction_from_dict`
already parses (PaySim/Kafka field names) — Pydantic validates *shape*
(types, required fields) at the HTTP boundary; `transaction_from_dict`
still owns the domain contract itself (e.g. `type` must be a real
`TransactionType`), so there's exactly one place that defines what a
valid transaction is, reused across Kafka and HTTP.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from fraud_detection.domain.entities import TransactionType


class TransactionRequest(BaseModel):
    """A transaction to score, in the same field names PaySim/Kafka use.

    `type` reuses the domain `TransactionType` enum directly, so an
    invalid value is rejected by Pydantic at the boundary (a 422 with a
    clear "not a valid enumeration member" detail) rather than reaching
    `domain.schemas.transaction_from_dict` first.
    """

    step: int = Field(..., ge=0, description="Simulated hours elapsed since the start of PaySim")
    type: TransactionType
    amount: float = Field(..., ge=0)
    nameOrig: str
    oldbalanceOrg: float = Field(..., ge=0)
    newbalanceOrig: float = Field(..., ge=0)
    nameDest: str
    oldbalanceDest: float = Field(..., ge=0)
    newbalanceDest: float = Field(..., ge=0)
    isFlaggedFraud: int = 0

    model_config = {
        "json_schema_extra": {
            "example": {
                "step": 1,
                "type": "TRANSFER",
                "amount": 181.0,
                "nameOrig": "C1231006815",
                "oldbalanceOrg": 181.0,
                "newbalanceOrig": 0.0,
                "nameDest": "C1666544295",
                "oldbalanceDest": 0.0,
                "newbalanceDest": 0.0,
                "isFlaggedFraud": 0,
            }
        }
    }


class PredictionResponse(BaseModel):
    prediction: int = Field(..., description="0 (legitimate) or 1 (fraud) at the model's threshold")
    fraud_probability: float = Field(..., ge=0, le=1)
    model_version: str
    latency_ms: float = Field(..., ge=0)


class HealthResponse(BaseModel):
    status: str = "ok"


class ReadinessChecks(BaseModel):
    model_loaded: bool
    feast_reachable: bool
    redis_reachable: bool


class ReadyResponse(BaseModel):
    ready: bool
    checks: ReadinessChecks
