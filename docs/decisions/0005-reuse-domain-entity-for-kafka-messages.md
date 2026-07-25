# 5. Reuse the domain Transaction entity as the Kafka wire schema

Date: 2026-07-25

## Status

Accepted

## Context

The natural instinct when adding Kafka is to define a message schema
specific to the streaming layer — e.g. a Pydantic `Transaction` model
in a `contracts/` package, used by the producer and consumer. But this
project already has exactly that: `fraud_detection.domain.entities.Transaction`
(ADR-0002) plus `fraud_detection.domain.schemas.transaction_to_dict`/
`transaction_from_dict`, which already do dict/JSON (de)serialization
with validation (`InvalidTransactionError` on a bad payload), already
mirror `data/contracts/transaction_schema.json`, and are already used
by `data.ingestion` and `models`.

Adding a second `Transaction` definition — even a well-built one — would
mean two schemas to keep in sync, which is precisely the
training/serving-skew failure mode ADR-0003 was written to avoid, just
moved to the Kafka boundary instead of the feature-engineering one.

## Decision

`fraud_detection.streaming.serializer` is a thin translation layer
between Kafka message bytes and `domain.entities.Transaction`:

- `serialize_transaction(transaction) -> bytes`: JSON-encodes via
  `domain.schemas.transaction_to_dict`.
- `deserialize_transaction(payload) -> Transaction`: JSON-decodes, then
  builds via `domain.schemas.transaction_from_dict`, which already
  raises `InvalidTransactionError` for a malformed message — the
  producer/consumer don't need their own validation logic.

No Pydantic dependency was added. `dataclass(frozen=True, slots=True)` on
`Transaction` plus the existing hand-written validation in
`transaction_from_dict` already gives immutability and validation;
Pydantic would only be worth adding if a future boundary (e.g. a
FastAPI request body in Milestone 6) needs its request-parsing/
OpenAPI-schema-generation features specifically.

## Consequences

- Producer and consumer share one schema by construction — there is no
  second definition that can drift from the first.
- `data/contracts/transaction_schema.json` remains the one wire-format
  contract for a transaction, whether it arrives via CSV, Kafka, or
  (later) an HTTP request body.
- If Milestone 6's FastAPI layer wants Pydantic for request validation,
  that's a boundary-layer concern for `serving/`, not a reason to
  duplicate `domain.entities.Transaction` — `serving/` would translate
  Pydantic <-> domain `Transaction` the same way `streaming/serializer`
  translates JSON bytes <-> domain `Transaction`.
