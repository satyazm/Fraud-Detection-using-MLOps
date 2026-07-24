# 2. Adopt clean architecture with an explicit domain layer

Date: 2026-07-24

## Status

Accepted

## Context

The platform integrates several technologies that are each likely to
change independently over its lifecycle: Kafka today, possibly a
different broker later; FastAPI today, possibly gRPC later;
XGBoost/LightGBM today, possibly a different model family later.
Business logic — what a transaction is, what a fraud decision means —
must not be entangled with any single one of these choices, or swapping
one forces changes throughout the codebase.

## Decision

Use clean/hexagonal architecture, with `fraud_detection.domain` as the
innermost layer:

- `domain/` holds framework-agnostic business objects (`Transaction`,
  `Prediction`, `FraudDecision`) and domain exceptions. It imports
  nothing from `data`, `features`, `models`, `streaming`, `serving`, or
  `monitoring`.
- Those other layers depend on `domain` and `common`, never on each
  other's internals.
- Boundary layers (`streaming`, `serving`) translate wire formats
  (Kafka JSON, HTTP bodies) into domain entities and back via
  `domain.schemas`; `models`/`features` operate only on domain
  entities, never on raw Kafka messages or HTTP payloads.
- The wire-format contract for a transaction is versioned separately in
  `data/contracts/transaction_schema.json`, kept in sync with
  `fraud_detection.domain.entities.Transaction`.

## Consequences

- Swapping Kafka, FastAPI, or the model implementation should only
  touch the corresponding boundary layer, not `domain` or the business
  logic in `models`/`features`.
- Slightly more boilerplate up front (explicit entities and
  (de)serialization functions instead of passing dicts around) in
  exchange for that isolation.
- `data/contracts/transaction_schema.json` and
  `fraud_detection.domain.entities.Transaction` must be kept in sync by
  hand for now; a contract test enforcing this is a natural addition
  once the streaming layer exists.
