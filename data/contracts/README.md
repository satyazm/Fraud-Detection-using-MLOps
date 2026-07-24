# data/contracts/

Versioned wire-format contracts shared across the platform's boundaries
(PaySim batch data, Kafka producer/consumer, FastAPI serving).

- `transaction_schema.json` — JSON Schema for a single transaction
  event. Kept in sync with `fraud_detection.domain.entities.Transaction`
  and `fraud_detection.domain.schemas`.
