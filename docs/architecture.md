# Architecture

## Status

Milestones 1-5 are implemented: `common`, `domain`, `data`, `features`
(including a real Feast + Redis integration), `models`, and `streaming`
(Kafka producer/consumer, plus a real PyFlink job computing features
from the live stream — no model inference in the stream yet) are real.
`serving` and `monitoring` are still scaffolding — this document
records intent for them so those milestones have a target to build
toward, and so deviations get captured as ADRs (see `docs/decisions/`).

## Layering (clean architecture)

`src/fraud_detection/` is organized by responsibility, not by technology,
so infrastructure (Kafka, Redis, MLflow, ...) stays swappable behind each
layer's interface:

| Package     | Responsibility                                            |
|-------------|------------------------------------------------------------|
| `domain`    | Business entities (`Transaction`, `Prediction`, `FraudDecision`) and domain exceptions — depends on nothing else in this package |
| `common`    | Config loading, logging — no dependency on any other layer |
| `data`      | Ingestion, validation, preprocessing, splitting for the PaySim dataset |
| `features`  | One feature pipeline shared by offline training, online inference, and streaming (ADR-0003); Feast + Redis integration (ADR-0006) |
| `models`    | Training, comparison, evaluation, MLflow tracking/registry |
| `streaming` | Kafka producer/consumer sharing `domain.entities.Transaction` as the wire schema (ADR-0005); a PyFlink job computing features from the live stream via the same `FeaturePipeline` (ADR-0006) |
| `serving`   | Inference API (FastAPI) that scores transactions            |
| `monitoring`| Data/model drift and performance observability              |
| `utils`     | Generic, dependency-free helpers                            |

Dependency direction runs one way, inward toward `domain`:
`serving`/`streaming`/`monitoring` depend on `models` and `features`,
which depend on `data`, `domain`, and `common` — never the reverse.
Boundary layers (`streaming`, `serving`) translate wire formats (Kafka
JSON, HTTP bodies) into `domain` entities via `domain.schemas`, so
`models`/`features` work with typed entities, not raw dicts. The wire
contract itself is versioned in `data/contracts/transaction_schema.json`.
See ADR-0002 (`docs/decisions/0002-clean-architecture-layering.md`) for
the full rationale.

## Target end-to-end flow

```
PaySim dataset
      |
      v
  data (ingest/validate)
      |
      v
  features (engineer)  ---------------------------+
      |                                            |
      v                                            v
  models (train, evaluate)              feast_prep / feast materialize
      |  --> MLflow (tracking + registry)          |
      v                                            v
  [ once a baseline model is validated ]         Redis (Feast online store)
      |                                            ^
      v                                            |
  streaming: producer -> Kafka -> flink-worker -----+
      |         (FeaturePipeline.transform_one() -> Feast push)
      v
  serving (FastAPI: Feast lookup -> model.predict_proba())
      |
      v
  monitoring (drift, performance, alerting) --> Prometheus/Grafana
```

The model is proven offline first (Milestones 2-3). The real-time
feature platform (Milestone 5) proves Kafka -> features -> online store
parity *before* wiring a model into that path — see ADR-0001 for how
decisions like this are tracked, and ADR-0006 for Milestone 5's
specifics (Feast, Redis, why local-execution PyFlink).

## Configuration & logging

- Config: `configs/base.yaml` + `configs/{env}.yaml` overlay, loaded via
  `fraud_detection.common.config.load_config(env)`. `env` defaults to the
  `APP_ENV` variable, then `"dev"`.
- Logging: `configs/logging.yaml` (structured JSON to stdout), loaded via
  `fraud_detection.common.logger.setup_logging()`.

## Local infrastructure

`docker-compose.yml` at the repo root defines the target local stack.
Kafka (+ Kafka UI) and Redis are live, used by `streaming/` and
`features/`'s Feast integration respectively. MLflow, Prometheus, and
Grafana remain scaffolding for later milestones.
