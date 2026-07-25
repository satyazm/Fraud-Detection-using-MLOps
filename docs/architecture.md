# Architecture

## Status

Milestones 1-4 are implemented: `common`, `domain`, `data`, `features`,
`models`, and `streaming` (producer/consumer; no inference in the
stream yet) are real. `serving` and `monitoring` are still scaffolding
— this document records intent for them so those milestones have a
target to build toward, and so deviations get captured as ADRs (see
`docs/decisions/`).

## Layering (clean architecture)

`src/fraud_detection/` is organized by responsibility, not by technology,
so infrastructure (Kafka, Redis, MLflow, ...) stays swappable behind each
layer's interface:

| Package     | Responsibility                                            |
|-------------|------------------------------------------------------------|
| `domain`    | Business entities (`Transaction`, `Prediction`, `FraudDecision`) and domain exceptions — depends on nothing else in this package |
| `common`    | Config loading, logging — no dependency on any other layer |
| `data`      | Ingestion, validation, preprocessing, splitting for the PaySim dataset |
| `features`  | One feature pipeline shared by offline training, online inference, and future streaming (ADR-0003) |
| `models`    | Training, comparison, evaluation, MLflow tracking/registry |
| `streaming` | Kafka producer/consumer, sharing `domain.entities.Transaction` as the wire schema (ADR-0005) |
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
  features (engineer)
      |
      v
  models (train, evaluate) --> MLflow (tracking + registry)
      |
      v
  [ once a baseline model is validated ]
      |
      v
  streaming (Kafka producer/consumer) --> serving (FastAPI inference)
      |
      v
  monitoring (drift, performance, alerting) --> Prometheus/Grafana
```

The model is proven offline first (Phase 2). Streaming and serving
infrastructure (Phases 4-5) wrap the validated model rather than the
other way around — see ADR-0001 for how decisions like this are tracked.

## Configuration & logging

- Config: `configs/base.yaml` + `configs/{env}.yaml` overlay, loaded via
  `fraud_detection.common.config.load_config(env)`. `env` defaults to the
  `APP_ENV` variable, then `"dev"`.
- Logging: `configs/logging.yaml` (structured JSON to stdout), loaded via
  `fraud_detection.common.logger.setup_logging()`.

## Local infrastructure

`docker-compose.yml` at the repo root defines the target local stack.
Kafka (+ Kafka UI) is live and used by `streaming/`. Redis, MLflow,
Prometheus, and Grafana remain scaffolding for later milestones.
