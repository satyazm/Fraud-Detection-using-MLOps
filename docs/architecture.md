# Architecture

## Status

Phase 1: scaffolding only. Nothing described below as "future" is
implemented yet — this document records intent so implementation phases
have a target to build toward, and so deviations get captured as ADRs
(see `docs/decisions/`).

## Layering (clean architecture)

`src/fraud_detection/` is organized by responsibility, not by technology,
so infrastructure (Kafka, Redis, MLflow, ...) stays swappable behind each
layer's interface:

| Package     | Responsibility                                            |
|-------------|------------------------------------------------------------|
| `common`    | Config loading, logging — no dependency on any other layer |
| `data`      | Ingestion, validation, schemas for the PaySim dataset       |
| `features`  | Feature engineering, shared between training and serving   |
| `models`    | Training, evaluation, model registry integration           |
| `streaming` | Kafka producers/consumers for real-time transaction events |
| `serving`   | Inference API (FastAPI) that scores transactions            |
| `monitoring`| Data/model drift and performance observability              |
| `utils`     | Generic, dependency-free helpers                            |

Dependency direction runs one way: `serving`/`streaming`/`monitoring`
depend on `models` and `features`, which depend on `data` and `common` —
never the reverse.

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

`docker-compose.yml` at the repo root defines the target local stack
(Kafka, Redis, MLflow, Prometheus, Grafana). Nothing in the codebase
talks to these services yet.
