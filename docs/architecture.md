# Architecture

## Status

Milestones 1-7 are implemented: `common`, `domain`, `data`, `features`
(including a real Feast + Redis integration), `models`, `streaming`
(Kafka producer/consumer, plus a real PyFlink job computing features
from the live stream), `api` (a FastAPI service scoring transactions
via Feast online features + an MLflow Production model), and
`monitoring` (Prometheus metrics, an Evidently AI data-drift report,
and the live-prediction log it reads from) are all real. Milestone 8
(Airflow, CI/CD, Kubernetes) has not started — this document records
intent for it so that milestone has a target to build toward, and so
deviations get captured as ADRs (see `docs/decisions/`).

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
| `api`       | FastAPI inference service: Feast online features (never recomputed) + an MLflow Production model -> fraud probability (ADR-0007) |
| `monitoring`| Prometheus metrics, the live-prediction log, and Evidently AI data-drift reports (ADR-0008) |
| `utils`     | Generic, dependency-free helpers                            |

Dependency direction runs one way, inward toward `domain`:
`api`/`streaming`/`monitoring` depend on `models` and `features`,
which depend on `data`, `domain`, and `common` — never the reverse.
Boundary layers (`streaming`, `api`) translate wire formats (Kafka
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
  api: Transaction -> Feast.read_online() -> MLflow Production model -> prediction
      |            \
      v              \--> monitoring.prediction_log (real logged requests)
  /metrics --> Prometheus --> Grafana                    |
      ^                                                    v
      |                                     fraud-detection drift-report
  redis-exporter, cadvisor                     (Evidently AI, vs. training data)
```

The model is proven offline first (Milestones 2-3). The real-time
feature platform (Milestone 5) proved Kafka -> features -> online store
parity *before* wiring a model into that path (ADR-0006); Milestone 6
wired the model in via `api`, reading the same online features rather
than recomputing them; Milestone 7 added observability around that —
see ADR-0001 for how decisions like this are tracked, ADR-0006 for
Milestone 5's specifics (Feast, Redis, why local-execution PyFlink),
ADR-0007 for Milestone 6's (Feast-or-error, Production-stage
resolution, `/health` vs `/ready`), and ADR-0008 for Milestone 7's
(which metrics and why, real vs. synthetic drift data, and what's
deliberately not covered — Kafka broker metrics, ground-truth model
performance).

## Configuration & logging

- Config: `configs/base.yaml` + `configs/{env}.yaml` overlay, loaded via
  `fraud_detection.common.config.load_config(env)`. `env` defaults to the
  `APP_ENV` variable, then `"dev"`.
- Logging: `configs/logging.yaml` (structured JSON to stdout), loaded via
  `fraud_detection.common.logger.setup_logging()`.

## Local infrastructure

`docker-compose.yml` at the repo root defines the target local stack.
Kafka (+ Kafka UI) and Redis are live, used by `streaming/` and
`features/`'s Feast integration respectively. The `api` service builds
and runs the FastAPI inference service (`docker/Dockerfile.api`),
pointing at the same local `mlruns/`/`feast_repo/` host-side commands
already use (bind-mounted), not the separate `mlflow` server below —
verified end to end (real build, real container, real `/predict` call
against a real model), see ADR-0007. Prometheus (scraping the `api`
service, `redis-exporter`, and `cadvisor`) and Grafana
(auto-provisioned with a dashboard, no manual setup) are live too —
see ADR-0008. Only the standalone `mlflow` tracking server remains
scaffolding for a later milestone.
