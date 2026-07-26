# Architecture

## Status

Milestones 1-8 are implemented: `common`, `domain`, `data`, `features`
(including a real Feast + Redis integration), `models`, `streaming`
(Kafka producer/consumer, plus a real PyFlink job computing features
from the live stream), `api` (a FastAPI service scoring transactions
via Feast online features + an MLflow Production model), `monitoring`
(Prometheus metrics, an Evidently AI data-drift report, and the
live-prediction log it reads from), and deployment/operations (a real
Kubernetes deployment on a local `kind` cluster, three Airflow DAGs
orchestrating this project's own CLI via `DockerOperator`, and an
expanded CI covering Docker builds, Kubernetes manifest validation,
and DAG import errors) are all real. Stress testing (also named in the
Milestone 8 brief) is not — see ADR-0009 for exactly what was and
wasn't covered and why.

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
than recomputing them; Milestone 7 added observability around that;
Milestone 8 put all of it on a real Kubernetes cluster and added the
scheduling/CI layer around it — see ADR-0001 for how decisions like
this are tracked, ADR-0006 for Milestone 5's specifics (Feast, Redis,
why local-execution PyFlink), ADR-0007 for Milestone 6's (Feast-or-error,
Production-stage resolution, `/health` vs `/ready`), ADR-0008 for
Milestone 7's (which metrics and why, real vs. synthetic drift data,
and what's deliberately not covered — Kafka broker metrics,
ground-truth model performance), and ADR-0009 for Milestone 8's (the
Kubernetes/MLflow artifact-proxying bug, three real bugs found running
the Airflow DAGs for real, and why CI validates manifests with
`kubeconform` rather than `kubectl apply --dry-run`).

## Kubernetes deployment and orchestration (Milestone 8)

`kubernetes/` holds the manifests for a real local `kind` cluster
running the same system: `redis`, `kafka`, and `mlflow` as
Deployments (the standalone MLflow tracking server, now actually used
here — unlike Docker Compose's `api`, K8s pods can't share a host
filesystem, so `api`/`training-job` point `MLFLOW_TRACKING_URI` at
this server over HTTP instead of a local `mlruns/` file store);
`api` as a 3-replica Deployment behind a Service, scaled 2-10 by an
HPA on CPU and reachable externally via an nginx `Ingress`;
`flink-worker` as a long-running Deployment; `training-job`/
`producer-job` as one-shot Jobs that make a fresh cluster
self-sufficient; and `prometheus`/`grafana`/`redis-exporter` mirroring
the Docker Compose monitoring stack. `docker/Dockerfile.worker` (a
generic image parameterized by CLI subcommand at runtime) backs both
the Jobs and every DockerOperator task in Airflow, below; `flink-worker`
gets its own image (`docker/Dockerfile.flink-worker`) because it's the
only one needing a JVM.

Orchestration on top of that runs as its own Docker Compose stack
(`airflow/docker-compose.yml`), not embedded in this project's own
Python environment: `airflow/dags/daily_feature_materialization.py`,
`weekly_retraining.py`, and `monthly_drift_report.py` each chain
`DockerOperator` tasks that launch `mloops-worker:latest` as sibling
containers, running this project's existing CLI rather than
reimplementing any of it. See ADR-0009 for what actually broke running
all of this for real (an MLflow artifact-proxying bug and an OOM, a
scheduler healthcheck that could never pass, an under-resourced DAG
that threatened the whole shared Docker VM, and a hardcoded
`localhost` Redis address) and how each was fixed.

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
see ADR-0008. The standalone `mlflow` tracking server remains unused
by this Docker Compose stack specifically (`api` here still reads the
local `mlruns/` file store directly, per ADR-0007) — it's the
Kubernetes deployment (below) that actually serves it.
