# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in milestones. **Milestones 1-8 are
done**: project scaffold, a shared feature-engineering pipeline, the
data pipeline, model training/comparison with MLflow tracking and
registry, Kafka streaming, a real-time feature platform (Feast +
Redis + a real PyFlink streaming job), a real-time inference API
(FastAPI: Feast online features + an MLflow Production model -> fraud
probability), observability (Prometheus metrics, an auto-provisioned
Grafana dashboard, and Evidently AI data-drift reports), and
deployment/operations (a real Kubernetes deployment, three Airflow
DAGs, and an expanded CI). Stress testing, also named in the Milestone
8 brief, is not done — see ADR-0009.

## Project layout

```
configs/             YAML configuration: base.yaml + {env}.yaml overlays, logging.yaml
data/
  raw/                Original, immutable source data (gitignored contents)
  processed/          train/validation/test parquet splits, features included (gitignored)
  sample/             Small samples for tests/local dev (gitignored contents)
  contracts/          Versioned wire-format contracts (e.g. transaction_schema.json)
  feast/              Feast's offline FileSource parquet (gitignored, generated)
  monitoring/         Live prediction log (gitignored, generated) — see monitoring/prediction_log.py
feast_repo/           Feast feature repo: feature_store.yaml + definitions.py
src/fraud_detection/  Installable package (see Package layout below)
tests/                Pytest test suite (mirrors src/fraud_detection layout)
docs/
  architecture.md      System architecture, layering, target data flow
  decisions/            Architecture Decision Records (ADRs)
  data_report.md         Generated PaySim data quality report
  model_report.md         Generated model comparison report
  drift_report.html       Generated Evidently AI drift report (gitignored — several MB, over this repo's 1MB pre-commit cap)
  images/                  Generated plots referenced by the reports above
mlruns/               MLflow tracking store (gitignored, local-only)
.flink-jars/          Flink<->Kafka connector JAR (gitignored, downloaded by `make flink-jar`)
scripts/              One-off operational/data scripts
docker/               Dockerfile.api (the inference service), feature_store.docker.yaml,
                        prometheus.yml, grafana/ (auto-provisioned datasource + dashboard)
docker-compose.yml    Local dev stack: Kafka, Redis, MLflow, api, Prometheus + redis-exporter
                        + cadvisor + Grafana
airflow/              Airflow DAGs
kubernetes/           Kubernetes manifests / Helm charts
.github/workflows/    CI pipelines
requirements/         Pinned dependency sets (base/dev/prod)
```

### Package layout

`src/fraud_detection/` is a single installable package, organized by
clean-architecture layer (see `docs/architecture.md` for the dependency
rules between layers):

```
fraud_detection/
  domain/        Business entities (Transaction, Prediction, FraudDecision) — depends on nothing else
  common/        Config loading, logging
  cli.py         `fraud-detection` command-line entry point
  data/          Ingestion, validation, preprocessing, splitting
  features/      Feature pipeline (ADR-0003) + Feast integration (ADR-0006):
                    feature_pipeline.py, transformers.py, registry.py   — the one feature implementation
                    entity_key.py                                       — deterministic Feast entity id
                    feast_prep.py                                       — builds the offline source parquet
                    feast_store.py                                      — FeatureStore protocol, Feast-backed
                    feast_ops.py                                        — feast apply/materialize
  models/        Training, comparison, evaluation, MLflow tracking/registry
  streaming/     Kafka producer/consumer (ADR-0005) + PyFlink job (ADR-0006):
                    producer.py, consumer.py, serializer.py             — Milestone 4
                    flink_job.py                                        — Kafka -> transform_one() -> Feast/Redis
  api/           Real-time inference API (ADR-0007):
                    app.py                — FastAPI app, startup/lifespan (loads the model once)
                    dependencies.py        — AppState + Depends() providers
                    prediction_service.py  — entity lookup -> Feast features -> model.predict_proba()
                    routers.py             — /health, /ready, /metrics, /predict
                    schemas.py             — Pydantic request/response models
  monitoring/    Observability (ADR-0008):
                    metrics.py             — Prometheus metric definitions + the shared redis live-check
                    prediction_log.py      — appends real /predict requests (drift's "live" data)
                    drift.py               — Evidently AI report: training data vs. prediction_log
  utils/         Generic helpers
```

Importing is unambiguous and namespace-safe, e.g.:

```python
from fraud_detection.common.config import load_config
```

## Requirements

- Python 3.11
- The PaySim CSV (`PS_20174660362_1_log.csv` from
  [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)) placed at
  `data/raw/PS_20174660362_1_log.csv` before running the data pipeline.
- Docker + Docker Compose, running, before using `producer`/`consumer`/
  `materialize`/`flink-worker`/`api` or their tests (they need real
  Kafka at `localhost:9092` and/or Redis at `localhost:6379`).
- A JDK (11, 17, or 21) for PyFlink, e.g. `brew install openjdk@17`,
  with `JAVA_HOME` set — see "Real-time feature platform" below.

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate

make install-dev   # installs dev deps, the package (editable), and git pre-commit hooks
```

`install`/`install-dev` handle a real quirk: `apache-flink` (PyFlink)
needs `setuptools<81` present before it builds (apache-beam's setup.py
uses `pkg_resources`) and must install with `--no-build-isolation` so
the build can see it — both Makefile targets already do this, a plain
`pip install -r requirements/dev.txt` will not work.

Copy `.env.example` to `.env` and fill in values as later milestones need
them (nothing reads `.env` yet).

## Common tasks

```bash
make lint        # ruff + black --check (src, tests, feast_repo)
make format      # ruff --fix + black
make typecheck   # mypy
make test        # pytest with coverage
make precommit   # run all pre-commit hooks against the full repo

# Data pipeline (Milestone 2):
make ingest      # load + schema-validate data/raw/PS_20174660362_1_log.csv
make validate    # generate docs/data_report.md and docs/images/*.png
make preprocess  # feature-engineer, clean, stratified-split, save to data/processed/

# Model training (Milestone 3):
make train       # train + compare LR/RF/XGBoost/LightGBM, log to MLflow, register the best
make evaluate    # evaluate the latest registered model against the test split

# Kafka streaming (Milestone 4):
make kafka-up    # start Kafka + Kafka UI (localhost:8080)
make producer    # stream PaySim transactions onto the `transactions` topic
make consumer    # consume and log transactions from the `transactions` topic
make kafka-down  # stop them

# Real-time feature platform (Milestone 5):
make redis-up       # start Redis (Feast's online store)
make infra-up       # kafka-up + redis-up together
make flink-jar      # one-time: download the Flink<->Kafka connector JAR
make feast-apply    # register Feast entity/feature-view definitions
make materialize    # build the offline source + push it into Redis via Feast
make flink-worker   # Kafka -> FeaturePipeline.transform_one() -> Feast/Redis, continuously
make infra-down     # stop kafka, kafka-ui, redis

# Real-time inference API (Milestone 6):
make api         # run the FastAPI service locally (localhost:8000)
make ready       # probe a running instance's /ready endpoint
make api-build   # build the Docker image
make api-up      # run it via docker compose (needs infra-up first)
make api-down    # stop it

# Observability (Milestone 7):
make monitoring-up     # Prometheus + redis-exporter + cadvisor + Grafana
make monitoring-down   # stop them
make drift-report      # Evidently AI report: training data vs. real logged /predict requests
```

Every command also runs directly, with overridable paths:

```bash
fraud-detection ingest --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection validate --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection preprocess --raw-path data/raw/PS_20174660362_1_log.csv --output-dir data/processed
fraud-detection train --tracking-uri file:./mlruns --experiment-name paysim-fraud-detection
fraud-detection evaluate --model-uri models:/fraud-detection-classifier/1
fraud-detection producer --topic transactions --rate 5 --limit 1000
fraud-detection consumer --topic transactions --group-id fraud-detection-consumer
fraud-detection feast-apply --repo-path feast_repo
fraud-detection materialize --sample-size 5000
fraud-detection flink-worker --topic transactions --bounded
fraud-detection api --host 0.0.0.0 --port 8000 --registry-name fraud-detection-classifier
fraud-detection ready --host localhost --port 8000
fraud-detection drift-report --reference-sample-size 5000 --output-path docs/drift_report.html
```

## Configuration

Configuration is split into `configs/base.yaml` (shared defaults) plus an
environment overlay (`configs/dev.yaml`, `configs/prod.yaml`) that's
deep-merged on top. Loaded via:

```python
from fraud_detection.common.config import load_config

config = load_config("dev")  # or load_config() to use APP_ENV, defaulting to "dev"
```

## Logging

Structured JSON logging is configured in `configs/logging.yaml` and
initialized via `fraud_detection.common.logger`:

```python
from fraud_detection.common.logger import get_logger

logger = get_logger(__name__)
```

## Model training & MLflow

`fraud-detection train` trains Logistic Regression, Random Forest,
XGBoost, and LightGBM on `data/processed/` (already feature-engineered
by `fraud-detection preprocess`), handles the ~0.13% fraud imbalance via
class weighting, and logs every run to a local MLflow file store
(`mlruns/`, gitignored) — params, metrics, the model artifact,
`feature_version` (from `fraud_detection.features.registry`), and the
git commit hash. The run with the best **validation** `average_precision`
(PR-AUC — the meaningful metric here, since accuracy is ~99.87% even for
a model that never predicts fraud) is registered under
`fraud-detection-classifier` in the MLflow Model Registry. Browse runs
with:

```bash
mlflow ui --backend-store-uri file:./mlruns
```

## Kafka streaming

`docker-compose.yml` runs a single-node Kafka broker in KRaft mode
(no ZooKeeper) plus [Kafka UI](https://github.com/provectus/kafka-ui).
Bring it up first:

```bash
make kafka-up
# or: docker compose up -d kafka kafka-ui
```

Wait for it to report healthy, then check
[localhost:8080](http://localhost:8080) — the Kafka UI should show
cluster `local`, status `online`, 1 broker.

```bash
docker compose ps kafka   # should show "healthy" after ~15-30s
```

Then, in two terminals:

```bash
# Terminal 1 — consume (starts first so it doesn't miss anything)
fraud-detection consumer --topic transactions

# Terminal 2 — produce
fraud-detection producer --topic transactions --rate 5 --limit 200
```

You should see `received transaction` log lines in terminal 1 as
terminal 2 streams. `producer` reads the raw PaySim CSV, drops the
`isFraud` label (not available in a real-time stream), converts each
row to the shared `fraud_detection.domain.entities.Transaction`, and
publishes it as JSON; `consumer` deserializes with the exact same
function and just logs — no feature engineering or inference yet. See
ADR-0005 for why there's no separate Kafka-specific schema.

**Two single-node Kafka gotchas already fixed in `docker-compose.yml`**
(worth knowing if you ever hand-roll a KRaft compose file):

1. `CLUSTER_ID` must be a base64-encoded UUID, not an arbitrary string
   — KRaft will refuse to start otherwise.
2. `offsets.topic.replication.factor` defaults to 3, which a single
   broker can never satisfy; `__consumer_offsets` then never gets
   created and **every** consumer group fails
   `FindCoordinator` forever with `COORDINATOR_NOT_AVAILABLE` — with no
   error surfaced to a naive consumer loop, so it just hangs silently.
   Fixed by setting the replication factor to 1 for a single-node dev
   cluster (see the comments in `docker-compose.yml`).

## Real-time feature platform (Feast + Redis + Flink)

```
  fraud-detection producer
          |
          v
    Kafka: transactions
          |
          v
  fraud-detection flink-worker            <- real PyFlink (local-execution mode)
    deserialize_transaction()                same serializer.py Milestone 4 uses
          |
          v
    FeaturePipeline.transform_one()          same feature code training uses (ADR-0003)
          |
          v
    FeastFeatureStore.write_online()         Feast's push API
          |
          v
        Redis                                Feast's online store
          |
          v
    get_online_features()  <-------------    what a serving API (Milestone 6) will call
```

There's a second, offline path for bulk/dev-scale materialization
(`fraud-detection materialize`): PaySim CSV sample ->
`FeaturePipeline.transform()` -> `feast_prep.build_offline_source()`
(writes `data/feast/transaction_features.parquet`, Feast's registered
`FileSource`) -> `feast materialize` -> Redis. Both paths write the
same features through the same `FeaturePipeline`; only how they reach
Feast differs (batch file vs. streaming push). See ADR-0006 for the
full reasoning, including why local-execution PyFlink was used instead
of a separate Flink cluster.

### Setup (one-time)

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"   # add to your shell profile
export PATH="$JAVA_HOME/bin:$PATH"

make flink-jar   # downloads the Flink<->Kafka connector JAR (not a pip package)
```

### Running it

```bash
make infra-up       # Kafka + Kafka UI + Redis
make feast-apply    # register the `transaction` entity + `transaction_features` view
make materialize    # build data/feast/transaction_features.parquet, push into Redis

# Terminal 1 — the streaming worker (real PyFlink)
make flink-worker

# Terminal 2 — produce transactions
fraud-detection producer --topic transactions --rate 5 --limit 200
```

Terminal 1 prints `OK entity_id=... name_orig=...` for each transaction
as PyFlink computes its features and pushes them to Redis. Verify a
lookup directly:

```python
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH

store = FeastFeatureStore(DEFAULT_FEAST_REPO_PATH, DEFAULT_OFFLINE_SOURCE_PATH)
store.read_online("<entity_id from the flink-worker log line>")
```

`fraud-detection flink-worker --bounded` (also what the test suite
uses) stops at whatever Kafka offset was latest when the job started,
instead of running forever — useful for one-off verification.

### Testing

`tests/features/test_feast_store.py`, `tests/streaming/test_flink_job.py`,
and the `feast-apply`/`materialize`/`flink-worker` tests in
`tests/test_cli.py` talk to real Redis (and, for the Flink ones, real
Kafka + a JVM + the connector JAR) and skip automatically — not fail —
if any of those aren't available:

```bash
make infra-up
make flink-jar
make test   # Feast/Flink tests run for real; skip cleanly otherwise
```

CI does not provision Kafka/Redis/Java, so these skip there — mypy/
ruff/black still run against every module regardless (see ADR-0006).

## Real-time inference API (FastAPI + Feast + MLflow)

```
      Transaction (HTTP POST /predict)
              |
              v
    entity_key.compute_entity_id()        same derived id Milestone 5 uses
              |
              v
    FeastFeatureStore.read_online()       the 9 engineered features — never recomputed
              |
              v
    raw fields (from the request) + one-hot `type` (preprocess()) + engineered features
              |
              v
    reindex to the exact column order MLflow logged for the Production run
              |
              v
    model.predict() / predict_proba()     the MLflow "Production"-stage model, loaded once
              |
              v
    { prediction, fraud_probability, model_version, latency_ms }
```

If a transaction hasn't already flowed through the Milestone 5
`producer -> Kafka -> flink-worker -> Feast` pipeline, `/predict`
returns a `503` (not a silently recomputed answer) — see ADR-0007 for
why. Business logic lives entirely in `api.prediction_service.PredictionService`,
never in the routes.

### Endpoints

| Method | Path       | Purpose                                                        |
|--------|------------|-----------------------------------------------------------------|
| GET    | `/health`  | Liveness — process is up. Never checks dependencies.            |
| GET    | `/ready`   | Readiness — model loaded, Feast client constructed, Redis actually reachable (each checked independently). |
| POST   | `/predict` | Score a transaction (PaySim/Kafka field names — see `/docs` for the schema). |
| GET    | `/docs`    | Interactive OpenAPI docs (FastAPI's default).                   |

### Setup (one-time): promote a model to "Production"

`/predict` only ever serves the MLflow version currently in the
**Production** stage — `fraud-detection train` registers a model but
deliberately does not auto-promote it (a human/CI gate should decide
that; see ADR-0007). Promote the version you want to serve:

```python
from mlflow.tracking import MlflowClient

MlflowClient().transition_model_version_stage(
    name="fraud-detection-classifier", version=1, stage="Production"
)
```

### Running it locally

```bash
make infra-up       # Kafka + Kafka UI + Redis
make feast-apply
make materialize    # or run flink-worker against a live producer — see above

make api            # localhost:8000
# in another terminal:
make ready           # or: curl localhost:8000/ready
curl -X POST localhost:8000/predict -H "Content-Type: application/json" -d '{
  "step": 1, "type": "TRANSFER", "amount": 181.0, "nameOrig": "C1231006815",
  "oldbalanceOrg": 181.0, "newbalanceOrig": 0.0, "nameDest": "C1666544295",
  "oldbalanceDest": 0.0, "newbalanceDest": 0.0
}'
```

### Running it via Docker

```bash
make infra-up
make feast-apply     # writes feast_repo/registry.db, bind-mounted into the container
# promote a model to Production (see above) before starting the container

make api-build
make api-up          # localhost:8000
```

The `api` container mounts the host's `mlruns/` (at the identical
absolute path — MLflow's local file store bakes that path into each
run's metadata) and `feast_repo/`, and swaps in
`docker/feature_store.docker.yaml` (Redis reached as `redis:6379`, the
compose service name, instead of `localhost:6379`) — see ADR-0007,
which also documents four real bugs this only surfaced once actually
built and run (missing C toolchain, the MLflow absolute-path mount, a
`registered_model_meta` write during model *load*, and a `/ready`
check that had its own hardcoded Redis address instead of sharing
Feast's). Verified end to end: `docker ps` shows the container's own
`HEALTHCHECK` reporting `healthy`, `/ready` returns all three checks
`true`, and a real `/predict` call against the first PaySim row
returned `200` with the expected low fraud probability.

### Testing

```bash
make test   # tests/api/: unit (no infra), API (TestClient + dependency_overrides,
             # no infra), integration (real MLflow + Feast + Redis, skips cleanly
             # without Redis — same convention as tests/features/test_feast_store.py)
```

## Observability (Prometheus + Grafana + Evidently AI)

```
    Kafka -> flink-worker -> Feast -> api (/predict)
                                        |
                    +-------------------+-------------------+
                    |                                        |
                    v                                        v
              /metrics (Prometheus format)          monitoring/prediction_log.py
                    |                                (real logged requests)
                    v                                        |
               Prometheus  ---> Grafana                       v
                    ^                              fraud-detection drift-report
                    |                                        |
           redis-exporter, cadvisor                          v
        (Redis stats, container CPU/mem)          docs/drift_report.html (Evidently AI)
```

Every real `/predict` request updates the six metrics `api/routers.py`
records (`prediction_requests_total`, `prediction_latency_seconds`,
`prediction_errors_total`, `model_predictions_total`,
`model_fraud_probability`, `redis_connection_status`) and appends its
raw fields + result to `monitoring/prediction_log.py`'s log — the same
log `drift-report` later compares against the training distribution.

### Metrics & dashboard

```bash
make infra-up          # Kafka + Redis
make api-up            # or `make api` to run it on the host instead
make monitoring-up     # Prometheus (localhost:9090) + redis-exporter + cadvisor + Grafana (localhost:3000)
```

Grafana (`admin` / `admin`, set in `docker-compose.yml`) comes up with
the "Fraud Detection Platform" dashboard already loaded — no manual
datasource or dashboard import — via
`docker/grafana/provisioning/`+`docker/grafana/dashboards/`. Panels:
API Health (requests/sec, response time p50/p95/p99, error rate),
Model Usage (fraud predictions/hour, average fraud probability,
prediction outcome distribution), Infrastructure (container CPU/memory
via cadvisor, Redis status via both `redis-exporter`'s `redis_up` and
the API's own `redis_connection_status`). No Kafka broker panel — see
ADR-0008 for why that's a deliberate scope cut, not an oversight.

**Known limitation, found by actually running this**: on Docker
Desktop for Mac, `cadvisor` can't resolve individual containers'
CPU/memory (a documented cAdvisor/Docker-Desktop incompatibility, not
a config bug — see ADR-0008), so the CPU/Memory panels will be empty
there. Everything else (`redis-exporter`, the API's own metrics,
Prometheus scraping, the Grafana dashboard itself) was verified
working end to end, including on Docker Desktop for Mac. `cadvisor`
should work as intended on a native Linux Docker host.

Send some traffic and watch it show up (needs a Production model and
Feast populated — see the "Real-time inference API" section above):

```bash
for i in $(seq 1 20); do
  curl -s -X POST localhost:8000/predict -H "Content-Type: application/json" -d '{
    "step": 1, "type": "TRANSFER", "amount": 181.0, "nameOrig": "C1231006815",
    "oldbalanceOrg": 181.0, "newbalanceOrig": 0.0, "nameDest": "C1666544295",
    "oldbalanceDest": 0.0, "newbalanceDest": 0.0
  }' > /dev/null
done
open http://localhost:3000   # or curl localhost:9090 for raw Prometheus
```

### Data drift (Evidently AI)

```bash
make drift-report   # or: fraud-detection drift-report
open docs/drift_report.html
```

Compares a sample of the raw PaySim training data against
`monitoring/prediction_log.py`'s real log of what `/predict` has
actually been asked to score, on `amount`/`type`/`oldbalanceOrg`/`newbalanceOrig`.
Fails cleanly (exit 1, no report written) if nothing has been logged
yet — send some `/predict` requests first.

### Not implemented: model performance monitoring (precision/recall/AP)

This architecture has no ground-truth feedback loop — nothing ever
tells the system whether a served prediction was actually correct — so
there's nothing honest to compute precision/recall/false-positive-rate
from yet. `model_fraud_probability` and the drift report are the real,
label-free subset of "is the model behaving normally" available today.
See ADR-0008.

## Other local infrastructure

`docker-compose.yml` also defines a standalone MLflow tracking server
unused by this compose stack specifically — the `api` service here
talks to the local `mlruns/` file store instead (see ADR-0007). It's
the Kubernetes deployment (below) that actually serves it. Bring the
Compose one up if needed: `docker compose up mlflow`.

## Kubernetes deployment

`kubernetes/` deploys the same system to a real local `kind` cluster —
`redis`, `kafka`, a real MLflow tracking server (`api`/`training-job`
point at it over HTTP, since K8s pods can't share a host filesystem
the way Compose's bind mount does), `api` (3 replicas, HPA-scaled
2-10 on CPU, reachable via an nginx `Ingress`), `flink-worker`, and
`prometheus`/`grafana`/`redis-exporter` mirroring the Compose
monitoring stack. See ADR-0009 for what actually broke running this
for real and how it was fixed.

One-time setup:

```bash
# Build the images kubernetes/*.yaml reference
docker build -f docker/Dockerfile.api -t mloops-api:latest .
docker build -f docker/Dockerfile.worker -t mloops-worker:latest .
docker build -f docker/Dockerfile.flink-worker -t mloops-flink-worker:latest .

# Create the cluster (run from the repo root — kind-cluster.yaml's
# extraMounts path is relative to the current working directory)
kind create cluster --config kubernetes/kind-cluster.yaml --name mloops

# Load the images into it — kind's own containerd doesn't see the
# host's Docker image store otherwise
kind load docker-image mloops-api:latest mloops-worker:latest mloops-flink-worker:latest --name mloops

# ingress-nginx and metrics-server aren't installed by kind by
# default; hpa.yaml/ingress.yaml need them
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

Deploy everything:

```bash
kubectl apply -f kubernetes/
```

`training-job` needs a model registered *and* promoted to
"Production" before `api` can serve real predictions (same one-time
step as Docker Compose's, see above, run against this cluster's MLflow
instead):

```python
from mlflow.tracking import MlflowClient
MlflowClient(tracking_uri="http://localhost:5000").transition_model_version_stage(
    name="fraud-detection-classifier", version=1, stage="Production"
)
# (port-forward first: kubectl port-forward -n fraud-detection svc/mlflow 5000:5000)
```

Then reach the API via the ingress controller's mapped port (see
`kind-cluster.yaml`'s `extraPortMappings`): `curl http://localhost:8090/health`.

## Airflow orchestration

Three DAGs (`airflow/dags/`) orchestrate this project's own CLI on a
schedule, each running `mloops-worker:latest` as a short-lived sibling
container via `DockerOperator` rather than reimplementing any pipeline
logic in Airflow itself:

| DAG | Schedule | What it does |
|-----|----------|---------------|
| `daily_feature_materialization` | `@daily` | extract -> validate -> preprocess -> materialize Feast |
| `weekly_retraining` | `@weekly` | preprocess -> train -> evaluate |
| `monthly_drift_report` | `@monthly` | check predictions collected -> Evidently drift report |

Runs as its own Docker Compose stack, not pip-installed into this
project's venv:

```bash
cd airflow
docker compose up airflow-init   # one-time: DB schema + admin user (admin/admin)
docker compose up -d
```

Needs the main stack's network to already exist (`docker compose up`
from the repo root at least once), and `mloops-worker:latest` built
(see the Kubernetes section above). Airflow UI: `http://localhost:8085`
(DAGs are paused on creation — unpause before triggering). Both
`weekly_retraining` and `daily_feature_materialization` run against a
small sample CSV rather than the full ~6.4M-row PaySim file, and cap
their containers at 1.5GB — see ADR-0009 for exactly why (the full
file threatens the whole Docker Desktop VM this stack and `kind` share,
not just the one task).

## Live demo dashboard

`dashboard/app.py` (`streamlit run dashboard/app.py`) streams real
PaySim transactions through the actual Kubernetes pipeline (Kafka ->
`flink-worker` -> Feast/Redis), scores each one via the deployed API's
`/predict`, and shows the prediction next to PaySim's own ground-truth
`isFraud` label plus running accuracy/precision/recall/F1 and a
confusion matrix — see `dashboard/README.md` for setup (needs a Kafka
port-forward) and ADR-0009 for two real bugs this surfaced.

## Roadmap

- **Milestone 1 (done):** Foundation — scaffold, tooling, config,
  logging, domain layer.
- **Milestone 2 (done):** Data pipeline & EDA.
- **Milestone 3 (done):** Feature engineering pipeline, model training
  and comparison, MLflow tracking and registry.
- **Milestone 4 (done):** Kafka streaming foundation — producer,
  consumer, shared domain-entity schema. No inference in the stream
  yet.
- **Milestone 5 (done):** Real-time feature platform — Feast, Redis,
  a real PyFlink streaming job computing features via the same
  `FeaturePipeline` training uses. No model inference in the stream
  yet.
- **Milestone 6 (done):** Real-time inference API — FastAPI, Feast
  online features (never recomputed) + an MLflow Production-stage
  model, `/health`/`/ready`/`/predict`, Docker.
- **Milestone 7 (done):** Observability — Prometheus metrics
  (`/metrics`), an auto-provisioned Grafana dashboard, `redis-exporter`
  + `cadvisor` for infrastructure metrics, and Evidently AI data-drift
  reports comparing training data against a real log of served
  predictions. No Kafka broker metrics or ground-truth-based model
  performance metrics (precision/recall/AP) — see ADR-0008.
- **Milestone 8 (done):** A real Kubernetes deployment (`kubernetes/`,
  a local `kind` cluster), three Airflow DAGs orchestrating this
  project's own CLI via `DockerOperator`, and an expanded CI (Docker
  image builds, `kubeconform` manifest validation, Airflow DAG import
  checks). Stress testing, also named in the milestone brief, is not
  done — see ADR-0009 for the full scope and every real bug found
  running each of these for real.

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
