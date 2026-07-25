# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in milestones. **Milestones 1-4 are
done**: project scaffold, a shared feature-engineering pipeline, the
data pipeline, model training/comparison with MLflow tracking and
registry, and a Kafka streaming foundation (producer + consumer, no
inference in the stream yet). No FastAPI yet.

## Project layout

```
configs/             YAML configuration: base.yaml + {env}.yaml overlays, logging.yaml
data/
  raw/                Original, immutable source data (gitignored contents)
  processed/          train/validation/test parquet splits, features included (gitignored)
  sample/             Small samples for tests/local dev (gitignored contents)
  contracts/          Versioned wire-format contracts (e.g. transaction_schema.json)
src/fraud_detection/  Installable package (see Package layout below)
tests/                Pytest test suite (mirrors src/fraud_detection layout)
docs/
  architecture.md      System architecture, layering, target data flow
  decisions/            Architecture Decision Records (ADRs)
  data_report.md         Generated PaySim data quality report
  model_report.md         Generated model comparison report
  images/                  Generated plots referenced by the reports above
mlruns/               MLflow tracking store (gitignored, local-only)
scripts/              One-off operational/data scripts
docker/               Per-service Dockerfiles, Prometheus config (added as needed)
docker-compose.yml    Local dev stack: Kafka, Redis, MLflow, Prometheus, Grafana
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
  features/      One feature pipeline shared by training, inference, and streaming (ADR-0003)
  models/        Training, comparison, evaluation, MLflow tracking/registry
  streaming/     Kafka producer/consumer (ADR-0005); no inference in the stream yet
  serving/       Model serving / inference API (Milestone 6)
  monitoring/    Model & data observability (Milestone 7)
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
- Docker + Docker Compose, running, before using `producer`/`consumer`
  or their tests (they need a real Kafka broker at `localhost:9092`).

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate

make install-dev   # installs dev deps, the package (editable), and git pre-commit hooks
```

Copy `.env.example` to `.env` and fill in values as later milestones need
them (nothing reads `.env` yet).

## Common tasks

```bash
make lint        # ruff + black --check
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

# Kafka streaming (Milestone 4) — needs `make kafka-up` first:
make kafka-up    # start Kafka + Kafka UI (localhost:8080) via Docker Compose
make producer    # stream PaySim transactions onto the `transactions` topic
make consumer    # consume and log transactions from the `transactions` topic
make kafka-down  # stop them

# Placeholder operational commands (real logic lands in later milestones):
make api
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

### Testing

`tests/streaming/` and the producer/consumer tests in `tests/test_cli.py`
talk to a real broker and skip automatically (not fail) if
`localhost:9092` isn't reachable:

```bash
make kafka-up
make test   # streaming tests run for real; skip cleanly if Kafka is down
```

## Other local infrastructure

`docker-compose.yml` also defines Redis, MLflow, Prometheus, and
Grafana for later milestones — nothing in the codebase talks to them
yet. Bring individual services up as needed, e.g.
`docker compose up redis mlflow`.

## Roadmap

- **Milestone 1 (done):** Foundation — scaffold, tooling, config,
  logging, domain layer.
- **Milestone 2 (done):** Data pipeline & EDA.
- **Milestone 3 (done):** Feature engineering pipeline, model training
  and comparison, MLflow tracking and registry.
- **Milestone 4 (done):** Kafka streaming foundation — producer,
  consumer, shared domain-entity schema. No inference in the stream
  yet.
- **Milestone 5:** Online inference (Kafka -> features -> XGBoost ->
  fraud probability), then Flink streaming features, Feast, Redis.
- **Milestone 6:** FastAPI inference API, Docker.
- **Milestone 7:** Monitoring — Prometheus, Grafana, Evidently AI.
- **Milestone 8:** Airflow, CI/CD, Kubernetes, deployment, stress testing.

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
