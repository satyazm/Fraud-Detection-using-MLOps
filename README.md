# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in milestones. **Milestones 1-3 are
done**: project scaffold, a shared feature-engineering pipeline, the
data pipeline, and model training/comparison with MLflow tracking and
registry. No Kafka, FastAPI, or streaming yet.

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
  features/      One feature pipeline shared by training, inference, and future streaming (ADR-0003)
  models/        Training, comparison, evaluation, MLflow tracking/registry
  streaming/     Real-time streaming, e.g. Kafka (Milestone 4)
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

# Placeholder operational commands (real logic lands in later milestones):
make producer
make consumer
make api
```

Every command also runs directly, with overridable paths:

```bash
fraud-detection ingest --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection validate --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection preprocess --raw-path data/raw/PS_20174660362_1_log.csv --output-dir data/processed
fraud-detection train --tracking-uri file:./mlruns --experiment-name paysim-fraud-detection
fraud-detection evaluate --model-uri models:/fraud-detection-classifier/1
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

## Local infrastructure

`docker-compose.yml` defines the target local dev stack (Kafka, Redis,
MLflow, Prometheus, Grafana). Nothing in the codebase talks to these
services yet — bring services up individually as later milestones need
them, e.g. `docker compose up kafka redis mlflow`.

## Roadmap

- **Milestone 1 (done):** Foundation — scaffold, tooling, config,
  logging, domain layer.
- **Milestone 2 (done):** Data pipeline & EDA.
- **Milestone 3 (done):** Feature engineering pipeline, model training
  and comparison, MLflow tracking and registry.
- **Milestone 4:** Kafka producer/consumer, transaction simulator.
- **Milestone 5:** Flink streaming features, Feast, Redis.
- **Milestone 6:** FastAPI inference API, Docker.
- **Milestone 7:** Monitoring — Prometheus, Grafana, Evidently AI.
- **Milestone 8:** Airflow, CI/CD, Kubernetes, deployment, stress testing.

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
