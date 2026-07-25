# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in milestones. **Milestone 1 (Foundation)**
is done: project scaffold, tooling, config, logging, domain layer.
**Milestone 2 (Data Pipeline & EDA)** is in progress: ingestion,
validation, preprocessing, and stratified splitting for PaySim. No model
training yet.

## Project layout

```
configs/             YAML configuration: base.yaml + {env}.yaml overlays, logging.yaml
data/
  raw/                Original, immutable source data (gitignored contents)
  processed/          train/validation/test parquet splits (gitignored contents)
  sample/             Small samples for tests/local dev (gitignored contents)
  contracts/          Versioned wire-format contracts (e.g. transaction_schema.json)
src/fraud_detection/  Installable package (see Package layout below)
tests/                Pytest test suite (mirrors src/fraud_detection layout)
docs/
  architecture.md      System architecture, layering, target data flow
  decisions/            Architecture Decision Records (ADRs)
  data_report.md         Generated PaySim data quality report
  images/                 Generated plots referenced by data_report.md
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
  data/          Ingestion, validation, preprocessing, splitting (implemented)
  features/      Feature engineering (Milestone 3)
  models/        Training & inference (Milestone 3)
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
make preprocess  # clean, encode, stratified-split, save to data/processed/

# Placeholder operational commands (real logic lands in later milestones):
make train
make producer
make consumer
make api
```

Each data-pipeline command also runs directly, with overridable paths:

```bash
fraud-detection ingest --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection validate --raw-path data/raw/PS_20174660362_1_log.csv
fraud-detection preprocess --raw-path data/raw/PS_20174660362_1_log.csv --output-dir data/processed
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

## Local infrastructure

`docker-compose.yml` defines the target local dev stack (Kafka, Redis,
MLflow, Prometheus, Grafana). Nothing in the codebase talks to these
services yet — bring services up individually as later milestones need
them, e.g. `docker compose up kafka redis mlflow`.

## Roadmap

- **Milestone 1 (done):** Foundation — scaffold, tooling, config,
  logging, domain layer.
- **Milestone 2 (in progress):** Data pipeline & EDA.
- **Milestone 3:** Feature engineering, baseline XGBoost, MLflow, model
  registry.
- **Milestone 4:** Kafka producer/consumer, transaction simulator.
- **Milestone 5:** Flink streaming features, Feast, Redis.
- **Milestone 6:** FastAPI inference API, Docker.
- **Milestone 7:** Monitoring — Prometheus, Grafana, Evidently AI.
- **Milestone 8:** Airflow, CI/CD, Kubernetes, deployment, stress testing.

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
