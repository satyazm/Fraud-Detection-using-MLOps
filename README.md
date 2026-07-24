# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in phases. **Phase 1** (this state) only
establishes the enterprise-grade project scaffold, tooling, configuration,
and logging — no data processing or ML code has been implemented yet.

## Project layout

```
configs/             YAML configuration: base.yaml + {env}.yaml overlays, logging.yaml
data/
  raw/                Original, immutable source data (gitignored contents)
  processed/          Cleaned/transformed data (gitignored contents)
  sample/             Small samples for tests/local dev (gitignored contents)
src/fraud_detection/  Installable package (see Package layout below)
tests/                Pytest test suite (mirrors src/fraud_detection layout)
docs/
  architecture.md      System architecture, layering, target data flow
  decisions/           Architecture Decision Records (ADRs)
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
  common/        Config loading, logging (implemented)
  cli.py         `fraud-detection` command-line entry point (placeholders)
  data/          Data ingestion & validation (Phase 2+)
  features/      Feature engineering (Phase 2+)
  models/        Training & inference (Phase 2+)
  streaming/     Real-time streaming, e.g. Kafka (Phase 4+)
  serving/       Model serving / inference API (Phase 5+)
  monitoring/    Model & data observability (Phase 5+)
  utils/         Generic helpers
```

Importing is unambiguous and namespace-safe, e.g.:

```python
from fraud_detection.common.config import load_config
```

## Requirements

- Python 3.11

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate

make install-dev   # installs dev deps, the package (editable), and git pre-commit hooks
```

Copy `.env.example` to `.env` and fill in values as later phases need
them (nothing reads `.env` yet in Phase 1).

## Common tasks

```bash
make lint        # ruff + black --check
make format      # ruff --fix + black
make typecheck    # mypy
make test        # pytest with coverage
make precommit   # run all pre-commit hooks against the full repo

# Placeholder operational commands (real logic lands in later phases):
make train
make producer
make consumer
make api
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
services yet — bring services up individually as later phases need them,
e.g. `docker compose up kafka redis mlflow`.

## Roadmap

- **Phase 1 (current):** Project scaffold, tooling, config, logging.
- **Phase 2:** Data understanding (EDA), feature engineering, baseline
  model (XGBoost/LightGBM), MLflow experiment tracking and registry.
- **Phase 3+:** Streaming ingestion (Kafka), model serving (FastAPI),
  monitoring (Prometheus/Grafana), orchestration (Airflow), deployment
  (Kubernetes).

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
