# Getting started

## Requirements

- Python 3.11
- The PaySim CSV (`PS_20174660362_1_log.csv` from
  [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)) placed at
  `data/raw/PS_20174660362_1_log.csv` before running the data pipeline.
- Docker + Docker Compose, running, before using `producer`/`consumer`/
  `materialize`/`flink-worker`/`api` or their tests (they need real
  Kafka at `localhost:9092` and/or Redis at `localhost:6379`).
- A JDK (11, 17, or 21) for PyFlink, e.g. `brew install openjdk@17`,
  with `JAVA_HOME` set — see the [Streaming & features guide](streaming-and-features.md).

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate

make install-dev   # installs dev deps, the package (editable), and git pre-commit hooks
```

Use the Makefile install targets: PyFlink requires `setuptools<81`
and `--no-build-isolation`, which they handle.

## Common tasks

| Area | Commands |
|------|----------|
| Code quality | `make lint`, `make format`, `make typecheck`, `make test`, `make precommit` |
| Data preparation | `make ingest`, `make validate`, `make preprocess` |
| Models | `make train`, `make evaluate` |
| Kafka | `make kafka-up`, `make producer`, `make consumer`, `make kafka-down` |
| Features | `make redis-up`, `make infra-up`, `make flink-jar`, `make feast-apply`, `make materialize`, `make flink-worker`, `make infra-down` |
| API | `make api`, `make ready`, `make api-build`, `make api-up`, `make api-down` |
| Monitoring | `make monitoring-up`, `make monitoring-down`, `make drift-report` |

The Makefile wraps the CLI, Docker Compose, and code quality tools.
Run `fraud-detection --help` or
`fraud-detection <command> --help` to see overridable paths, topics,
ports, and other options. For example:

```bash
fraud-detection preprocess --raw-path data/raw/PS_20174660362_1_log.csv --output-dir data/processed
fraud-detection train --tracking-uri file:./mlruns --experiment-name paysim-fraud-detection
fraud-detection producer --topic transactions --rate 5 --limit 1000
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

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
