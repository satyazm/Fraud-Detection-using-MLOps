# Real-Time Fraud Detection MLOps Platform

A production-grade MLOps platform for real-time fraud detection, built on
the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) synthetic
mobile-money transaction dataset.

This repository is being built in milestones. **Milestones 1-5 are
done**: project scaffold, a shared feature-engineering pipeline, the
data pipeline, model training/comparison with MLflow tracking and
registry, Kafka streaming, and a real-time feature platform (Feast +
Redis + a real PyFlink streaming job). No model inference or FastAPI
yet — that's Milestone 6.

## Project layout

```
configs/             YAML configuration: base.yaml + {env}.yaml overlays, logging.yaml
data/
  raw/                Original, immutable source data (gitignored contents)
  processed/          train/validation/test parquet splits, features included (gitignored)
  sample/             Small samples for tests/local dev (gitignored contents)
  contracts/          Versioned wire-format contracts (e.g. transaction_schema.json)
  feast/              Feast's offline FileSource parquet (gitignored, generated)
feast_repo/           Feast feature repo: feature_store.yaml + definitions.py
src/fraud_detection/  Installable package (see Package layout below)
tests/                Pytest test suite (mirrors src/fraud_detection layout)
docs/
  architecture.md      System architecture, layering, target data flow
  decisions/            Architecture Decision Records (ADRs)
  data_report.md         Generated PaySim data quality report
  model_report.md         Generated model comparison report
  images/                  Generated plots referenced by the reports above
mlruns/               MLflow tracking store (gitignored, local-only)
.flink-jars/          Flink<->Kafka connector JAR (gitignored, downloaded by `make flink-jar`)
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
- Docker + Docker Compose, running, before using `producer`/`consumer`/
  `materialize`/`flink-worker` or their tests (they need real Kafka at
  `localhost:9092` and/or Redis at `localhost:6379`).
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
fraud-detection feast-apply --repo-path feast_repo
fraud-detection materialize --sample-size 5000
fraud-detection flink-worker --topic transactions --bounded
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

## Other local infrastructure

`docker-compose.yml` also defines MLflow, Prometheus, and Grafana for
later milestones — nothing in the codebase talks to them yet. Bring
them up as needed, e.g. `docker compose up mlflow`.

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
- **Milestone 6:** Online inference (Kafka/Feast -> XGBoost -> fraud
  probability), FastAPI serving API, Docker.
- **Milestone 7:** Monitoring — Prometheus, Grafana, Evidently AI.
- **Milestone 8:** Airflow, CI/CD, Kubernetes, deployment, stress testing.

See `docs/architecture.md` and `docs/decisions/` for the reasoning
behind these choices.
