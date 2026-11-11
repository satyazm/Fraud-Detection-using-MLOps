# Real-time inference API (FastAPI + Feast + MLflow)

`POST /predict` derives the transaction's Feast entity ID, reads its
nine engineered features from Redis, combines them with raw request
fields, and orders the columns exactly as recorded for the MLflow model.
The API loads the model in MLflow's **Production** stage once at
startup. If the transaction's features are absent, it returns 503
instead of recomputing them. See [ADR-0007](../decisions/0007-fastapi-inference-service.md).

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Process liveness |
| GET | `/ready` | Model, Feast client, and Redis readiness |
| POST | `/predict` | Fraud class and probability for a transaction |
| GET | `/metrics` | Prometheus metrics |
| GET | `/docs` | Interactive OpenAPI documentation |

## Promote a model once

`fraud-detection train` registers its best model but does not promote
it. Promote the version to serve, then start or restart the API:

```python
from mlflow.tracking import MlflowClient

MlflowClient().transition_model_version_stage(
    name="fraud-detection-classifier", version=1, stage="Production"
)
```

## Run locally

```bash
make infra-up
make feast-apply
make materialize    # or populate Feast through Kafka and flink-worker
make api            # http://localhost:8000
```

In another terminal:

```bash
make ready
curl -X POST localhost:8000/predict -H "Content-Type: application/json" -d '{
  "step": 1, "type": "TRANSFER", "amount": 181.0, "nameOrig": "C1231006815",
  "oldbalanceOrg": 181.0, "newbalanceOrig": 0.0, "nameDest": "C1666544295",
  "oldbalanceDest": 0.0, "newbalanceDest": 0.0
}'
```

## Run with Docker Compose

```bash
make infra-up
make feast-apply
make materialize
make api-build
make api-up
```

Promote a model before starting the container. The Compose API uses the
host's local `mlruns/` store and mounts `feast_repo/`; its Feast config
points to `redis:6379` on the Compose network. See
[ADR-0007](../decisions/0007-fastapi-inference-service.md) for the
container path and Redis configuration details. `make test` covers
unit tests plus an API integration test that skips without Redis.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
