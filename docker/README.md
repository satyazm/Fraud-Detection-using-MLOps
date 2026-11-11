# docker/

The local stack (`kafka`, `kafka-ui`, `redis`, `mlflow`, `prometheus`, `redis-exporter`, `cadvisor`, `grafana`, `api`) is defined in [`docker-compose.yml`](../docker-compose.yml) at the repo root.

| File | Purpose |
|---|---|
| `Dockerfile.api` | FastAPI inference service image |
| `Dockerfile.worker` | Generic CLI image (train, evaluate, producer, materialize, drift-report, …) used by Kubernetes Jobs and Airflow `DockerOperator` tasks |
| `Dockerfile.flink-worker` | PyFlink streaming worker; the only image with a JDK and the Kafka connector JAR |
| `feature_store.docker.yaml` | `feast_repo/feature_store.yaml` with Redis at `redis:6379` (the Compose service name) instead of `localhost:6379`; bind-mounted over the real file for containers |
| `prometheus.yml` | Prometheus scrape config (API, redis-exporter, cAdvisor) |
| `grafana/` | Auto-provisioned datasource and the "Fraud Detection Platform" dashboard |

See the [inference API](../docs/guides/inference-api.md) and [observability](../docs/guides/observability.md) guides.
