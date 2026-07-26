# docker/

The local dev stack (`kafka`, `redis`, `mlflow`, `prometheus`, `grafana`,
`api`) is defined in `docker-compose.yml` at the repo root.

- `Dockerfile.api` (Milestone 6) — builds the FastAPI inference
  service's image; see the `api` service in `docker-compose.yml` and
  the "Real-time inference API" section of the root README.
- `feature_store.docker.yaml` — `feast_repo/feature_store.yaml` with
  the Redis `connection_string` swapped from `localhost:6379` (correct
  for host-side `flink-worker`/`materialize`/tests) to `redis:6379`
  (the compose network's service name); bind-mounted over the real
  file for the `api` service only.

Still to add, when the services they belong to are implemented:
`Dockerfile.consumer` (streaming), `prometheus.yml` (scrape config).
