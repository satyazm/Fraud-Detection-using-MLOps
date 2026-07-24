# docker/

The local dev stack (`kafka`, `redis`, `mlflow`, `prometheus`, `grafana`)
is defined in `docker-compose.yml` at the repo root.

This directory holds per-service build assets that don't exist yet:
`Dockerfile.api` (serving), `Dockerfile.consumer` (streaming),
`prometheus.yml` (scrape config). Added as the services they belong to
are implemented.
