# Observability (Prometheus + Grafana + Evidently AI)

`/metrics` exposes request counts, errors, latency, prediction counts,
fraud-probability distribution, and Redis reachability. Each successful
`/predict` also appends the input and result to
`data/monitoring/prediction_log.jsonl`. The prediction log supplies
live data for the Evidently drift report.

## Metrics and dashboard

```bash
make infra-up
make api-up            # or make api to run on the host
make monitoring-up     # Prometheus :9090 and Grafana :3000
```

Grafana (`admin` / `admin`, set in `docker-compose.yml`) provisions the
"Fraud Detection Platform" dashboard automatically. It shows API,
model-usage, Redis, and container metrics. On Docker Desktop for Mac,
cAdvisor may leave per-container CPU/memory panels empty; see
[ADR-0008](../decisions/0008-monitoring.md). Kafka broker metrics
are outside the current dashboard's scope.

## Data drift

```bash
make drift-report
open docs/drift_report.html
```

The report compares real logged requests with a PaySim reference
sample for `amount`, `type`, `oldbalanceOrg`, and `newbalanceOrig`.
It requires at least one successful `/predict` request. The platform
has no ground-truth feedback loop, so it cannot calculate live
precision, recall, or false-positive rate; probability and data drift
are the available label-free signals.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
