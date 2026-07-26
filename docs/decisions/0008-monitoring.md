# 8. Observability: Prometheus, Grafana, Evidently AI, and what's deliberately not there

Date: 2026-07-26

## Status

Accepted

## Context

Milestones 1-6 built a working real-time fraud detection system; nothing
in it says whether it's *healthy*. Milestone 7's job is observability:
can an operator tell, without reading logs by hand, whether the API is
serving traffic normally, whether the model's predictions look sane,
and whether the data flowing through it still resembles what the model
was trained on.

## Decisions

### The four named metrics, plus two more

`prediction_requests_total{outcome}`, `prediction_latency_seconds`,
`prediction_errors_total{reason}`, and `model_predictions_total{prediction}`
are exactly the names/shapes requested. Two more were added because the
spec's own dashboard requirements ("average fraud probability,"
"prediction distribution") have no metric to read them from otherwise:
`model_fraud_probability` (a Histogram, so both the average and the
distribution come from the same series) and `redis_connection_status`
(a Gauge — "Redis connection status" was explicitly listed as an API
metric to expose, not left to `/ready` alone). All six live in
`monitoring/metrics.py`, recorded from `api/routers.py`'s `/predict`
and `/metrics` handlers — the metric *definitions* are the one place
that knows `prometheus_client`'s API, same as `features/registry.py`
is the one place feature definitions live.

### `/metrics` is a plain route, not `prometheus_client.make_asgi_app()`

`prometheus_client` ships a ready-made ASGI app for exactly this, but
mounting it can't easily reach `Depends(get_app_state)` — and
`redis_connection_status` needs *this specific app instance's* Redis
address, re-checked live on every scrape. A `Gauge.set_function()`
callback (the other obvious approach) is a single closure shared by
the whole process, which breaks the moment more than one app instance
exists — exactly what this project's own test suite does (several
`create_app()` calls per test run). Recording it inline in a normal
`@router.get("/metrics")` handler, using the same `Depends(get_app_state)`
every other route already uses, sidesteps both problems: correct per
app instance, live per scrape, no new FastAPI-mounting pattern to learn.

### Drift compares training data to a *real* log of `/predict` requests, not synthetic data

Evidently needs "reference" and "current" data. Reference is a sample
of the raw PaySim CSV (the training distribution). Current data — the
"live production data" the milestone brief asks for — comes from
`monitoring/prediction_log.py`, which appends every real `/predict`
request's raw fields (plus the prediction and probability) to a JSONL
file. This was a deliberate choice over a synthetic stand-in: a drift
report only means something if "current" is what the service actually
saw.

**A real bug this design caught on itself**: the first version had
`PredictionService`'s log path default to the real
`monitoring.prediction_log.DEFAULT_LOG_PATH`. Running the test suite
silently wrote fake test predictions (a stubbed 0.99-probability
response, a `sample_transactions_df` fixture row) into the file meant
to hold genuine live traffic — confirmed by inspecting it after a test
run. Fixed by making `prediction_log_path` a required constructor
argument with no default: every call site (the real `api/app.py`, and
five test call sites) must now say explicitly where predictions get
logged, so a forgotten override is a `TypeError` at construction time,
not a silent data-quality bug discovered later. `api/app.py`'s
`create_app`/`load_app_state` are the only places that default it to
the real path.

**A second real bug, found running the actual `api` container**: even
with that fixed, the containerized API's log lived only inside the
container's own writable layer (`/app/data/monitoring/`) — invisible
to a host-side `fraud-detection drift-report`, and gone on
`docker compose down`. Confirmed with `docker exec fraud-api cat
/app/data/monitoring/prediction_log.jsonl` (50 lines present) against
an empty host `data/monitoring/`. Fixed by bind-mounting
`./data/monitoring:/app/data/monitoring` in `docker-compose.yml`, the
same pattern already used for `mlruns/`/`feast_repo/` — verified again
afterward: 50 more requests, this time visible on the host immediately.

### Evidently's 0.7.x API, and a narrow, named column scope

Evidently rewrote its API between the commonly-documented 0.4.x
(`Report(metrics=[DataDriftPreset()])` over legacy `Dashboard`/`Tab`
objects) and 0.7.x (`Report` + `Dataset.from_pandas(df, data_definition=DataDefinition())`
+ `.run(reference_data=..., current_data=...)`). The pinned version is
0.7.21 (current at the time this was built); verified end to end
against both a real drift scenario (a 5x `amount` shift, correctly
flagged) and identical distributions (correctly reporting zero drift —
not just "it runs," but "it discriminates"). Scoped to exactly the four
columns the milestone named (`amount`, `type`, `oldbalanceOrg`,
`newbalanceOrig`) — all four are raw PaySim columns both the reference
CSV and the prediction log already carry under the same names, so there
is no schema translation between the two sources.

Run for real against the live `api` container (50 real PaySim rows sent
through `/predict`, then `fraud-detection drift-report` against a
5000-row reference): reported `drift_share: 1.0` (all 4 columns
flagged) — surprising at first, until confirmed *not* a bug: PaySim's
raw CSV is ordered by simulation `step` (time), so the first 50 rows
(what got sent) really do differ systematically from a 5000-row sample
spanning much more of the simulated timeline. Verified by re-running
with the reference narrowed to the identical 50 rows the traffic came
from — `drift_share: 0.0`, exactly as it should be when "current" is a
subset of "reference." The 100%-drift result was a real finding about
the demo data, not a defect in the report.

### Included: `redis-exporter`, `cadvisor`. Excluded: Kafka metrics

Real Redis-level metrics (memory, ops/sec, connected clients — not
just "can the API reach it") and per-container CPU/memory for the
whole compose stack are both a single standard Docker image away
(`oliver006/redis_exporter`, `gcr.io/cadvisor/cadvisor`), so both are
in. Kafka broker metrics are not: Kafka has no Prometheus-format
endpoint of its own — getting one requires a JMX exporter running as a
Java agent inside Kafka's JVM (`KAFKA_OPTS` wiring, a separate MBean
config file), a meaningfully bigger and separately-riskier piece of
infrastructure than either of the other two for one dashboard panel.
Left out, not hidden: the Grafana dashboard and `docker/prometheus.yml`
both say so directly, and this is exactly the kind of scope cut this
project already documents rather than silently skips (see ADR-0006's
"CI stays infra-free" for the same instinct applied elsewhere).

**A real platform limitation, found running it**: on Docker Desktop for
Mac (this project's actual local dev environment), `cadvisor` cannot
resolve individual containers' CPU/memory — every
`container_cpu_usage_seconds_total{name=~"..."}` query matches zero
series, even with `/var/run/docker.sock` explicitly mounted (tried
first; didn't fix it). `docker logs fraud-cadvisor` shows the real
cause: `failed to identify the read-write layer ID for container
"...": open /rootfs/var/lib/docker/image/overlayfs/layerdb/mounts/.../mount-id:
no such file or directory` — cAdvisor's per-container introspection
expects a standard native-Linux Docker storage layout, which Docker
Desktop's virtualized VM doesn't present the same way. This is a known
cAdvisor-on-Docker-Desktop-for-Mac incompatibility, not a
misconfiguration on this project's side (`redis-exporter` and the
`fraud-detection-api`/`redis` Prometheus targets all work correctly in
the same environment — confirmed via Prometheus's own `/api/v1/targets`,
all reporting `health: up`). Kept in `docker-compose.yml` anyway
(would work on a native Linux Docker host, the more realistic
production target for this stack) with the CPU/Memory panels'
limitation called out directly in the README rather than presented as
verified when it wasn't.

### Not implemented: model performance monitoring (precision/recall/AP)

This architecture has no ground-truth feedback loop — nothing ever
tells the running system whether a served prediction was actually
right or wrong. Computing precision/recall/false-positive-rate without
labels would mean inventing numbers, not monitoring them. What *is*
real and implemented instead: prediction-distribution monitoring
(`model_fraud_probability`'s histogram, `model_predictions_total`'s
per-class counts, and the drift report), which is the honest subset of
"model performance monitoring" achievable without labels. If ground
truth becomes available later (e.g., confirmed fraud/chargeback data
joined back against `prediction_log`'s entries), computing
precision/recall/AP from that log is a contained follow-up — the log
already carries `prediction`/`fraud_probability`/`model_version` per
row, so nothing about its schema would need to change.

### Grafana: auto-provisioned, not click-through

Both the Prometheus datasource and the dashboard are provisioned via
files bind-mounted into the `grafana` container
(`docker/grafana/provisioning/`, `docker/grafana/dashboards/`) rather
than configured by hand in the UI after startup — `docker compose up`
alone reproduces the exact same dashboard every time, which matters
for "a dashboard you can demonstrate during interviews" specifically:
nothing to remember to re-click after a fresh `docker compose down -v`.

## Consequences

- `src/fraud_detection/monitoring/` is now real:
  `metrics.py` (Prometheus definitions + the shared `redis_reachable`
  live-check, replacing a duplicate that used to live in `api/routers.py`),
  `prediction_log.py` (the live-data log), `drift.py` (the Evidently
  report). `api/prediction_service.py` gained a required
  `prediction_log_path` argument; `api/app.py`/`fraud-detection api`
  gained a matching `--prediction-log-path` flag/parameter.
- New CLI command: `fraud-detection drift-report`.
- `docker-compose.yml` gained `redis-exporter` and `cadvisor`; the
  `prometheus` and `grafana` services went from scaffolding to
  actually configured (`docker/prometheus.yml`,
  `docker/grafana/provisioning/`, `docker/grafana/dashboards/`).
- `requirements/base.txt` gained `prometheus_client==0.24.1` (already
  present transitively, now pinned directly) and `evidently==0.7.21`
  (new, heavy, verified conflict-free against every existing pin via
  `pip check`).
