# 9. Deployment and operations: Kubernetes, Airflow, and CI/CD

Date: 2026-07-27

## Status

Accepted

## Context

Milestones 1-7 built a working, observable, real-time fraud detection
system entirely on Docker Compose, run and tested by hand. Milestone
8's job is operations: can the same system be deployed to something
that looks like a real cluster, retrained/materialized on a schedule
without a human running commands, and checked automatically before
code merges — and does any of that actually survive being run for
real, not just read as YAML.

## Decisions

### Kubernetes: a real local `kind` cluster, not manifests nobody applied

Every manifest in `kubernetes/` was applied to a real `kind` cluster
on this machine and exercised for real: `training-job` and
`producer-job` run to completion, `api` serves real `/predict`
traffic behind its Service, Prometheus/Grafana scrape real targets,
`redis-exporter` reports real Redis stats. `kind-cluster.yaml` is
kind's own cluster config (`kind create cluster --config`), not a
`kubectl apply`-able resource — kept in `kubernetes/` because it's the
one piece of local cluster setup specific to this project, but
excluded from manifest validation (see CI, below) for the same reason.
Two cluster-level prerequisites aren't installed by `kind` by default
and have to be added once per cluster: `ingress-nginx` (for
`ingress.yaml`) and `metrics-server` (for `hpa.yaml`'s CPU-based
scaling) — both called out directly in the affected manifests'
comments rather than assumed.

### MLflow on Kubernetes: `--serve-artifacts`, not `--default-artifact-root`

**A real bug, found running the actual training-job Job**: the MLflow
server was first configured with `--default-artifact-root
/mlflow/artifacts`. This bakes a plain local filesystem path into each
experiment's `artifact_location`; the MLflow *client* then tries to
write model artifacts directly to that path on its own filesystem —
fine for a host-side process, broken for a Kubernetes pod, which has
no `/mlflow` mounted. `training-job` failed with `PermissionError:
[Errno 13] Permission denied: '/mlflow'` on `mlflow.log_dict`,
confirmed from the pod's own logs. Fixed by switching to
`--serve-artifacts --artifacts-destination /mlflow/artifacts`, so the
tracking server proxies artifact reads/writes over its own HTTP API
instead of requiring direct filesystem access — the client only ever
talks to `http://mlflow:5000`. The two experiments already in the
sqlite backing store had the old, now-incompatible `artifact_location`
baked in from before the fix; since neither had a single successful
run recorded, the pragmatic fix was wiping `mlflow.db`/`artifacts/` on
the PVC rather than migrating them.

**A second real bug, found immediately after the first fix**: with
artifact proxying on, the server itself now does the work of moving
model bytes, and the original 512Mi memory limit wasn't enough — the
pod was **OOMKilled** (`exitCode 137`, confirmed via the container's
own `lastState`) as soon as a real training run tried to log a model.
Fixed by raising the limit to 1Gi and capping gunicorn to `--workers
2` (mlflow's default worker count is unrelated to the pod's CPU
limit, so the default was higher than needed here). Verified after
both fixes: a full `training-job` run (4 models, best one registered),
promoting the winning version to the `Production` stage, and a full
`api` deployment rollout picking it up cleanly (`/ready` reporting
`model_loaded: true` against the freshly restarted pods) — the same
manual promotion step ADR-0007 already established for Docker
Compose, just against this cluster's own MLflow server instead of a
local `mlruns/` file store.

### Airflow: its own Docker Compose stack, DockerOperator, not embedded in this repo's venv

Airflow runs as a separate `docker-compose.yml` (Postgres backing DB,
`LocalExecutor`, webserver + scheduler) rather than being pip-installed
into this project's own virtualenv — a large, opinionated dependency
tree with no business being importable by `fraud_detection` code.
Each of the three DAGs (`daily_feature_materialization`,
`weekly_retraining`, `monthly_drift_report`) uses `DockerOperator` to
launch the same `mloops-worker:latest` image Kubernetes uses
(`docker/Dockerfile.worker`) as short-lived sibling containers on the
host's Docker daemon ("Docker-outside-of-Docker," via the bind-mounted
`/var/run/docker.sock`) — Airflow orchestrates this project's existing
CLI, it doesn't reimplement any of it.

**A real bug, found bringing the stack up**: `airflow-scheduler`'s own
healthcheck (`curl localhost:8974/health`) never passed — the
container sat at `starting` indefinitely. `airflow config get-value
scheduler enable_health_check` returned `False`: the scheduler's
health-check HTTP server is off by default, so nothing was ever
listening on that port for the healthcheck to reach. Fixed with
`AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: "true"` in the compose
file's shared environment block.

**A third real bug, found triggering all three DAGs**: `weekly_retraining`
and `daily_feature_materialization` both pointed their `DockerOperator`
tasks at the full ~6.4M-row PaySim CSV. `kubernetes/jobs.yaml`'s own
comments already documented that this file OOM-kills a K8s Job even
at a 4Gi limit — the same lesson repeated here, worse: DockerOperator
sets no memory limit by default, so nothing capped the container, and
this machine's single Docker Desktop VM (7.75GB total) is shared with
the `kind` cluster's own containers, already using several GB. The
`preprocess_latest_data` container was observed climbing past 2.3GB
and rising before being stopped by hand to avoid taking down the
*entire* VM — not just its own task. Fixed by pointing both DAGs at
the same small, class-balanced sample CSV `kubernetes/jobs.yaml`
already uses (`data/raw/paysim_k8s_sample.csv`) and adding an explicit
`mem_limit="1500m"` (matching that Job's own limit) as a safety net,
so a future regression fails one task instead of threatening every
container sharing the host.

**A fourth real bug, found immediately after**: `daily_feature_materialization`'s
`materialize` task failed with `redis.exceptions.ConnectionError: Error
111 connecting to localhost:6379` — `feast_repo/feature_store.yaml`'s
checked-in `connection_string: "localhost:6379"` is correct for
host-side CLI use but unreachable from inside a container on the
`mloops_default` Docker network, exactly the problem ADR-0007 already
solved for Docker Compose's `api` service by bind-mounting
`docker/feature_store.docker.yaml` (identical file,
`connection_string: "redis:6379"`) over the baked-in one. The `api`
service's docker-compose entry already does this; this DAG's
`materialize` task didn't. Fixed by mounting the same override file
the same way. All three DAGs now run to a real, verified completion:
`monthly_drift_report` writes a real `docs/drift_report.html`,
`weekly_retraining` registers a new model version, and
`daily_feature_materialization` writes real feature rows into Redis
(confirmed via `redis-cli KEYS`/`DBSIZE` before and after).

### CI: `kubeconform`, not `kubectl apply --dry-run`

The obvious first choice for validating `kubernetes/*.yaml` in CI —
`kubectl apply --dry-run=client` — turned out not to work offline at
all: confirmed directly (`KUBECONFIG=/dev/null`) that it fails
immediately, because `apply`'s three-way merge needs to fetch the
*current* live object from a real API server even in "client" dry-run
mode, and modern `kubectl` also fetches its OpenAPI schema from the
server rather than a bundled copy. A GitHub Actions runner has neither.
`kubeconform` validates each manifest's structure against real
Kubernetes OpenAPI schemas with no cluster required at all — confirmed
working against every manifest in this repo (`-strict`, 27/27 valid),
correctly skipping `kind-cluster.yaml` (not a real `kubectl`-consumed
resource) rather than erroring on it.

Also added: a `build-images` job (matrix over the three Dockerfiles —
`api`, `worker`, `flink-worker` — build only, no push) and a
`validate-airflow-dags` job that imports each DAG module directly in
plain Python with `apache-airflow`/`apache-airflow-providers-docker`
installed, rather than standing up Postgres + scheduler + webserver in
CI — confirmed this is enough to catch real import/construction errors
without needing a live DB, matching how lightly these DAGs are
actually loaded by the real scheduler at parse time.

**Deliberately not added: an automated deploy/push step.** No
container registry is configured for this project and no cloud
cluster exists to deploy to — a "CD" step pushing images to nowhere,
or a `kubectl apply` against a `kind` cluster that only exists on one
developer's laptop, would be theater, not deployment. Getting from a
green CI run to the running `kind` cluster stays a deliberate, manual
`kubectl apply -f kubernetes/`, matching how this project already
treats "no fake work" everywhere else (ADR-0006's "CI stays
infra-free," ADR-0008's "left out, not hidden").

### Not covered by this milestone: stress testing

The Milestone 8 brief also named stress testing; this pass covers
Kubernetes deployment, Airflow orchestration, and CI/CD expansion —
stress testing the deployed system (load against `api`'s Service,
HPA scaling behavior under sustained load, Kafka throughput limits)
is a real, separate piece of work, not started, and is called out
here rather than left to look finished by omission.

## Consequences

- `kubernetes/`: `namespace.yaml`, `configmap.yaml`, `secret.yaml`,
  `redis.yaml`, `kafka.yaml`, `mlflow.yaml`, `api.yaml`,
  `flink-worker.yaml`, `jobs.yaml`, `hpa.yaml`, `ingress.yaml`,
  `prometheus.yaml`, `grafana.yaml`, `kind-cluster.yaml` — all applied
  and verified against a real cluster, not just written.
- `docker/Dockerfile.worker`, `docker/Dockerfile.flink-worker`: new
  images backing the K8s Jobs/Deployment and Airflow's `DockerOperator`
  tasks.
- `airflow/docker-compose.yml`, `airflow/dags/*.py`: a real, separately
  run orchestration stack, all three DAGs verified end to end.
- `.github/workflows/ci.yml`: `lint-and-test` now also covers
  `airflow/dags`; three new jobs — `build-images`,
  `validate-k8s-manifests` (`kubeconform`), `validate-airflow-dags`.
