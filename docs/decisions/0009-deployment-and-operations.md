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

### `kafka`/`redis` need real volumes, not just a Deployment

**A real bug, found doing exactly this kind of "does it actually still
work" verification pass**: neither `kafka.yaml` nor `redis.yaml`
mounted any volume at all — both wrote to the container's own
ephemeral filesystem. A pod restart (the kubelet recreating one under
host memory pressure, not even a deliberate action — several pods
restarted during an earlier resource-pressure incident) silently wiped
both: `flink-worker` started crash-looping with
`UnknownTopicOrPartitionException` for the `transactions` topic
`producer-job` had already created and populated hours earlier, and
`redis-cli DBSIZE` against the cluster's Redis reported `0` despite
Airflow's `daily_feature_materialization` DAG having "already" written
real feature rows — to Docker Compose's *separate* Redis instance, it
turned out, a mix-up surfaced by this same investigation (the two
environments' Redis instances are entirely independent; nothing in the
Kubernetes deployment had ever actually pushed features into *this*
one before).

Fixed by giving both a `PersistentVolumeClaim` at the same paths
`docker-compose.yml` already persists to (`/var/lib/kafka/data`,
`/data`) and a `Recreate` deployment strategy (same reasoning as
`mlflow.yaml`'s — a single `ReadWriteOnce` volume can't be attached to
two pods at once). Verified for real, not just applied: re-ran
`producer-job` to recreate the topic, deleted the crash-looping
`flink-worker` pod to force an immediate retry rather than wait out its
backoff, watched it start processing and `redis-cli DBSIZE` climb to
`200` (matching `producer-job`'s `--limit 200` exactly), sent a real
`/predict` request for one of those exact transactions
(`{"prediction":1,"fraud_probability":0.9999...}`, correctly matching
that row's real `isFraud=1` label), then deleted both the `kafka` and
`redis` pods again to prove the fix: `DBSIZE` still `200` after Redis
came back, `flink-worker` reconnected with zero restarts (topic still
there), and the same `/predict` call still succeeded.

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
(confirmed via `redis-cli KEYS`/`DBSIZE` before and after) — Docker
Compose's Redis, since this DAG runs on the Compose network, entirely
separate from the Kubernetes cluster's own Redis (see the
`kafka`/`redis` persistence bug above for exactly the confusion this
distinction caused once).

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

### `dashboard/app.py`: a live demo, and two more real bugs it found

A Streamlit dashboard, not part of any deployed service, that streams
real PaySim rows through the actual pipeline (publishes to Kafka via
`streaming.serializer`/`domain.schemas` — this project's own
wire-format code, not a reimplementation) and scores each one via the
deployed API's `/predict`, showing the result next to PaySim's
ground-truth `isFraud` label and running accuracy/precision/recall/F1.
Verified live in a real browser, not just read as code: streamed 62
real transactions, watched Scored/Accuracy/the confusion matrix update
in real time, including one genuine false negative (94-97% recall
across the run, not a suspicious flat 100%).

**A real bug, found the moment it tried to publish**: Kafka's single
listener (`kafka.yaml`) only advertises its in-cluster DNS name
(`kafka:9092`). `kubectl port-forward` proxies the *bootstrap*
connection fine, but Kafka's client protocol then reconnects directly
to whatever address the broker advertises in its metadata response —
which the host can't resolve. Confirmed via `rdkafka` logs:
`kafka:9092/1: Failed to resolve 'kafka:9092'`. Fixed with a second
`EXTERNAL` listener advertised as `localhost:9094`, mirroring
docker-compose.yml's own PLAINTEXT (host) + PLAINTEXT_INTERNAL
(containers) split — the same fix, for the same reason, the moment a
second kind of non-pod consumer showed up.

**A second real bug, in the dashboard's own code**: its Kafka
`Producer` was cached in `st.session_state` keyed only on "does one
exist yet," so changing the bootstrap-servers setting (or its default,
while fixing the bug above) kept silently publishing through the
*old*, now-broken connection — confirmed via `rdkafka` logs still
resolving the stale address minutes after the visible setting had
changed. Fixed by also tracking which `bootstrap_servers` the cached
producer was built with and recreating it on a mismatch.

### `flink-worker`'s crash-loop: no fault isolation around the Feast write

`flink-worker` had a standing tendency to crash-loop, predating any of
the changes above (it had already restarted several times before this
milestone's own work began). Root cause, found by reading
`streaming.flink_job._ComputeAndPushFeatures.map()`: only
`InvalidTransactionError` (from deserializing a malformed Kafka
message) was caught. `FeaturePipeline.transform_one()` and
`FeastFeatureStore.write_online()` — the Feast/Redis write — had no
exception handling at all, and `write_online()` doesn't wrap its
errors the way `read_online()` does. Any transient failure there (a
Redis blip, a connection reset — exactly the kind of thing more likely
during the resource-pressure incidents this milestone's testing
produced) propagated straight up through PyFlink and killed the
*entire* streaming job, not just that one record.

Worse than a single crash: `run_flink_worker()` enables no Flink
checkpointing, and the Kafka source starts from
`KafkaOffsetsInitializer.earliest()`. With no checkpointed offsets to
resume from, every restart replays the *entire* topic from the
beginning — so a failure triggered once by real conditions (e.g. Redis
under memory pressure) would likely be triggered again by the replay
hitting the same point under similar conditions, a genuine
self-perpetuating crash loop, not just wasted reprocessing.

Fixed by wrapping the transform+write step in its own `try/except`,
mirroring the existing `InvalidTransactionError` handling: log the
failure with the entity id and skip that one record, exactly like a
malformed message already did, rather than taking the whole job down.
Deliberately a broad `except Exception` at this one specific per-record
boundary — the job's whole reason to exist is to keep running, and the
failure is still logged (structured JSON, visible in `kubectl logs`
and any log aggregation), not silently dropped.

Verified two ways, not just read as a diff: a new unit test
(`tests/streaming/test_flink_job_map_function.py`) exercises
`_ComputeAndPushFeatures.map()` directly with a store whose
`write_online` raises, confirming the failure is caught and the record
skipped, and — since this host has no working JVM (only macOS's
`java` stub, which fools both this test file's and `test_flink_job.py`'s
`shutil.which("java")` skip-check into thinking Java is available,
itself a pre-existing gap, not something this fix touches) — the
real fix was rebuilt into `mloops-flink-worker:latest`, reloaded into
`kind`, and rolled out; the new pod started clean and a real
`/predict` call against a transaction it had just streamed still
succeeded.

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
  and verified against a real cluster, not just written. `kafka.yaml`
  and `redis.yaml` gained their own `PersistentVolumeClaim`s
  (`kafka-data`, `redis-data`) after the bug above; `kafka.yaml` later
  gained a second `EXTERNAL` listener (port 9094) after the dashboard
  bug below.
- `docker/Dockerfile.worker`, `docker/Dockerfile.flink-worker`: new
  images backing the K8s Jobs/Deployment and Airflow's `DockerOperator`
  tasks.
- `airflow/docker-compose.yml`, `airflow/dags/*.py`: a real, separately
  run orchestration stack, all three DAGs verified end to end.
- `.github/workflows/ci.yml`: `lint-and-test` now also covers
  `airflow/dags` and `dashboard`; three new jobs — `build-images`,
  `validate-k8s-manifests` (`kubeconform`), `validate-airflow-dags`.
- `dashboard/app.py`: a local-only live demo, `streamlit` added to
  `requirements/dev.txt` (not installed by any deployed image).
- `src/fraud_detection/streaming/flink_job.py`: per-record fault
  isolation around the Feast write, fixing `flink-worker`'s crash-loop
  tendency; new unit test coverage in
  `tests/streaming/test_flink_job_map_function.py`.
