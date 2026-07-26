# 7. Real-time inference API: FastAPI, Feast-or-error, Production-stage MLflow

Date: 2026-07-26

## Status

Accepted

## Context

Milestone 5 made features computed from live Kafka events queryable in
near-real-time via Feast/Redis, but proved that path with no model in
it. Milestone 6's job is the piece ADR-0006 explicitly deferred: score
a transaction, in the request path, using the Production model and the
online features Milestone 5 already computes — without recomputing
feature logic a second time, and without reloading the model per
request.

## Decisions

### Why the API's `PredictionService` never recomputes features

Feast's online store (`transaction_features`, `feast_repo/definitions.py`)
holds exactly the 9 columns in `features.registry.FEATURE_REGISTRY` —
the *engineered* features. The model, however, was trained on 21
columns: those 9, plus 7 raw PaySim fields (`step`, `amount`,
`oldbalanceOrg`, ...) and 5 one-hot `type_*` columns that
`data.preprocessing.preprocess()` produces (confirmed against
`data/processed/train.parquet`). Feast was never going to hold the
raw/one-hot columns — they aren't "engineered," they're the request's
own input, so `PredictionService._build_feature_row` takes them
directly from the incoming `Transaction` instead, reusing
`preprocess()` (the *same* encoding training used) rather than writing
a second `type`-encoding implementation. The 9 engineered columns
always come from `FeatureStore.read_online()`, never from
`FeaturePipeline` run inline — `PredictionService` has no dependency on
`FeaturePipeline` at all, on purpose.

A consequence: if a transaction hasn't already flowed through the
Milestone 5 Kafka → Flink → Feast pipeline, `/predict` returns a clean
503, not a silently-computed answer. This is deliberate, not a gap —
recomputing here would mean two feature implementations that can drift
apart, exactly what ADR-0003/ADR-0006 exist to prevent. The 503's
message is the real underlying error (`FeatureStoreError`), so it's
diagnosable, not generic.

### Why feature order comes from the MLflow run, not a hardcoded list

Serving must send the model a DataFrame with the *exact* column
set/order it trained on, or it silently scores garbage no exception
will catch. `models.training.train_and_compare` already logs
`feature_names.json` as an MLflow artifact for every run (pre-existing,
unrelated to this milestone). `model_registry.load_feature_names(run_id)`
reads that artifact rather than a second, hand-maintained column list —
one source of truth, the same principle `features.registry.FEATURE_REGISTRY`
already applies to feature definitions themselves.

### Why "Production" stage, not "latest version"

`evaluate` (Milestone 3) resolves the latest registered version
on purpose — evaluation should see whatever was just trained. Serving
needs a different guarantee: a freshly trained model is registered but
not automatically promoted, so a human/CI gate must move a version to
the MLflow "Production" stage before it can score live traffic.
`model_registry.resolve_production_model` implements that; a fresh
`fraud-detection train` run alone does not make a new model servable.
(MLflow stages are deprecated in favor of aliases as of 2.9 — still
fully functional in the pinned 2.17.2 and the more direct fit for a
single "the one serving right now" slot; switching to aliases is a
contained follow-up if a future MLflow upgrade forces it.)

### Why `/health` and `/ready` are different endpoints, and startup never crashes the process

`api.app.load_app_state` catches every exception from resolving the
Production model, loading it, and constructing the Feast client. If
any of that fails (no model promoted yet, MLflow/Feast/Redis down),
the process still starts: `/health` (liveness) stays `200 ok`, but
`/ready` reports `model_loaded`/`feast_reachable`/`redis_reachable`
individually and `/predict` returns 503 via `get_prediction_service`
(which reuses the domain's own `ModelNotReadyError` intent, though it
surfaces as an HTTP status rather than a raised domain exception at
that boundary). The alternative — raising during startup — would
crash-loop the whole process for an outage `/ready` exists specifically
to describe instead; this is the standard liveness/readiness split for
exactly this reason.

`feast_reachable` and `redis_reachable` check different things on
purpose: Feast's Python client has no dedicated health-check call, so
"reachable" here means the client constructed against the registry
without error; `redis_reachable` is a live TCP probe against the
actual online-store backend, since that's what `get_online_features()`
ultimately depends on. A `FeastFeatureStore` can construct
successfully with Redis down — confirmed directly (`/ready` returned
`feast_reachable: true, redis_reachable: false` against the real
`feast_repo/` with no Redis running).

### A real bug this surfaced: `FeastFeatureStore.read_online` didn't wrap Redis outages

Manually verifying `/predict` against a real Production model with
Redis intentionally down returned a bare 500, not the 503
`api.routers.predict` maps `FeatureStoreError` to. Root cause:
`read_online` (Milestone 5) only ever raised `FeatureStoreError` for
"entity not found" — a genuine `redis.exceptions.ConnectionError` from
a Redis outage passed straight through uncaught, because Milestone 5's
own tests always skip when Redis is unreachable, so that path was
never exercised. Fixed at the source (`features/feast_store.py`), not
in the API layer: `read_online` now wraps `RedisError` into
`FeatureStoreError` too, so every caller — the Flink worker and this
API — gets one exception type for "features aren't available right
now," matching the `FeatureStore` protocol's actual contract. Covered
by `tests/features/test_feast_store_redis_errors.py`, deliberately not
gated on Redis being reachable (it needs Redis to be *unreachable* to
be meaningful).

### Docker: Feast's Redis hostname needs a container-side override

`feast_repo/feature_store.yaml`'s `connection_string: "localhost:6379"`
is correct for host-side `flink-worker`/`materialize`/tests but
unreachable from inside the `api` container — the same reason `kafka`
needs two listeners (`docker-compose.yml`). Rather than making the
tracked `feast_repo/feature_store.yaml` container-aware (which would
break the tested host-side path, or require an env-var-substitution
mechanism this project doesn't otherwise use), `docker/feature_store.docker.yaml`
duplicates it with `redis:6379` and is bind-mounted over the real file
for the `api` service only. The `api` container also mounts the host's
real `mlruns/`/`feast_repo/` rather than pointing at the separate
`mlflow` compose service — that HTTP server remains unused scaffolding
(as `docker-compose.yml` already documented) until a future milestone
points training at it too; reusing the already-tested local file-store
path was the lower-risk choice for this milestone.

### Verified against a real `docker build`/`docker compose up` — four more real bugs

Building and running the actual image (once a Docker daemon was
available) surfaced four issues no amount of static review would have
caught, in order:

1. **No C compiler in the base image.** `apache-beam` (pulled in by
   `apache-flink`, a `requirements/base.txt` dependency even though
   this image never imports `pyflink`) compiles a C extension
   (`coder_impl_fast.c`) at install time. `python:3.11-slim` has none —
   works on a host machine only because Xcode/Homebrew already provide
   one. Fixed: `apt-get install build-essential` before the pip install
   (plain `gcc` alone wasn't enough either — the first attempt failed
   on a missing `stdlib.h`, i.e. missing libc headers, not a missing
   compiler).
2. **MLflow's local file store bakes the absolute host path into every
   run's metadata.** `meta.yaml`'s `artifact_uri` is a literal
   `file:///Users/.../mlruns/<exp>/<run>/artifacts`, written at
   training time — mounting `mlruns/` at a *different* container path
   (the original design, `/app/mlruns`) makes `mlflow.sklearn.load_model()`
   look for that exact host path and fail with "No such file or
   directory". Fixed: bind-mount at the identical `${PWD}`-based path
   instead (`docker-compose.yml` interpolates `${PWD}` from the
   invoking shell), so the container sees the same absolute path the
   metadata already points at.
3. **`mlflow.sklearn.load_model()` writes during a read.** It creates a
   `registered_model_meta` marker in the artifact directory even when
   just loading a model — a `:ro` bind mount on `mlruns/` failed with
   "Read-only file system" on that exact file. Fixed: mount read-write.
4. **The API's own Redis-reachability probe didn't share Feast's
   config.** `/ready` reported `redis_reachable: false` inside the
   container even though Feast itself reached Redis fine — `app.py` had
   a second, separately hardcoded `"localhost:6379"` for its live TCP
   check, unrelated to whatever `feast_repo/feature_store.yaml` (or its
   Docker override) actually configured. Fixed by adding
   `FeastFeatureStore.online_store_address()`, which parses the same
   `connection_string` Feast's own client already loaded, so there's
   exactly one place that knows where Redis is — not two that can
   silently disagree. Covered by
   `test_online_store_address_matches_configured_connection_string` in
   `tests/features/test_feast_store_redis_errors.py`.

After all four fixes: `docker compose build api` succeeds,
`docker compose up -d kafka kafka-ui redis api` brings up a healthy
stack (`docker ps` shows the container's own `HEALTHCHECK` — which
dogfoods `fraud-detection ready` — reporting `healthy`), `/ready`
returns all three checks `true`, and a real `/predict` call against the
first row of the actual PaySim CSV (after `fraud-detection materialize`
populated Feast) returned `200` with a correct, low fraud probability —
matching that row's known non-fraud label.

## Consequences

- `src/fraud_detection/api/` is new: `app.py` (lifespan/startup),
  `dependencies.py` (`AppState` + `Depends` providers),
  `prediction_service.py` (business logic, no FastAPI import),
  `routers.py` (thin HTTP mapping), `schemas.py` (Pydantic models).
  Replaces the empty `src/fraud_detection/serving/` placeholder
  `docs/architecture.md` originally reserved — named `api` to match
  where this milestone actually built it.
- `models/model_registry.py` gained `resolve_production_model`,
  `load_feature_names`, `ProductionModel`. `features/feast_store.py`
  gained `online_store_address()`, and `read_online` now wraps
  `RedisError`. Neither changes existing callers' behavior on the
  success path.
- `fraud-detection api`/`fraud-detection ready` replace the `api`
  CLI placeholder; `ready` also backs the Docker image's `HEALTHCHECK`.
- A model must be explicitly promoted to the MLflow "Production" stage
  before `/predict` will ever return a prediction — documented in the
  README, not automated, on purpose (see "Why 'Production' stage" above).
