# 6. Real-time feature platform: Feast + Redis + local-execution PyFlink

Date: 2026-07-25

## Status

Accepted

## Context

Milestone 4 gave us a Kafka producer/consumer that logs transactions —
no feature computation in the stream. Milestone 5's job is to make
features computed from live Kafka events queryable in near-real-time,
the way a serving API (Milestone 6) will need them: by looking up a
transaction's precomputed features in milliseconds, not by running
`FeaturePipeline.transform()` inline in the request path.

That requires three new pieces, each with its own decision to make:
somewhere to define and serve features (a feature store), somewhere
fast enough for point lookups to back it (an online store), and
something to run the Kafka-to-features computation continuously (a
stream processor).

## Decisions

### Why Feast

Feast is the piece that turns "a Python function that computes
features" into "a queryable feature store with an offline/online
split, a registry, and a stable `get_online_features()` API." Building
that abstraction by hand (which `LocalFeatureStore`, added in
ADR-0003, deliberately stubbed out) would mean reinventing point-in-time
correctness, online/offline consistency, and a registry — all things
Feast already does. ADR-0003 explicitly reserved this slot: "A
Feast-backed implementation (Milestone 5) satisfies the same Protocol,
so feature_pipeline/transformers don't change when Feast arrives." It
didn't — `FeastFeatureStore` implements `FeatureStore` unchanged.

### Why Redis

Feast's online store needs point-lookup latency (single-digit
milliseconds), not the scan/aggregate latency a file or SQL table
offline store is built for. Redis is Feast's most common online store
choice, is a single `docker-compose` service with no schema migration
story of its own, and needs nothing beyond a `connection_string` in
`feast_repo/feature_store.yaml`.

### Why PyFlink (and specifically local-execution mode)

You asked for real Apache Flink, not a Kafka-consumer-loop wearing a
Flink-shaped label — and real Flink is what got built and verified end
to end: a genuine PyFlink `KafkaSource` reading the real Kafka broker,
computing features via the real `FeaturePipeline.transform_one()` in a
Flink Python worker process, pushing to the real Feast/Redis online
store, read back via `get_online_features()` with an exact match.

The one real design choice within "use Flink" was *how* to run it.
PyFlink supports two modes:

1. **Cluster mode**: a separate JobManager/TaskManager deployment
   (Docker containers), with jobs submitted to it. This is how Flink
   runs in production.
2. **Local-execution mode**: `StreamExecutionEnvironment.get_execution_environment()`
   launches an embedded mini-cluster in-process via a JVM gateway
   (py4j) — no separate cluster to deploy.

This project uses local-execution mode. Cluster mode would need a
custom Flink Docker image with `fraud_detection` and its dependencies
(pandas, scikit-learn, xgboost — the same stack `FeaturePipeline`
needs) installed inside the TaskManager containers, plus Kafka
connector JAR management on the cluster side too. That's real,
learnable infrastructure work, but it's orthogonal to this milestone's
actual goal (proving Kafka → Flink → Feast → Redis feature parity) and
would have roughly doubled the risk surface for no correctness
difference — local-execution mode exercises the exact same PyFlink
`DataStream` API, connector, and Python-worker execution model
(confirmed: features are computed in a separate Python process the
JVM spawns, communicating over Apache Beam's portability protocol,
identical to how a TaskManager would run it). If this project later
needs cluster-mode Flink (e.g. for parallelism beyond one machine),
that's a natural, isolated follow-up — nothing in `flink_job.py`'s
actual logic would need to change, only how the job gets submitted.

**Real setup cost, worth recording**: PyFlink needs a JVM (JDK 11/17/21;
`brew install openjdk@17` + `JAVA_HOME`), `apache-flink` needed
`setuptools<81` and `pip install --no-build-isolation` to build
cleanly (apache-beam's setup.py uses `pkg_resources`), and the Kafka
connector is a separate JAR (`flink-sql-connector-kafka`, not a pip
package) fetched once via `make flink-jar`. All three are one-time
local setup steps, documented in the README, not blockers — every one
of them was hit and resolved while building this, not left as a
"should work" gap.

**CI stays infra-free.** Kafka, Redis, and Java are not provisioned in
CI (same call Milestone 4 made for Kafka). Flink/Feast/Redis-dependent
tests skip cleanly (verified: `pyflink`/`feast` *import* fine without
a JVM/Redis present — only *running* a job or querying Redis needs
them) rather than failing or hanging CI. mypy/ruff/black still run
against every new module regardless.

### Why feature parity matters, and how it's enforced here

Training/serving skew — offline and online feature code silently
drifting apart — is the single most common way real fraud-detection
systems fail quietly (a model trained on features that don't match
what serving computes degrades without an obvious error). This
project's answer, unchanged since ADR-0003: there is exactly one
feature implementation, `FeaturePipeline`. The streaming path (this
milestone) and the offline path (Milestone 2/3) both call it —
`transform()` for batches, `transform_one()` for a single live
transaction — never a separate reimplementation. Verified directly,
not assumed: `tests/streaming/test_flink_job.py` asserts a feature
value read back from Redis after going through the *entire* real
pipeline (produce → Kafka → PyFlink → Feast push → Redis) equals
`FeaturePipeline.transform_one()` called directly on the same
transaction — including floating-point precision, not just
approximately.

### Trade-off: a derived entity id

PaySim has no transaction id (a real payment processor would supply
one). `fraud_detection.features.entity_key.compute_entity_id` derives
one deterministically by hashing a subset of a transaction's fields.
This is a known simplification, not a hidden one: `compute_entity_ids`
(the batch path) calls `compute_entity_id` (the single-transaction
path) per row rather than reimplementing the hash, specifically so the
same transaction can never get two different ids depending on which
code path computed it — which would silently break every
offline/online lookup this milestone exists to prove correct.

### Trade-off: sample-scale offline materialization

The offline `FileSource` is built from a configurable sample of the
PaySim CSV (`--sample-size`, default 2000 rows via
`fraud-detection materialize`), not the full ~6.4M-row dataset. The
milestone's own scope says "file/parquet is acceptable for
development" for the offline store; materializing millions of rows
into a single local Redis instance for a dev/demo feature platform
would cost real time and memory for no architectural difference. If
this needs to scale later, `build_offline_source` already takes any
already-featurized DataFrame — the sample-size limit lives in the CLI
layer, not in feature logic.

## Consequences

- `fraud_detection.features` gained `entity_key.py`, `feast_prep.py`,
  `feast_ops.py`, `feast_store.py`; `fraud_detection.streaming` gained
  `flink_job.py`. No existing feature-computation file
  (`transformers.py`, `feature_pipeline.py`, `registry.py`) changed.
- `feast_repo/definitions.py` derives its `FeatureView` schema from
  `FEATURE_REGISTRY` — a new registered feature is available to Feast
  automatically on the next `feast apply`, with no second definition
  to maintain or forget.
- Local-execution PyFlink is a real dependency (JVM, JAR, build flags)
  a contributor must set up once locally — documented in the README's
  setup section, not hidden.
- Moving to cluster-mode Flink later is additive (new deployment +
  submission path) rather than a rewrite, because `flink_job.py`'s
  actual pipeline logic doesn't reference how/where the job runs.
