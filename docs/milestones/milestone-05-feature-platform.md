# Milestone 5: Real-Time Feature Platform (Feast + Redis + Apache Flink)

Status: **Done**. Commit `9a42d11` on `main`. 91 tests passing locally
(80 passing / 11 skipped-cleanly in CI, which has no Kafka/Redis/Java).

## Goal

Extend the Kafka streaming foundation from Milestone 4 with a real-time
feature platform:

```
producer → Kafka → flink-worker (real Apache Flink) → FeaturePipeline.transform_one()
    → Feast.write_online() → Redis → get_online_features()
```

No model inference, no FastAPI, no monitoring — those are later
milestones. The scope here is strictly: compute features from a live
Kafka stream, using the *same* feature code offline training uses, and
make them queryable via Feast within milliseconds through Redis.

A key upfront decision: real Apache Flink (PyFlink) was explicitly
requested and used, not a Kafka-consumer-loop relabeled as "Flink." A
lighter-weight alternative was offered first given the infrastructure
risk involved, but real PyFlink was chosen, so that's what got built —
see "Issues faced" below for what that actually took.

---

## What was built

### `src/fraud_detection/features/` (new files)

- **`entity_key.py`** — PaySim has no transaction id. `compute_entity_id(transaction)`
  derives one deterministically by hashing `step:type:name_orig:name_dest:amount:oldbalance_org`.
  `compute_entity_ids(df)` (the batch path) calls `compute_entity_id` per
  row rather than reimplementing the hash, so a transaction can never
  get two different ids depending on which code path computed it.
- **`feast_prep.py`** — `build_offline_source()` takes the output of
  `FeaturePipeline.transform()` and writes exactly the columns Feast's
  offline `FileSource` needs: the entity id, a synthetic `event_timestamp`
  (derived from PaySim's `step`, since PaySim has no real timestamps),
  and the registered features — nothing recomputed.
- **`feast_store.py`** — `FeastFeatureStore`, implementing the *exact*
  `FeatureStore` protocol `LocalFeatureStore` already satisfied (this
  slot was explicitly reserved back in the ADR-0003 decision, before
  Milestone 3: "A Feast-backed implementation (Milestone 5) satisfies
  the same Protocol, so feature_pipeline/transformers don't change when
  Feast arrives." It didn't change.).
- **`feast_ops.py`** — `apply_feast_definitions()` / `materialize_feast_features()`,
  which shell out to the real `feast` CLI (same pattern `models.training.get_git_commit_hash()`
  already uses for `git`) rather than reimplementing Feast's own
  repo-scanning and materialization logic.

### `src/fraud_detection/streaming/flink_job.py` (new)

A genuine PyFlink `DataStream` job:
`KafkaSource` → `deserialize_transaction()` (same function `consumer.py`
uses) → `FeaturePipeline.transform_one()` (same function offline
training goes through) → `FeastFeatureStore.write_online()` (Feast's
push API). Runs via **local-execution mode** — PyFlink's embedded
mini-cluster, launched through its own JVM gateway in-process — rather
than a separate Flink cluster. Supports `bounded=True` (stops at
whatever Kafka offset was latest when the job started) so tests
terminate instead of streaming forever.

### `feast_repo/` (new)

- `feature_store.yaml` — Redis online store, file offline store.
- `definitions.py` — the `transaction` entity and `transaction_features`
  `FeatureView`. Its schema is built directly from
  `fraud_detection.features.registry.FEATURE_REGISTRY` — there is no
  second list of feature names anywhere in the repo.

### CLI (`src/fraud_detection/cli.py`)

Three new commands: `fraud-detection feast-apply`, `materialize`,
`flink-worker` (the last with a `--bounded` flag).

### Docker / Makefile

- `docker-compose.yml`: Redis service hardened with a healthcheck
  (Kafka + Kafka UI already existed from Milestone 4).
- `Makefile`: `redis-up`, `redis-down`, `infra-up` (kafka+redis
  together), `infra-down`, `flink-jar` (downloads the Flink↔Kafka
  connector JAR, which is not a pip package), `feast-apply`,
  `materialize`, `flink-worker`.

### Tests (14 new, 91 total)

- `tests/features/test_entity_key.py`, `test_feast_prep.py` — pure,
  no external infra.
- `tests/features/test_feast_store.py` — needs real Redis, skips
  cleanly if unreachable.
- `tests/streaming/test_flink_job.py` — the actual proof of correctness:
  runs the **complete real pipeline** (produce → Kafka → PyFlink →
  Feast push → Redis) and asserts the feature values read back via
  `get_online_features()` are *exactly* equal (including float
  precision) to calling `FeaturePipeline.transform_one()` directly on
  the same transaction. Skips cleanly without Kafka + Redis + a JVM +
  the connector JAR.
- New CLI-level tests for `feast-apply`/`materialize`/`flink-worker`.

### Docs

- `docs/decisions/0006-feast-redis-flink.md` — why Feast, why Redis,
  why PyFlink specifically in local-execution mode (and what cluster
  mode would additionally cost), why feature parity matters and how
  it's enforced, and the two deliberate trade-offs (derived entity id,
  sample-scale offline materialization).
- README: full "Real-time feature platform" section — architecture
  diagram, one-time setup, running instructions, testing.
- `docs/architecture.md` updated (status, layer table, target flow
  diagram).

---

## Issues faced, and how each was actually resolved

Every one of these was hit for real while building this — not
anticipated risks, actual blockers that had to be diagnosed.

### 1. PyFlink wouldn't install

```
ModuleNotFoundError: No module named 'pkg_resources'
```

`apache-flink`'s dependency `apache-beam` has a `setup.py` that imports
`pkg_resources` at build time. Modern `pip`'s build isolation creates a
fresh, minimal build environment per package that doesn't include
`pkg_resources` unless the ambient environment has a compatible
`setuptools` *and* isolation is disabled.

**Fix:** `pip install "setuptools<81"` first, then
`pip install --no-build-isolation apache-flink==2.3.0`. Verified this
works from a completely fresh venv, not just the already-mutated dev
venv, before trusting it. Documented in `requirements/base.txt`,
`Makefile` (`install`/`install-dev` now do this automatically), and CI.

### 2. No Java runtime

```
Unable to locate a Java Runtime.
```

PyFlink is a Python API over Flink's Java core — it launches a JVM via
a `py4j` gateway. There was no JDK on the machine at all.

**Fix:** `brew install openjdk@17` (Flink 2.x needs JDK 11/17/21), then
export `JAVA_HOME=/opt/homebrew/opt/openjdk@17` and prepend it to
`PATH` — deliberately *not* the `sudo ln -sfn ... /Library/Java/...`
system-wide symlink Homebrew suggests, since that needs elevated
privileges and isn't necessary (`JAVA_HOME` alone is sufficient).
Verified importing `pyflink` modules does **not** require Java at all
(only actually creating a `StreamExecutionEnvironment` does) — this
matters because it means pytest *collection* is safe in CI even
without a JVM present; only the gated tests that actually run a job
skip.

### 3. Dependency resolution conflict once Feast entered the picture

```
ERROR: ResolutionImpossible
The conflict is caused by:
    apache-beam 2.61.0 depends on pyarrow<17.0.0 and >=3.0.0
    The user requested pyarrow==17.0.0
    apache-beam 2.60.0 depends on numpy<1.27.0
    feast 0.65.0 depends on numpy<3 and >=2.0.0
```

Installing `feast` and `apache-flink` together (a fresh, single-shot
`pip install -r requirements/dev.txt`) failed outright. Two separate
pinned constraints from `apache-beam` (a transitive dependency of
`apache-flink`) directly conflicted with `feast`'s and our own pins.

**Fix, not a guess:** inspected the actual declared constraints via
`importlib.metadata.requires()` rather than trial-and-error:

- `feast` needs `numpy>=2.0.0,<3`; `apache-beam` needs
  `numpy>=1.14.3,<2.2.0` → the real overlap is `numpy==2.1.3`.
- `apache-beam` needs `pyarrow<17.0.0`; `feast`/`mlflow`/`apache-flink`
  are all satisfied by `pyarrow>=16.1.0` too → pinned `pyarrow==16.1.0`.

Verified the fix with a **from-scratch venv** (`pip install "setuptools<81"`
then `pip install --no-build-isolation -r requirements/dev.txt` in
one shot) before trusting it, then re-ran the *entire* existing test
suite (pandas/sklearn/xgboost/lightgbm/matplotlib all still import and
all Milestone 1-4 tests still pass) since bumping `numpy` and `pyarrow`
touches everything downstream, not just the new Feast/Flink code.

### 4. Kafka connector JAR isn't a pip package

PyFlink's `KafkaSource` Python class is just a wrapper — it needs the
actual Java Kafka connector on the classpath, which isn't installed by
`pip install apache-flink`.

**Fix:** downloaded `flink-sql-connector-kafka` from Maven Central
directly. The only version published there was built for Flink 2.2
(`5.0.0-2.2`); our runtime is Flink 2.3.0. Tested it against the real
broker rather than assuming compatibility — it works. Added
`make flink-jar` (idempotent download) and a clear `FileNotFoundError`
in `flink_job.py` if the JAR is missing, pointing at that target,
instead of a cryptic Java `ClassNotFoundException` surfacing later.

### 5. Consumer groups silently receiving nothing (again, now inside Flink)

This exact failure class was already fixed for the plain Kafka
consumer in Milestone 4, but it silently affects PyFlink's
`KafkaSource` too, since it also uses Kafka's consumer-group protocol
under the hood:

```
FindCoordinator response error: COORDINATOR_NOT_AVAILABLE
```

Root cause (unchanged from Milestone 4): `offsets.topic.replication.factor`
defaults to 3, which a single-broker dev cluster can never satisfy, so
`__consumer_offsets` never gets created and **every** consumer group —
including PyFlink's — fails to find a coordinator forever, with no
error surfaced to a naive poll loop.

**Fix:** already set to `1` in `docker-compose.yml` from Milestone 4;
confirmed it also fixes PyFlink's consumption (it does — PyFlink read
real messages correctly once this was in place, from the very first
Flink+Kafka smoke test).

### 6. PyFlink 2.x removed `RichMapFunction`

The design called for a stateful map operator (construct the Feast
client and `FeaturePipeline` once per worker in `open()`, not once per
record). Following older PyFlink examples:

```
ImportError: cannot import name 'RichMapFunction' from 'pyflink.datastream.functions'
```

**Fix:** read the actual installed source
(`inspect.getsource(MapFunction)`, `inspect.getsource(Function)`)
instead of trusting an outdated tutorial. In PyFlink 2.x, the base
`Function`/`MapFunction` classes already carry `open(self, runtime_context)`/
`close(self)` — the separate `Rich*` naming was removed. Subclassed
`MapFunction` directly.

### 7. A CLI design flaw caught before it shipped

`--offline-source-path` was originally an independently overridable
flag on both `materialize` and `flink-worker`. But Feast's registered
`FileSource` path is **fixed** at `feast apply` time, from whatever
`feast_repo/definitions.py` declares (`DEFAULT_OFFLINE_SOURCE_PATH`).
Overriding the CLI flag would silently write the offline parquet to a
location `feast materialize` never actually reads from — a working
CLI flag that quietly does nothing useful.

**Fix:** removed the flag from both commands' CLI surface entirely
(kept as an internal parameter on the underlying Python functions,
where it's genuinely useful and safe for direct callers/tests that
don't go through `feast materialize`'s fixed `FileSource`). Documented
the reasoning inline in `cli.py` so it doesn't get silently
reintroduced later.

### 8. Narrow materialize window (not a bug, but surprising)

First manual `feast materialize` run only pulled in a fraction of the
2000-row sample. Cause: the CLI computes the materialize window
dynamically from the sample's actual `step` range
(`end = start + timedelta(hours=max(step)+1)`), and PaySim's first
2000 raw rows all fall within `step=1` (the same simulated hour) — so
the correct, dynamically-computed window was only 2 hours wide.
Verified this was correct (not a bug) by checking that all 2000 rows
still landed in Redis despite the narrow window, since they share the
same synthetic timestamp bucket, and confirming values via
`get_online_features()`.

---

## Verification performed (not claimed, actually run)

- Every stage tested against real infrastructure at least once,
  standalone, before wiring into the full pipeline: local PyFlink
  execution → PyFlink reading real Kafka messages → PyFlink running
  our real `FeaturePipeline.transform_one()` inside a worker process →
  `feast apply` → `feast materialize` → `get_online_features()` →
  Feast's push API.
- **Exact-match correctness check**, twice: once via the offline
  materialize path, once via the live streaming push path — both times
  comparing Redis-backed `get_online_features()` output against
  `FeaturePipeline.transform_one()` called directly, asserting equality
  including floating-point precision (not `pytest.approx`).
- Full test suite (91 tests) run twice against freshly recreated Kafka
  + Redis containers (`docker compose down -v` then back up), to rule
  out "works because of leftover state."
- `ruff`, `black`, `mypy` (strict) clean across `src/`, `tests/`, and
  `feast_repo/`.
- `pre-commit run --all-files` clean, including the mypy hook.
- Pushed to GitHub; CI green (`80 passed, 11 skipped` — confirmed via
  the raw CI log that the 11 skips are all the Kafka/Redis/Java-gated
  tests skipping for the expected, logged reason, not silently passing
  or erroring).

## Known, documented trade-offs (not oversights)

- **Derived entity id**: PaySim has no transaction id; one is derived
  deterministically by hashing a subset of fields, documented in
  `entity_key.py` and ADR-0006, not hidden.
- **Sample-scale offline materialization** (2000 rows by default, not
  the full ~6.4M-row dataset) — the milestone's own scope says
  file/parquet is acceptable for local development.
- **Local-execution PyFlink, not a cluster** — a real, load-bearing
  architectural choice (ADR-0006), not a shortcut: it exercises the
  identical `DataStream` API, Kafka connector, and Python-worker
  execution model a cluster deployment would, without the added risk
  of building a custom Flink image for this milestone's actual goal
  (proving Kafka→Flink→Feast→Redis parity). Documented as a natural,
  isolated follow-up if real parallelism is ever needed.
- **CI has no Kafka/Redis/Java** — same call already made for Kafka in
  Milestone 4. All infra-dependent tests are written to skip cleanly,
  confirmed both locally (by stopping the containers) and in CI logs.
