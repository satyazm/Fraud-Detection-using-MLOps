# 3. Single feature pipeline shared across offline, online, and streaming

Date: 2026-07-25

## Status

Accepted

## Context

Training/serving skew — offline feature engineering code and online
feature code silently drifting apart — is one of the most common causes
of production ML failures, and unusually easy to hit here: PaySim data
will eventually be featurized both in bulk for training and one event
at a time from Kafka for live inference. Writing that logic twice (once
vectorized for batch, once for a single row) is exactly how skew
happens.

## Decision

- `fraud_detection.features.transformers` holds pure, stateless
  functions, each `DataFrame -> DataFrame`, operating on the raw
  Transaction schema (the same shape `data.ingestion` and
  `domain.schemas.transaction_to_dict` both produce). No feature here
  fits statistics from data (no means/stds/encoders) — every value is a
  deterministic function of a single row.
- `fraud_detection.features.feature_pipeline.FeaturePipeline` composes
  those transformers. `transform()` is the batch/offline entry point;
  `transform_one()` builds a one-row frame from a domain `Transaction`
  and calls `transform()` — so there is exactly one implementation of
  every feature, not two kept in sync by hand.
- `fraud_detection.features.registry` is the single source of truth for
  what features exist. `FeaturePipeline.transform()` validates its own
  output against it on every run, so a transformer/registry mismatch
  fails fast instead of shipping an undocumented feature. This registry
  is also what a Feast `FeatureView` will eventually be generated from.
- `fraud_detection.features.feature_store` defines a `FeatureStore`
  `Protocol` (offline batch read/write, online point read/write) with
  one local implementation (`LocalFeatureStore`) for now. A Feast-backed
  implementation (Milestone 5) satisfies the same `Protocol`, so
  `feature_pipeline`/`transformers` don't change when Feast and Redis
  arrive — only the store's construction site does.
- Feature computation must run before
  `data.preprocessing.preprocess()` in any pipeline, since some
  features (e.g. `is_dest_merchant`) read `nameDest`, which
  preprocessing drops.

## Consequences

- A Kafka/Flink consumer built in Milestone 4 calls
  `FeaturePipeline.transform_one()` per event with no new feature code.
- Swapping `LocalFeatureStore` for Feast in Milestone 5 touches only
  `feature_store.py` and its call sites, not feature logic.
- The "stateless only" assumption breaks the moment a model needs a
  fitted transform (e.g. a scaler). That's deferred on purpose: fitting
  introduces a second concern — persisting and loading fitted state
  identically in both the offline and online paths — that deserves its
  own decision once a model actually needs it, not one bundled into
  this ADR.
