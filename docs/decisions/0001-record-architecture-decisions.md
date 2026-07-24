# 1. Record architecture decisions

Date: 2026-07-24

## Status

Accepted

## Context

As the platform grows (streaming, feature store, model registry,
serving), significant technical choices need a durable record of *why*
they were made, not just *what* was chosen — so future contributors
(and future us) aren't left reverse-engineering intent from code.

## Decision

We will use Architecture Decision Records, as described by Michael
Nygard, for decisions with meaningful cost-to-reverse: technology
choices, layer boundaries, data contracts, and deviations from the plan
in `docs/architecture.md`.

Each ADR lives in `docs/decisions/NNNN-title-with-dashes.md`, numbered
sequentially, and is never edited after acceptance — a changed decision
gets a new ADR that supersedes the old one.

## Consequences

- Decisions and their rationale are searchable in git history rather
  than living only in chat/PR discussion.
- Small overhead per significant decision; not used for routine
  implementation choices.
