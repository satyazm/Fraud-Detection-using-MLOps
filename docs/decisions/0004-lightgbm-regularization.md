# 4. LightGBM needs explicit L2 regularization at this dataset's class weight

Date: 2026-07-25

## Status

Accepted

## Context

The first `fraud-detection train` run (Milestone 3) scored LightGBM at
validation PR-AUC **0.0138** — dramatically worse than Logistic
Regression (0.95), Random Forest (0.998), and XGBoost (0.998), despite
all four models seeing the same features and the same imbalance
(~0.13% fraud, `scale_pos_weight` = negative/positive ≈ 774).

A large, unexplained gap between otherwise-comparable models is a bug
until proven otherwise, not a "finding." This ADR records the
investigation, not just the fix, so the reasoning survives past this
commit.

## Investigation

All experiments below were run against the real 6,362,620-row PaySim
split (`data/processed/`), not synthetic data.

**1. Symptom, not just the score.** At the raw 774x weight, LightGBM
flagged 76,549 of 954,392 validation rows (8%) as fraud, against a true
rate of 0.13% — a false-positive flood, not a ranking failure (ROC-AUC
was still 0.92, so the model had *some* signal; PR-AUC collapsed
because precision at every threshold was poor).

**2. Ruled out: imbalance-handling mechanism.** `is_unbalance=True`
(LightGBM's own built-in imbalance handling) produced the identical
score to explicit `scale_pos_weight=774` — LightGBM computes the same
ratio internally. The *mechanism* wasn't the issue; the *magnitude* at
this weight was.

**3. Ruled out: "too few boosting rounds."** The opposite was true —
more rounds made it *worse* at the raw weight, holding everything else
fixed:

| n_estimators | validation PR-AUC | false positives |
|---|---|---|
| 10  | 0.878 | 176 |
| 50  | 0.180 | 5,351 |
| 100 | 0.168 | 6,617 |
| 200 | 0.014 | 75,424 |

This is an overfitting signature, not an undertrained one.

**4. Confirmed via early stopping.** Adding `early_stopping(20)` against
a validation set (raw weight, 200-round budget) picked
`best_iteration_ = 1` — validation PR-AUC peaked after a single round
and degraded every round after. Early stopping alone recovered PR-AUC
to 0.876.

**5. Ruled out: LightGBM is just fragile under this weight.** XGBoost,
given the *identical* raw 774x weight, stayed at PR-AUC 0.998-0.999
across `n_estimators` from 10 to 500 — no degradation. Whatever was
happening was specific to LightGBM's boosting, not an inherent property
of training under a 774x weight.

**6. Root cause.** XGBoost defaults to `reg_lambda=1` (L2
regularization on leaf weights); LightGBM defaults to `reg_lambda=0`
(none). With no regularization and an extreme per-positive-sample
weight, LightGBM's leaf-wise (best-first) growth has nothing to dampen
the compounding effect of that weight across boosting rounds — it
keeps carving increasingly aggressive, narrow splits that fit the
heavily-upweighted minority class ever more specifically, which is
exactly the "more rounds = worse" and "best_iteration_ = 1" pattern
above. Matching XGBoost's default — `reg_lambda=1.0`, nothing else
changed, still the raw 774x weight, still 200 rounds, no early stopping
— recovered PR-AUC to **0.995**.

## Decision

`fraud_detection.models.training._build_lightgbm` sets `reg_lambda=1.0`
and uses the raw (undampened) `scale_pos_weight`, matching how XGBoost
is configured. No weight-dampening heuristic, no early-stopping
machinery added to the shared training loop — the targeted fix for the
actual root cause is one hyperparameter.

## Consequences

- LightGBM now scores competitively (PR-AUC ~0.995 vs XGBoost's ~0.998)
  instead of looking broken.
- This is dataset/weight-magnitude-specific: at a much smaller class
  weight, `reg_lambda` might matter far less. Revisit if `models/data`
  changes shift the imbalance ratio significantly.
- Early stopping remains a reasonable general improvement (it would
  have caught this without diagnosing the regularization gap) but was
  deliberately not added here — it would require passing a validation
  `eval_set` through `train_and_compare`'s otherwise-uniform
  `model.fit(x_train, y_train)` call for every model spec, which is
  more shared-loop complexity than this fix needed. Worth reconsidering
  if a future model proves sensitive to overfitting in a way a fixed
  hyperparameter can't address.
