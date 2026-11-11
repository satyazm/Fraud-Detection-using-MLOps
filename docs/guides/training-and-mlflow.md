# Model training & MLflow

`fraud-detection train` trains Logistic Regression, Random Forest,
XGBoost, and LightGBM on `data/processed/` (already feature-engineered
by `fraud-detection preprocess`), handles the ~0.13% fraud imbalance via
class weighting, and logs every run to a local MLflow file store
(`mlruns/`, gitignored) — params, metrics, the model artifact,
`feature_version` (from `fraud_detection.features.registry`), and the
git commit hash. The run with the best **validation** `average_precision`
(PR-AUC — the meaningful metric here, since accuracy is ~99.87% even for
a model that never predicts fraud) is registered under
`fraud-detection-classifier` in the MLflow Model Registry. Browse runs
with:

```bash
mlflow ui --backend-store-uri file:./mlruns
```

## Train / validation / test split

`fraud-detection preprocess` splits **chronologically by PaySim's `step`**
(simulated hours) with `data.split.temporal_split`: the earliest ~70% of
rows train, the next ~15% validate, the latest ~15% test. Cutoffs fall on
`step` boundaries, so a step is never divided across splits. A random
split would put near-identical, same-account, same-period rows on both
sides and inflate test metrics; production always scores *future*
transactions.

| Split | Rows | `step` range | Fraud rate |
|---|---|---|---|
| train | 4,463,587 | 1–323 | 0.0816% |
| validation | 980,416 | 324–378 | 0.0575% |
| test | 918,617 | 379–743 | 0.4361% |

Fraud prevalence differs across splits, so PR-AUC values are not directly
comparable between them. Model selection uses the validation split only;
the test split is reported but never used to choose a model.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
