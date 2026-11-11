# Airflow orchestration & live demo

## Airflow orchestration

Three DAGs (`airflow/dags/`) orchestrate this project's own CLI on a
schedule, each running `mloops-worker:latest` as a short-lived sibling
container via `DockerOperator` rather than reimplementing any pipeline
logic in Airflow itself:

| DAG | Schedule | What it does |
|-----|----------|---------------|
| `daily_feature_materialization` | `@daily` | extract -> validate -> preprocess -> materialize Feast |
| `weekly_retraining` | `@weekly` | preprocess -> train -> evaluate |
| `monthly_drift_report` | `@monthly` | check predictions collected -> Evidently drift report |

Runs as its own Docker Compose stack, not pip-installed into this
project's venv:

```bash
cd airflow
docker compose up airflow-init   # one-time: DB schema + admin user (admin/admin)
docker compose up -d
```

Needs the main stack's network to already exist (`docker compose up`
from the repo root at least once), and `mloops-worker:latest` built
(see the [Kubernetes guide](kubernetes.md)). Airflow UI: `http://localhost:8085`
(DAGs are paused on creation — unpause before triggering). Both
`weekly_retraining` and `daily_feature_materialization` run against a
small sample CSV rather than the full ~6.4M-row PaySim file, and cap
their containers at 1.5GB — see ADR-0009 for exactly why (the full
file threatens the whole Docker Desktop VM this stack and `kind` share,
not just the one task).

## Live demo dashboard

`dashboard/app.py` (`streamlit run dashboard/app.py`) streams real
PaySim transactions through the actual Kubernetes pipeline (Kafka ->
`flink-worker` -> Feast/Redis), scores each one via the deployed API's
`/predict`, and shows the prediction next to PaySim's own ground-truth
`isFraud` label plus running accuracy/precision/recall/F1 and a
confusion matrix — see `dashboard/README.md` for setup (needs a Kafka
port-forward) and ADR-0009 for two real bugs this surfaced.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
