# airflow/

Three DAGs orchestrating this project's own CLI on a schedule — see the [Airflow guide](../docs/guides/airflow-and-dashboard.md) for setup and [ADR-0009](../docs/decisions/0009-deployment-and-operations.md) for the decisions and real bugs behind them.

Runs as its own Docker Compose stack (`docker-compose.yml` here), separate from the main project's Python environment.
