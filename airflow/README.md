# airflow/

Three DAGs orchestrating this project's own CLI on a schedule — see
the main README's "Airflow orchestration" section for setup and
ADR-0009 for the decisions and real bugs behind them.

Runs as its own Docker Compose stack (`docker-compose.yml` here),
separate from the main project's Python environment.
