"""Monthly: collect predictions -> run Evidently -> generate HTML report.

"Collect predictions" isn't a separate pipeline step here — predictions
are already collected continuously, by every real `/predict` request
(`monitoring/prediction_log.py`, Milestone 7) — so this DAG's first
task just confirms there's something to compare against (a real
check, not a placeholder task for its own sake) before running the
actual report.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.environ["PROJECT_ROOT"]  # see airflow/docker-compose.yml
IMAGE = "mloops-worker:latest"
MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/docs", target="/app/docs", type="bind"),
]

with DAG(
    dag_id="monthly_drift_report",
    description="Collect predictions -> run Evidently -> generate HTML report",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "evidently"],
) as dag:
    common = dict(
        image=IMAGE,
        network_mode="bridge",
        mounts=MOUNTS,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    check_predictions_collected = DockerOperator(
        task_id="check_predictions_collected",
        entrypoint=["sh", "-c"],
        command=[
            "test -s /app/data/monitoring/prediction_log.jsonl "
            '&& echo "$(wc -l < /app/data/monitoring/prediction_log.jsonl) predictions logged"'
        ],
        **common,
    )

    drift_report = DockerOperator(
        task_id="drift_report",
        command=[
            "drift-report",
            "--log-path",
            "/app/data/monitoring/prediction_log.jsonl",
            "--output-path",
            "/app/docs/drift_report.html",
        ],
        **common,
    )

    check_predictions_collected >> drift_report
