"""Weekly: preprocess (load latest data) -> train -> evaluate.

"Register Best Model" isn't a separate task here — `fraud-detection
train` already registers the best of the trained candidates as part of
one run (existing Milestone 3 behavior; see `_cmd_train` in
src/fraud_detection/cli.py), so adding a distinct registration step
would just be Airflow re-describing something the CLI already does,
not new work. `evaluate` runs after as a real sanity check against the
held-out test split, not a no-op.

mlruns/ is mounted at the *same absolute path* inside the container as
on the host, not `/app/mlruns` — MLflow's local file store bakes the
absolute host path into each run's meta.yaml as its artifact_uri
(confirmed the hard way in ADR-0007's Docker verification), so a
different container-side path breaks `mlflow.sklearn.load_model()`
later. This mirrors docker-compose.yml's `${PWD}/mlruns:${PWD}/mlruns`
mount for the exact same reason.

Uses the same small sample CSV as `kubernetes/jobs.yaml`'s
training-job, not the full ~6.4M-row file, and for the exact same
reason: confirmed the hard way here too — `preprocess_latest_data`
against the full CSV grew past 2.3GB and climbing on this machine's
single shared 7.75GB Docker Desktop VM (the same VM the kind cluster
containers already live in), which would have taken the whole VM
down, not just this task. `mem_limit` below is a safety net so a
future regression fails this one task instead of threatening
everything else sharing the VM.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.environ["PROJECT_ROOT"]  # see airflow/docker-compose.yml
TRACKING_URI = f"file:{PROJECT_ROOT}/mlruns"
IMAGE = "mloops-worker:latest"
MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/mlruns", target=f"{PROJECT_ROOT}/mlruns", type="bind"),
]

with DAG(
    dag_id="weekly_retraining",
    description="Load latest data -> train -> evaluate -> register best model",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "mlflow"],
) as dag:
    common = dict(
        image=IMAGE,
        network_mode="bridge",  # no kafka/redis/mlflow-service dependency — file-store only
        mounts=MOUNTS,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        mem_limit="1500m",  # matches kubernetes/jobs.yaml's training-job limit
    )

    preprocess = DockerOperator(
        task_id="preprocess_latest_data",
        command=[
            "preprocess",
            "--raw-path",
            "/app/data/raw/paysim_k8s_sample.csv",
            "--output-dir",
            "/app/data/processed",
        ],
        **common,
    )

    train = DockerOperator(
        task_id="train_and_register",
        command=[
            "train",
            "--processed-dir",
            "/app/data/processed",
            "--tracking-uri",
            TRACKING_URI,
            "--registry-name",
            "fraud-detection-classifier",
        ],
        **common,
    )

    evaluate = DockerOperator(
        task_id="evaluate",
        command=[
            "evaluate",
            "--processed-dir",
            "/app/data/processed",
            "--tracking-uri",
            TRACKING_URI,
            "--registry-name",
            "fraud-detection-classifier",
        ],
        **common,
    )

    preprocess >> train >> evaluate
