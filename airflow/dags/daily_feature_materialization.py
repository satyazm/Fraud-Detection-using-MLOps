"""Daily: extract -> validate -> preprocess -> materialize Feast.

Every task runs the shared `mloops-worker:latest` image (the same one
Kubernetes uses — docker/Dockerfile.worker) as a short-lived sibling
container via DockerOperator, on the main docker-compose.yml's network
so it can reach `kafka`/`redis` by service name — Airflow orchestrates
this project's existing CLI, it doesn't reimplement any of it.

`preprocess` and `materialize` are chained here matching the
milestone's own stated DAG shape, but note honestly: `materialize`
re-reads the raw CSV itself (feature-engineers it fresh, builds
Feast's offline source) rather than consuming `preprocess`'s
train/val/test splits — the two are independent siblings data-flow-
wise, not a producer/consumer pair. See
docs/decisions/0009-deployment-and-operations.md.

Uses the same small sample CSV as `kubernetes/jobs.yaml`'s
training-job and `weekly_retraining`, not the full ~6.4M-row file —
confirmed the hard way (see `weekly_retraining`'s docstring) that the
full file threatens the whole Docker Desktop VM this and the kind
cluster containers share, not just one task. `mem_limit` is a safety
net for the same reason.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.environ["PROJECT_ROOT"]  # see airflow/docker-compose.yml
IMAGE = "mloops-worker:latest"
NETWORK = "mloops_default"
MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/feast_repo", target="/app/feast_repo", type="bind"),
]

with DAG(
    dag_id="daily_feature_materialization",
    description="Extract -> validate -> preprocess -> materialize Feast",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "feast"],
) as dag:
    common = dict(
        image=IMAGE,
        network_mode=NETWORK,
        mounts=MOUNTS,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        mem_limit="1500m",  # matches kubernetes/jobs.yaml's training-job limit
    )

    extract = DockerOperator(
        task_id="extract_data",
        command=["ingest", "--raw-path", "/app/data/raw/paysim_k8s_sample.csv"],
        **common,
    )

    validate = DockerOperator(
        task_id="validate",
        command=[
            "validate",
            "--raw-path",
            "/app/data/raw/paysim_k8s_sample.csv",
            "--report-path",
            "/app/docs/data_report.md",
            "--images-dir",
            "/app/docs/images",
        ],
        mounts=[
            *MOUNTS,
            Mount(source=f"{PROJECT_ROOT}/docs", target="/app/docs", type="bind"),
        ],
        image=IMAGE,
        network_mode=NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        mem_limit="1500m",
    )

    preprocess = DockerOperator(
        task_id="preprocess",
        command=[
            "preprocess",
            "--raw-path",
            "/app/data/raw/paysim_k8s_sample.csv",
            "--output-dir",
            "/app/data/processed",
        ],
        **common,
    )

    materialize = DockerOperator(
        task_id="materialize",
        command=[
            "materialize",
            "--raw-path",
            "/app/data/raw/paysim_k8s_sample.csv",
            "--repo-path",
            "/app/feast_repo",
            "--sample-size",
            "5000",
        ],
        # feast_repo/feature_store.yaml's "localhost:6379" is correct for
        # host-side commands but unreachable from inside this container on
        # the compose network — same override docker-compose.yml's `api`
        # service already applies (see ADR-0007), confirmed needed here
        # the hard way (materialize failed with ConnectionRefusedError to
        # localhost:6379 without it).
        mounts=[
            *MOUNTS,
            Mount(
                source=f"{PROJECT_ROOT}/docker/feature_store.docker.yaml",
                target="/app/feast_repo/feature_store.yaml",
                type="bind",
                read_only=True,
            ),
        ],
        image=IMAGE,
        network_mode=NETWORK,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        mem_limit="1500m",
    )

    extract >> validate >> preprocess >> materialize
