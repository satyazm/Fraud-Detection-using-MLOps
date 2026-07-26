"""Command-line entry point for the fraud detection platform.

`ingest`/`validate`/`preprocess` run the data pipeline (Milestone 2).
`train`/`evaluate` run model training/evaluation with MLflow tracking
(Milestone 3). `producer`/`consumer` stream/log PaySim transactions via
Kafka (Milestone 4; no inference yet). `feast-apply`/`materialize`/
`flink-worker` run the real-time feature platform (Milestone 5):
register Feast definitions, push offline features into Redis, and run
the PyFlink job that computes features from the live Kafka stream.
`api`/`ready` run and probe the real-time inference service (Milestone
6): Feast online features -> MLflow Production model -> fraud
probability. `drift-report` (Milestone 7) compares that training data
against real logged `/predict` requests via Evidently AI.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import mlflow.sklearn
from mlflow.exceptions import MlflowException

from fraud_detection.common.logger import get_logger
from fraud_detection.data.exceptions import DataError
from fraud_detection.data.ingestion import DEFAULT_RAW_PATH, load_paysim_csv
from fraud_detection.data.preprocessing import preprocess
from fraud_detection.data.reporting import (
    DEFAULT_IMAGES_DIR,
    DEFAULT_REPORT_PATH,
    generate_plots,
    render_markdown_report,
)
from fraud_detection.data.split import stratified_split
from fraud_detection.data.validation import assert_trainable, run_data_quality_checks
from fraud_detection.features.feast_ops import (
    DEFAULT_FEAST_REPO_PATH,
    FeastOperationError,
    apply_feast_definitions,
    materialize_feast_features,
)
from fraud_detection.features.feast_prep import (
    DEFAULT_BASE_TIMESTAMP,
    DEFAULT_OFFLINE_SOURCE_PATH,
    build_offline_source,
)
from fraud_detection.features.feature_pipeline import DEFAULT_FEATURE_PIPELINE
from fraud_detection.features.registry import feature_version as compute_feature_version
from fraud_detection.models.dataset import DEFAULT_PROCESSED_DIR, load_dataset
from fraud_detection.models.evaluation import evaluate_predictions
from fraud_detection.models.exceptions import ModelError
from fraud_detection.models.model_registry import (
    DEFAULT_MODEL_NAME,
    register_model,
    resolve_latest_model_uri,
)
from fraud_detection.models.reporting import (
    DEFAULT_MODEL_REPORT_PATH,
    plot_model_comparison,
    render_model_report,
)
from fraud_detection.models.training import (
    DEFAULT_EXPERIMENT_NAME,
    DEFAULT_TRACKING_URI,
    get_git_commit_hash,
    select_best_run,
    train_and_compare,
)
from fraud_detection.monitoring.drift import (
    DEFAULT_REFERENCE_SAMPLE_SIZE,
    DriftReportError,
    generate_drift_report,
    load_reference_sample,
)
from fraud_detection.monitoring.drift import DEFAULT_REPORT_PATH as DEFAULT_DRIFT_REPORT_PATH
from fraud_detection.monitoring.prediction_log import (
    DEFAULT_LOG_PATH,
    PredictionLogError,
    load_predictions,
)
from fraud_detection.streaming.consumer import (
    DEFAULT_GROUP_ID,
    consume_transactions,
)
from fraud_detection.streaming.flink_job import (
    DEFAULT_GROUP_ID as DEFAULT_FLINK_GROUP_ID,
)
from fraud_detection.streaming.flink_job import (
    DEFAULT_KAFKA_CONNECTOR_JAR,
    run_flink_worker,
)
from fraud_detection.streaming.producer import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_TOPIC,
    produce_transactions,
)

logger = get_logger("fraud_detection.cli")

DEFAULT_API_HOST = "0.0.0.0"
DEFAULT_API_PORT = 8000


def _cmd_ingest(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    logger.info("ingestion succeeded", extra={"rows": len(df), "columns": len(df.columns)})
    return 0


def _cmd_validate(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    report = run_data_quality_checks(df)
    image_paths = generate_plots(df, output_dir=args.images_dir)
    report_path = render_markdown_report(report, output_path=args.report_path)

    logger.info(
        "validation report generated",
        extra={
            "report_path": str(report_path),
            "image_count": len(image_paths),
            "fraud_percentage": report.fraud_percentage,
        },
    )
    return 0


def _cmd_preprocess(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    report = run_data_quality_checks(df)
    try:
        assert_trainable(report)
    except DataError as exc:
        logger.error("dataset failed trainability check", extra={"error": str(exc)})
        return 1

    # Feature engineering runs before preprocessing: some features (e.g.
    # is_dest_merchant) read nameDest, which preprocess() drops. See
    # docs/decisions/0003-shared-feature-pipeline.md.
    featurized_df = DEFAULT_FEATURE_PIPELINE.transform(df)
    processed_df = preprocess(featurized_df)
    split = stratified_split(processed_df)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for split_name, subset in split._asdict().items():
        split_path = output_dir / f"{split_name}.parquet"
        subset.to_parquet(split_path, index=False)
        logger.info(
            "saved processed split",
            extra={"split": split_name, "rows": len(subset), "path": str(split_path)},
        )

    return 0


def _cmd_train(args: argparse.Namespace) -> int:
    try:
        dataset = load_dataset(args.processed_dir)
    except ModelError as exc:
        logger.error("failed to load dataset", extra={"error": str(exc)})
        return 1

    results = train_and_compare(
        dataset,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
    )
    best = select_best_run(results)

    try:
        version = register_model(best.run_id, model_name=args.registry_name)
    except MlflowException as exc:
        logger.error("failed to register best model", extra={"error": str(exc)})
        return 1

    plot_model_comparison(results, output_path=args.images_dir / "model_comparison.png")
    report_path = render_model_report(
        results,
        best,
        feature_version=compute_feature_version(),
        git_commit=get_git_commit_hash(),
        registered_model_name=args.registry_name,
        registered_model_version=str(version.version),
        output_path=args.report_path,
    )

    logger.info(
        "training complete",
        extra={
            "best_model": best.model_name,
            "registered_version": version.version,
            "report_path": str(report_path),
        },
    )
    return 0


def _cmd_evaluate(args: argparse.Namespace) -> int:
    try:
        dataset = load_dataset(args.processed_dir)
    except ModelError as exc:
        logger.error("failed to load dataset", extra={"error": str(exc)})
        return 1

    mlflow.set_tracking_uri(args.tracking_uri)

    model_uri = args.model_uri
    if model_uri is None:
        try:
            model_uri = resolve_latest_model_uri(args.registry_name)
        except ModelError as exc:
            logger.error("failed to resolve model uri", extra={"error": str(exc)})
            return 1

    try:
        model = mlflow.sklearn.load_model(model_uri)
    except (MlflowException, OSError) as exc:
        logger.error("failed to load model", extra={"model_uri": model_uri, "error": str(exc)})
        return 1

    predictions = model.predict(dataset.x_test)
    probabilities = model.predict_proba(dataset.x_test)[:, 1]
    metrics = evaluate_predictions(dataset.y_test, predictions, probabilities)

    logger.info(
        "evaluation complete",
        extra={"model_uri": model_uri, **metrics.as_metrics_dict()},
    )
    return 0


def _cmd_producer(args: argparse.Namespace) -> int:
    limit = None if args.limit == 0 else args.limit
    sent = produce_transactions(
        raw_path=args.raw_path,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        rate_per_second=args.rate,
        limit=limit,
    )
    logger.info("producer command complete", extra={"sent": sent})
    return 0


def _cmd_consumer(args: argparse.Namespace) -> int:
    processed = consume_transactions(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        max_messages=args.max_messages,
    )
    logger.info("consumer command complete", extra={"processed": processed})
    return 0


def _cmd_feast_apply(args: argparse.Namespace) -> int:
    try:
        apply_feast_definitions(args.repo_path)
    except FeastOperationError as exc:
        logger.error("feast apply failed", extra={"error": str(exc)})
        return 1

    logger.info("feast apply complete", extra={"repo_path": str(args.repo_path)})
    return 0


def _cmd_materialize(args: argparse.Namespace) -> int:
    try:
        df = load_paysim_csv(args.raw_path)
    except DataError as exc:
        logger.error("ingestion failed", extra={"error": str(exc)})
        return 1

    sample = df.head(args.sample_size)
    featurized = DEFAULT_FEATURE_PIPELINE.transform(sample)
    # Always DEFAULT_OFFLINE_SOURCE_PATH, matching the fixed FileSource
    # path feast_repo/definitions.py registers — `feast materialize`
    # always reads from there, so this can't be independently overridden
    # via a CLI flag without the two silently going out of sync.
    offline_path = build_offline_source(featurized, output_path=DEFAULT_OFFLINE_SOURCE_PATH)

    try:
        apply_feast_definitions(args.repo_path)
    except FeastOperationError as exc:
        logger.error("feast apply failed", extra={"error": str(exc)})
        return 1

    start = DEFAULT_BASE_TIMESTAMP
    end = start + timedelta(hours=int(sample["step"].max()) + 1)
    try:
        materialize_feast_features(start, end, args.repo_path)
    except FeastOperationError as exc:
        logger.error("feast materialize failed", extra={"error": str(exc)})
        return 1

    logger.info(
        "materialize complete",
        extra={
            "offline_source_path": str(offline_path),
            "rows": len(sample),
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    )
    return 0


def _cmd_flink_worker(args: argparse.Namespace) -> int:
    try:
        run_flink_worker(
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            repo_path=args.repo_path,
            # Not CLI-overridable: unused by the online push path
            # (write_online never reads it), and if it *were* used it
            # would face the same fixed-FileSource issue materialize
            # has — see the comment in _cmd_materialize.
            offline_source_path=DEFAULT_OFFLINE_SOURCE_PATH,
            kafka_connector_jar=args.kafka_connector_jar,
            bounded=args.bounded,
        )
    except (FileNotFoundError, RuntimeError) as exc:
        logger.error("flink worker failed", extra={"error": str(exc)})
        return 1

    return 0


def _cmd_api(args: argparse.Namespace) -> int:
    import uvicorn

    from fraud_detection.api.app import create_app

    app = create_app(
        model_name=args.registry_name,
        tracking_uri=args.tracking_uri,
        prediction_log_path=args.prediction_log_path,
    )
    # log_config=None: skip uvicorn's own logging setup so "uvicorn"/
    # "uvicorn.access" loggers fall back to propagating into the root
    # logger this process already configured (configs/logging.yaml),
    # instead of uvicorn installing a second, differently formatted
    # (non-JSON) console handler.
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)
    return 0


def _cmd_ready(args: argparse.Namespace) -> int:
    url = f"http://{args.host}:{args.port}/ready"
    try:
        with urllib.request.urlopen(url, timeout=args.timeout) as response:
            body = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError) as exc:
        logger.error(
            "readiness check failed: could not reach the API", extra={"url": url, "error": str(exc)}
        )
        return 1

    logger.info("readiness check", extra={"url": url, **body})
    return 0 if body.get("ready") else 1


def _cmd_drift_report(args: argparse.Namespace) -> int:
    try:
        reference = load_reference_sample(args.raw_path, args.reference_sample_size)
    except DataError as exc:
        logger.error(
            "drift report failed: could not load reference data", extra={"error": str(exc)}
        )
        return 1

    try:
        current = load_predictions(args.log_path)
    except PredictionLogError as exc:
        logger.error(
            "drift report failed: no live predictions to compare against", extra={"error": str(exc)}
        )
        return 1

    try:
        summary = generate_drift_report(reference, current, output_path=args.output_path)
    except DriftReportError as exc:
        logger.error("drift report failed", extra={"error": str(exc)})
        return 1

    logger.info(
        "drift report command complete",
        extra={
            "report_path": str(summary.report_path),
            "drifted_columns": summary.drifted_columns,
            "total_columns": summary.total_columns,
            "drift_share": summary.drift_share,
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fraud-detection", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser(
        "ingest", help="Load and schema-validate the raw PaySim CSV"
    )
    ingest_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    ingest_parser.set_defaults(func=_cmd_ingest)

    validate_parser = subparsers.add_parser(
        "validate", help="Generate the data quality report and diagnostic plots"
    )
    validate_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    validate_parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    validate_parser.add_argument("--images-dir", type=Path, default=DEFAULT_IMAGES_DIR)
    validate_parser.set_defaults(func=_cmd_validate)

    preprocess_parser = subparsers.add_parser(
        "preprocess", help="Feature-engineer, clean, stratified-split, and save the dataset"
    )
    preprocess_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    preprocess_parser.add_argument("--output-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    preprocess_parser.set_defaults(func=_cmd_preprocess)

    train_parser = subparsers.add_parser(
        "train", help="Train and compare candidate models, log to MLflow, register the best"
    )
    train_parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    train_parser.add_argument("--tracking-uri", type=str, default=DEFAULT_TRACKING_URI)
    train_parser.add_argument("--experiment-name", type=str, default=DEFAULT_EXPERIMENT_NAME)
    train_parser.add_argument("--registry-name", type=str, default=DEFAULT_MODEL_NAME)
    train_parser.add_argument("--report-path", type=Path, default=DEFAULT_MODEL_REPORT_PATH)
    train_parser.add_argument("--images-dir", type=Path, default=DEFAULT_IMAGES_DIR)
    train_parser.set_defaults(func=_cmd_train)

    evaluate_parser = subparsers.add_parser(
        "evaluate", help="Evaluate a registered (or explicit) model against the test split"
    )
    evaluate_parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    evaluate_parser.add_argument("--tracking-uri", type=str, default=DEFAULT_TRACKING_URI)
    evaluate_parser.add_argument("--registry-name", type=str, default=DEFAULT_MODEL_NAME)
    evaluate_parser.add_argument(
        "--model-uri",
        type=str,
        default=None,
        help="Explicit MLflow model URI; defaults to the latest registered version",
    )
    evaluate_parser.set_defaults(func=_cmd_evaluate)

    producer_parser = subparsers.add_parser(
        "producer", help="Stream PaySim transactions onto a Kafka topic"
    )
    producer_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    producer_parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    producer_parser.add_argument("--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP_SERVERS)
    producer_parser.add_argument(
        "--rate", type=float, default=5.0, help="Messages per second (<=0 for no delay)"
    )
    producer_parser.add_argument(
        "--limit", type=int, default=1000, help="Max rows to stream; 0 means unlimited"
    )
    producer_parser.set_defaults(func=_cmd_producer)

    consumer_parser = subparsers.add_parser(
        "consumer", help="Consume and log PaySim transactions from Kafka"
    )
    consumer_parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    consumer_parser.add_argument("--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP_SERVERS)
    consumer_parser.add_argument("--group-id", type=str, default=DEFAULT_GROUP_ID)
    consumer_parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after N messages; omit to run until interrupted",
    )
    consumer_parser.set_defaults(func=_cmd_consumer)

    feast_apply_parser = subparsers.add_parser(
        "feast-apply", help="Register Feast entity/feature-view definitions (`feast apply`)"
    )
    feast_apply_parser.add_argument("--repo-path", type=Path, default=DEFAULT_FEAST_REPO_PATH)
    feast_apply_parser.set_defaults(func=_cmd_feast_apply)

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="Build the offline feature source and materialize it into Redis via Feast",
    )
    materialize_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    materialize_parser.add_argument("--repo-path", type=Path, default=DEFAULT_FEAST_REPO_PATH)
    materialize_parser.add_argument(
        "--sample-size",
        type=int,
        default=2000,
        help="Rows to feature-engineer and materialize (file offline store is dev-scale)",
    )
    materialize_parser.set_defaults(func=_cmd_materialize)

    flink_worker_parser = subparsers.add_parser(
        "flink-worker",
        help="Kafka -> FeaturePipeline.transform_one() -> Feast/Redis (PyFlink streaming job)",
    )
    flink_worker_parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    flink_worker_parser.add_argument(
        "--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP_SERVERS
    )
    flink_worker_parser.add_argument("--group-id", type=str, default=DEFAULT_FLINK_GROUP_ID)
    flink_worker_parser.add_argument("--repo-path", type=Path, default=DEFAULT_FEAST_REPO_PATH)
    flink_worker_parser.add_argument(
        "--kafka-connector-jar", type=Path, default=DEFAULT_KAFKA_CONNECTOR_JAR
    )
    flink_worker_parser.add_argument(
        "--bounded",
        action="store_true",
        help="Stop at the latest offset when the job starts, instead of streaming forever",
    )
    flink_worker_parser.set_defaults(func=_cmd_flink_worker)

    api_parser = subparsers.add_parser(
        "api",
        help="Run the FastAPI inference service (Feast features -> MLflow Production model)",
    )
    api_parser.add_argument("--host", type=str, default=DEFAULT_API_HOST)
    api_parser.add_argument("--port", type=int, default=DEFAULT_API_PORT)
    api_parser.add_argument("--registry-name", type=str, default=DEFAULT_MODEL_NAME)
    api_parser.add_argument("--tracking-uri", type=str, default=DEFAULT_TRACKING_URI)
    api_parser.add_argument("--prediction-log-path", type=Path, default=DEFAULT_LOG_PATH)
    api_parser.set_defaults(func=_cmd_api)

    ready_parser = subparsers.add_parser(
        "ready", help="Probe a running `api` instance's /ready endpoint"
    )
    ready_parser.add_argument("--host", type=str, default="localhost")
    ready_parser.add_argument("--port", type=int, default=DEFAULT_API_PORT)
    ready_parser.add_argument("--timeout", type=float, default=5.0)
    ready_parser.set_defaults(func=_cmd_ready)

    drift_report_parser = subparsers.add_parser(
        "drift-report",
        help="Evidently AI data-drift report: training data vs. real logged /predict requests",
    )
    drift_report_parser.add_argument("--raw-path", type=Path, default=DEFAULT_RAW_PATH)
    drift_report_parser.add_argument(
        "--reference-sample-size", type=int, default=DEFAULT_REFERENCE_SAMPLE_SIZE
    )
    drift_report_parser.add_argument("--log-path", type=Path, default=DEFAULT_LOG_PATH)
    drift_report_parser.add_argument("--output-path", type=Path, default=DEFAULT_DRIFT_REPORT_PATH)
    drift_report_parser.set_defaults(func=_cmd_drift_report)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result: int = args.func(args)
    return result


if __name__ == "__main__":
    sys.exit(main())
