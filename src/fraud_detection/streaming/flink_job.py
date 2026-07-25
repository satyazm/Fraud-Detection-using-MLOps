"""PyFlink streaming job: Kafka -> deserialize -> transform_one() -> Feast/Redis.

    Kafka Consumer
        |
        v
    deserialize_transaction()      (streaming.serializer — same as consumer.py)
        |
        v
    FeaturePipeline.transform_one() (features.feature_pipeline — same as offline training)
        |
        v
    FeastFeatureStore.write_online() (features.feast_store — Feast push API -> Redis)

No feature logic lives here. `_ComputeAndPushFeatures.map()` imports
and calls the exact same functions the offline pipeline and the plain
Kafka consumer already use — this module is wiring, not a second
implementation. See docs/decisions/0006-feast-redis-flink.md for why
this runs as a local-execution-mode PyFlink job (an embedded
mini-cluster via the JVM gateway PyFlink launches in-process) rather
than submitting to a separate Flink cluster.
"""

from __future__ import annotations

from pathlib import Path

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import MapFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from fraud_detection.common.config import PROJECT_ROOT
from fraud_detection.common.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TOPIC = "transactions"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_GROUP_ID = "fraud-detection-flink-worker"
DEFAULT_FEAST_REPO_PATH = PROJECT_ROOT / "feast_repo"
DEFAULT_OFFLINE_SOURCE_PATH = PROJECT_ROOT / "data" / "feast" / "transaction_features.parquet"
DEFAULT_KAFKA_CONNECTOR_JAR = (
    PROJECT_ROOT / ".flink-jars" / "flink-sql-connector-kafka-5.0.0-2.2.jar"
)


class _ComputeAndPushFeatures(MapFunction):  # type: ignore[misc]  # MapFunction is Any (no pyflink stubs)
    """Per-record: deserialize, compute features, push to Feast/Redis.

    A stateful `MapFunction` (not a plain lambda) so the Feast client
    and FeaturePipeline are constructed once per worker in `open()`,
    not once per record — reconnecting to Redis for every message
    would be both slow and wasteful. PyFlink runs this in a separate
    Python worker process (not the driver), so all `fraud_detection`
    imports happen inside these methods, not at module scope.
    """

    def __init__(self, repo_path: str, offline_source_path: str) -> None:
        self._repo_path = repo_path
        self._offline_source_path = offline_source_path
        self._store: object = None
        self._pipeline: object = None

    def open(self, runtime_context: object) -> None:
        from fraud_detection.features.feast_store import FeastFeatureStore
        from fraud_detection.features.feature_pipeline import FeaturePipeline

        self._store = FeastFeatureStore(
            repo_path=self._repo_path,
            offline_source_path=self._offline_source_path,
        )
        self._pipeline = FeaturePipeline()

    def map(self, value: str) -> str:
        from fraud_detection.domain.exceptions import InvalidTransactionError
        from fraud_detection.features.entity_key import compute_entity_id
        from fraud_detection.streaming.serializer import deserialize_transaction

        try:
            transaction = deserialize_transaction(value.encode("utf-8"))
        except InvalidTransactionError as exc:
            return f"SKIPPED invalid message: {exc}"

        features = self._pipeline.transform_one(transaction)  # type: ignore[attr-defined]
        entity_id = compute_entity_id(transaction)
        self._store.write_online(entity_id, features)  # type: ignore[attr-defined]

        return f"OK entity_id={entity_id} name_orig={transaction.name_orig}"


def run_flink_worker(
    topic: str = DEFAULT_TOPIC,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    group_id: str = DEFAULT_GROUP_ID,
    repo_path: Path | str = DEFAULT_FEAST_REPO_PATH,
    offline_source_path: Path | str = DEFAULT_OFFLINE_SOURCE_PATH,
    kafka_connector_jar: Path | str = DEFAULT_KAFKA_CONNECTOR_JAR,
    bounded: bool = False,
) -> None:
    """Run the streaming feature-computation job.

    Args:
        bounded: If True, the Kafka source stops at whatever offset was
            latest when the job started (a snapshot), so the job
            terminates instead of streaming forever. Production usage
            leaves this False; tests set it True so they don't hang —
            see tests/streaming/test_flink_job.py.
    """
    jar_path = Path(kafka_connector_jar).resolve()
    if not jar_path.exists():
        raise FileNotFoundError(
            f"Flink Kafka connector JAR not found at {jar_path}. Run `make flink-jar` first."
        )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{jar_path}")

    source_builder = (
        KafkaSource.builder()
        .set_topics(topic)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_properties({"bootstrap.servers": bootstrap_servers, "group.id": group_id})
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    )
    if bounded:
        source_builder = source_builder.set_bounded(KafkaOffsetsInitializer.latest())
    kafka_source = source_builder.build()

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-transactions",
    )

    stream.map(_ComputeAndPushFeatures(str(repo_path), str(offline_source_path))).print()

    logger.info(
        "starting flink worker",
        extra={"topic": topic, "bootstrap_servers": bootstrap_servers, "bounded": bounded},
    )
    env.execute("fraud-detection-flink-worker")
