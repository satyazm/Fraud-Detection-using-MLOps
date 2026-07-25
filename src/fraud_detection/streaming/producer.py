"""Kafka producer: streams PaySim transactions onto the `transactions` topic.

Reads the raw PaySim CSV, drops the `isFraud` label (not available at
inference time — real transactions arrive without a ground-truth
answer), converts each remaining row into a domain `Transaction`, and
publishes it as JSON via `serializer.serialize_transaction`. No feature
engineering or model inference here — that's the next Milestone 4
layer, reusing this exact producer/consumer pair.
"""

from __future__ import annotations

import time
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from fraud_detection.common.logger import get_logger
from fraud_detection.data.ingestion import DEFAULT_RAW_PATH, load_paysim_csv
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.domain.schemas import transaction_from_dict
from fraud_detection.streaming.serializer import serialize_transaction

logger = get_logger(__name__)

DEFAULT_TOPIC = "transactions"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
PROGRESS_LOG_INTERVAL = 100


def produce_transactions(
    raw_path: Path | str = DEFAULT_RAW_PATH,
    topic: str = DEFAULT_TOPIC,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    rate_per_second: float = 5.0,
    limit: int | None = 1000,
) -> int:
    """Stream up to `limit` rows from `raw_path` onto `topic`.

    Args:
        raw_path: PaySim CSV to read from.
        topic: Kafka topic to publish to.
        bootstrap_servers: Kafka bootstrap servers string.
        rate_per_second: Messages per second; 0 (or less) disables the
            delay and publishes as fast as the broker accepts them.
        limit: Maximum rows to stream. `None` streams the whole file —
            for the full ~6.4M-row PaySim CSV, that means "run for a
            very long time at any realistic rate," not "instant."

    Returns:
        The number of messages successfully published.
    """
    df = load_paysim_csv(raw_path)
    if limit is not None:
        df = df.head(limit)

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delay_seconds = 1.0 / rate_per_second if rate_per_second > 0 else 0.0

    sent = 0
    skipped = 0
    for raw_record in df.to_dict(orient="records"):
        record = {str(key): value for key, value in raw_record.items() if key != "isFraud"}
        try:
            transaction = transaction_from_dict(record)
        except InvalidTransactionError as exc:
            logger.warning("skipping invalid row", extra={"error": str(exc)})
            skipped += 1
            continue

        producer.produce(
            topic, value=serialize_transaction(transaction), callback=_delivery_callback
        )
        producer.poll(0)
        sent += 1

        if sent % PROGRESS_LOG_INTERVAL == 0:
            logger.info("producer progress", extra={"sent": sent, "topic": topic})

        if delay_seconds:
            time.sleep(delay_seconds)

    producer.flush(10)
    logger.info("producer finished", extra={"sent": sent, "skipped": skipped, "topic": topic})
    return sent


def _delivery_callback(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("message delivery failed", extra={"error": str(err)})
