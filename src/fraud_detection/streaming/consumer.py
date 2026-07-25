"""Kafka consumer: subscribes to the `transactions` topic and logs each
received transaction.

No feature engineering or inference yet — that's the next Milestone 4
layer. It will reuse `serializer.deserialize_transaction` unchanged and
feed the resulting `Transaction` straight into
`fraud_detection.features.feature_pipeline.FeaturePipeline.transform_one()`,
the same call an online API would make.
"""

from __future__ import annotations

from confluent_kafka import Consumer, KafkaError, KafkaException

from fraud_detection.common.logger import get_logger
from fraud_detection.domain.exceptions import InvalidTransactionError
from fraud_detection.streaming.serializer import deserialize_transaction

logger = get_logger(__name__)

DEFAULT_TOPIC = "transactions"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_GROUP_ID = "fraud-detection-consumer"
DEFAULT_POLL_TIMEOUT_SECONDS = 1.0


def consume_transactions(
    topic: str = DEFAULT_TOPIC,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int | None = None,
    poll_timeout_seconds: float = DEFAULT_POLL_TIMEOUT_SECONDS,
) -> int:
    """Consume from `topic`, logging each valid transaction.

    Args:
        max_messages: Stop after this many successfully processed
            transactions. `None` runs until interrupted (Ctrl+C) —
            the normal mode for a long-lived streaming consumer.

    Returns:
        The number of transactions successfully processed.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])

    processed = 0
    skipped = 0
    try:
        while max_messages is None or processed < max_messages:
            msg = consumer.poll(poll_timeout_seconds)
            if msg is None:
                continue

            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)

            value = msg.value()
            if value is None:
                continue

            try:
                transaction = deserialize_transaction(value)
            except InvalidTransactionError as exc:
                logger.warning("skipping invalid message", extra={"error": str(exc)})
                skipped += 1
                continue

            logger.info(
                "received transaction",
                extra={
                    "type": transaction.type.value,
                    "amount": transaction.amount,
                    "name_orig": transaction.name_orig,
                    "name_dest": transaction.name_dest,
                    "step": transaction.step,
                },
            )
            processed += 1
    except KeyboardInterrupt:
        logger.info("consumer interrupted")
    finally:
        consumer.close()

    logger.info(
        "consumer finished", extra={"processed": processed, "skipped": skipped, "topic": topic}
    )
    return processed
