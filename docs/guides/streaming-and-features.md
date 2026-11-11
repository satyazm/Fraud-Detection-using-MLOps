# Streaming & real-time features

## Kafka streaming

`docker-compose.yml` runs a single-node KRaft Kafka broker and
[Kafka UI](https://github.com/provectus/kafka-ui) at
[localhost:8080](http://localhost:8080). Start the broker and check its
health before producing messages:

```bash
make kafka-up
docker compose ps kafka
```

In separate terminals, start the consumer and then the producer:

```bash
fraud-detection consumer --topic transactions
fraud-detection producer --topic transactions --rate 5 --limit 200
```

The producer removes the historical `isFraud` label and sends JSON
serialized from the shared `Transaction` entity. This consumer only
logs messages; the PyFlink worker below computes live features. The
single-broker replication settings and KRaft cluster ID are explained
in [docker-compose.yml](../../docker-compose.yml) and [ADR-0005](../decisions/0005-reuse-domain-entity-for-kafka-messages.md).

## Real-time feature platform (Feast + Redis + Flink)

The PyFlink worker reads Kafka transactions and calls
`FeaturePipeline.transform_one()`, which delegates to the batch
`transform()` method. It pushes the nine engineered features through
Feast into Redis. For batch materialization, `materialize` computes
features from a PaySim sample,
writes `data/feast/transaction_features.parquet`, and runs Feast
materialization. Both paths use the same feature definitions. See
[ADR-0006](../decisions/0006-feast-redis-flink.md).

### One-time setup

```bash
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"   # add to your shell profile
export PATH="$JAVA_HOME/bin:$PATH"
make flink-jar   # download the Flink Kafka connector JAR
```

### Run the feature pipeline

```bash
make infra-up       # Kafka and Redis
make feast-apply    # register Feast definitions
make materialize    # populate Redis from the offline source
make flink-worker   # continuously process Kafka transactions
```

In another terminal, produce transactions:

```bash
fraud-detection producer --topic transactions --rate 5 --limit 200
```

The worker prints an `entity_id` for each processed transaction.
Verify an online lookup with:

```python
from fraud_detection.features.feast_store import FeastFeatureStore
from fraud_detection.features.feast_ops import DEFAULT_FEAST_REPO_PATH
from fraud_detection.features.feast_prep import DEFAULT_OFFLINE_SOURCE_PATH

store = FeastFeatureStore(DEFAULT_FEAST_REPO_PATH, DEFAULT_OFFLINE_SOURCE_PATH)
store.read_online("<entity_id from the flink-worker log line>")
```

`fraud-detection flink-worker --bounded` stops at the latest Kafka
offset present when the job starts. Redis/Kafka/Flink integration tests
run when those services, a JDK, and the connector JAR are available;
otherwise they skip. Use `make infra-up`, `make flink-jar`, then
`make test` to run them.

---

[← Back to the main README](../../README.md) · [Documentation index](../README.md)
