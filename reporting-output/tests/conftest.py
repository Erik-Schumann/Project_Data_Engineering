"""Shared fixtures for reporting-output's integration tests. These need the
real docker-compose stack running: Kafka, Schema Registry, Kafka Connect,
and reporting-db, all reachable on their host-mapped localhost ports (see
../../.env for KAFKA_EXTERNAL_PORT/SCHEMA_REGISTRY_PORT/CONNECT_PORT/
REPORTING_DB_PORT). Run `docker compose up -d` from the repo root first.
"""
import os
import time

import psycopg2
import psycopg2.extras
import pytest
import requests
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA_BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("TEST_SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECT_URL = os.environ.get("TEST_CONNECT_URL", "http://localhost:8083")
REPORTING_DB_DSN = os.environ.get(
    "TEST_REPORTING_DB_DSN",
    "host=localhost port=5433 dbname=reporting user=reporting password=reporting",
)

# de.iu.Rating.V002's schemas are hand-authored under client-input/schemas/
# (client-input is the producer that owns them) - reused here rather than
# duplicated, the same relationship catalog-input/frontend has with
# seed_catalog.py living under seed/.
SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "client-input", "schemas")


def _read_schema(filename):
    # utf-8-sig: same Windows-BOM gotcha generator.py's _load_schema and
    # seed_catalog.py's _load_schema already document.
    with open(os.path.join(SCHEMAS_DIR, filename), encoding="utf-8-sig") as f:
        return f.read()


@pytest.fixture(scope="session", autouse=True)
def require_live_stack():
    """Every test in this package is integration-only - fail fast with a
    clear message instead of a confusing connection-refused deep inside a
    test if the stack isn't up."""
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        pytest.exit(
            f"Kafka Connect not reachable at {CONNECT_URL} - these are integration "
            f"tests and need the real stack. Run `docker compose up -d` from the "
            f"repo root first. ({exc})",
            returncode=1,
        )


@pytest.fixture(scope="session")
def rating_producer():
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer": AvroSerializer(registry, _read_schema("de.iu.Rating.V002-key.avsc")),
        "value.serializer": AvroSerializer(registry, _read_schema("de.iu.Rating.V002-value.avsc")),
    })
    yield producer
    producer.flush(10)


@pytest.fixture
def raw_consumer():
    """A plain (non-Avro) Consumer - used for watermark-offset checks on
    DLQ topics, where we care whether *something* new landed, not about
    decoding the payload."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"integration-test-{os.getpid()}-{time.time_ns()}",
        "enable.auto.commit": False,
    })
    yield consumer
    consumer.close()


@pytest.fixture
def reporting_db():
    conn = psycopg2.connect(REPORTING_DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    yield conn
    conn.close()


def connector_status(name):
    resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)
    resp.raise_for_status()
    return resp.json()


def topic_total_offset(consumer, topic):
    """Sum of high-watermark offsets across every partition - a simple
    "how many messages have ever been produced to this topic" proxy, good
    enough to detect "something new landed" without decoding Avro."""
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = metadata.topics[topic].partitions
    total = 0
    for partition_id in partitions:
        from confluent_kafka import TopicPartition
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10)
        total += high
    return total


def wait_until(predicate, timeout_s=30, interval_s=0.5):
    """Polls predicate() until it returns truthy or timeout_s elapses,
    returning the last (falsy) result on timeout rather than raising - lets
    callers assert on it with a clear failure message instead of a bare
    TimeoutError."""
    deadline = time.monotonic() + timeout_s
    result = predicate()
    while not result and time.monotonic() < deadline:
        time.sleep(interval_s)
        result = predicate()
    return result
