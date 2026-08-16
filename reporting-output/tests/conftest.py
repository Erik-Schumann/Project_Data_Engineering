"""Shared fixtures for reporting-output's integration tests. These need the
real docker-compose stack running: Kafka, Schema Registry, Kafka Connect,
and reporting-db, all reachable on their host-mapped localhost ports (see
../../.env for KAFKA_EXTERNAL_PORT/SCHEMA_REGISTRY_PORT/CONNECT_PORT/
REPORTING_DB_PORT). Run `docker compose up -d` from the repo root first.
"""
import importlib.util
import os
import pathlib
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

# The 10 sql/*/job.py modules build their own PG_DSN from REPORTING_DB_HOST/
# PORT/NAME/USER/PASSWORD at *import* time (e.g. sql/user-rating-avg/job.py) -
# a distinct set of env vars from TEST_REPORTING_DB_DSN above, which only
# this file's own reporting_db fixture uses. Set once here, before
# test_sql_jobs_integration.py ever calls load_job() below, so every job
# module resolves to the same host-mapped localhost port everything else in
# this file already uses.
os.environ.setdefault("REPORTING_DB_HOST", "localhost")
os.environ.setdefault("REPORTING_DB_PORT", "5433")
os.environ.setdefault("REPORTING_DB_NAME", "reporting")
os.environ.setdefault("REPORTING_DB_USER", "reporting")
os.environ.setdefault("REPORTING_DB_PASSWORD", "reporting")

SQL_JOBS_DIR = pathlib.Path(__file__).resolve().parent.parent / "sql"


def load_job(name):
    """Imports sql/<name>/job.py (e.g. "user-rating-avg") as its own
    module, keyed by a unique name - every job file is literally named
    job.py, so a plain `import job` would collide the moment a second one
    loaded into sys.modules. A fresh exec_module() per call (not cached)
    also means it re-reads REPORTING_DB_HOST/etc fresh each time, so a
    test's monkeypatch.setenv (e.g. overriding TRENDING_WINDOW_MINUTES)
    takes effect as long as it runs before load_job()."""
    path = SQL_JOBS_DIR / name / "job.py"
    spec = importlib.util.spec_from_file_location(f"reporting_job_{name.replace('-', '_')}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

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


@pytest.fixture(scope="session")
def require_live_stack():
    """Fail fast with a clear message instead of a confusing connection-
    refused deep inside a test if the stack isn't up. Not autouse: only
    test_connector_dlq_integration.py needs the full stack (Kafka, Schema
    Registry, Connect) - test_sql_jobs_integration.py only needs
    reporting-db itself (see require_reporting_db below), and gating it on
    Connect too would make it fail even when all it actually needs is up."""
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
def require_reporting_db():
    """test_sql_jobs_integration.py's equivalent of require_live_stack
    above - reporting-db only, since those tests write straight to
    items/users/ratings/watch_events and never touch Kafka/Connect at
    all."""
    try:
        conn = psycopg2.connect(REPORTING_DB_DSN, connect_timeout=5)
        conn.close()
    except Exception as exc:
        pytest.exit(
            f"reporting-db not reachable at {REPORTING_DB_DSN!r} - these are "
            f"integration tests and need it running. Run `docker compose up -d "
            f"reporting-db` from the repo root first. ({exc})",
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
