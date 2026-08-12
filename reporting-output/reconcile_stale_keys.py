"""One-off/occasional maintenance script, not a running service. Rating/
Watch.Event staleness (an item/user deleted normally, after ratings/watch
events already reference it) is a different scenario, handled a different
way now: reporting-db's own `ratings`/`watch_events` tables carry
`FOREIGN KEY ... ON DELETE CASCADE` back to `items`/`users`
(reporting-output/postgres/init/01_schema.sql), so that cleanup happens
automatically, at the source, the instant the referenced item/user row is
actually deleted here - see reporting-output/README.md's "Retention"
section. This script exists for the case that mechanism can't reach: a
catalog-db volume reset, where no tombstone is ever produced in the first
place, so `reporting-db.items`/`users` never gets deleted from either.

catalog-db's `item`/`app_user` tables are the actual source of truth, but
Postgres logical replication (pgoutput, what Debezium uses) never emits
row-level delete events for a `TRUNCATE` or a full volume reset - only for
row-by-row `DELETE`s. If catalog-db ever gets reset that way, the old rows'
keys are stuck in the compacted de.iu.Item.V001/de.iu.User.V001 topics
forever, with no natural way for them to expire - and every downstream
consumer (reporting-db via the JDBC sink connectors, client-input-db's
items/users mirror) inherits that staleness.

Fix: diff catalog-db's current key set against reporting-db's (a live,
queryable proxy for "every key currently materialized from the topics")
and publish a tombstone straight to Kafka for every orphaned key. That's
the same mechanism catalog-db's own row deletes already use downstream
(see client-input/README.md's "tombstones evict the key"), so this cleans
every consumer at once rather than patching reporting-db in isolation.

Run once via a throwaway container on the de-realtime_de-net network, e.g.:

    docker run --rm --network de-realtime_de-net \
      -v "$(pwd)/reporting-output/reconcile_stale_keys.py:/app/script.py:ro" \
      -v "$(pwd)/catalog-input/schemas:/app/schemas:ro" \
      -e CATALOG_DB_HOST=catalog-db -e CATALOG_DB_NAME=catalog \
      -e CATALOG_DB_USER=catalog -e CATALOG_DB_PASSWORD=catalog \
      -e REPORTING_DB_HOST=reporting-db -e REPORTING_DB_NAME=reporting \
      -e REPORTING_DB_USER=reporting -e REPORTING_DB_PASSWORD=reporting \
      -e KAFKA_BOOTSTRAP=kafka:29092 -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
      python:3.12-slim sh -c "pip install --quiet confluent-kafka psycopg2-binary && python /app/script.py"
"""
import os

import psycopg2
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
SCHEMAS_DIR = os.environ.get("SCHEMAS_DIR", "/app/schemas")


def _load_schema(filename: str) -> str:
    # utf-8-sig: catalog-input/schemas/*.avsc carry a UTF-8 BOM (Windows-
    # authored), which breaks json.loads under a plain utf-8 read.
    with open(os.path.join(SCHEMAS_DIR, filename), encoding="utf-8-sig") as f:
        return f.read()


def _pg_connect(prefix: str):
    return psycopg2.connect(
        host=os.environ[f"{prefix}_HOST"],
        dbname=os.environ[f"{prefix}_NAME"],
        user=os.environ[f"{prefix}_USER"],
        password=os.environ[f"{prefix}_PASSWORD"],
    )


def _fetch_ids(prefix: str, table: str, column: str) -> set[str]:
    with _pg_connect(prefix) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {column} FROM {table}")
        return {row[0] for row in cur.fetchall()}


def reconcile(entity: str, topic: str, key_field: str, key_schema_file: str,
              catalog_table: str, reporting_table: str) -> int:
    catalog_ids = _fetch_ids("CATALOG_DB", catalog_table, key_field)
    reporting_ids = _fetch_ids("REPORTING_DB", reporting_table, key_field)
    stale = reporting_ids - catalog_ids
    print(f"{entity}: catalog-db has {len(catalog_ids)}, reporting-db has "
          f"{len(reporting_ids)}, {len(stale)} stale key(s) to tombstone")
    if not stale:
        return 0

    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer": AvroSerializer(registry, _load_schema(key_schema_file)),
    })
    for key_value in stale:
        producer.produce(topic=topic, key={key_field: key_value}, value=None)
    producer.flush(30)
    return len(stale)


def main() -> None:
    items = reconcile("items", "de.iu.Item.V001", "item_id",
                       "de.iu.Item.V001-key.avsc", "item", "items")
    users = reconcile("users", "de.iu.User.V001", "user_id",
                       "de.iu.User.V001-key.avsc", "app_user", "users")
    print(f"Done - tombstoned {items} item key(s), {users} user key(s).")


if __name__ == "__main__":
    main()
