#!/usr/bin/env bash
# Creates all Kafka topics for the pipeline. Runs once as a one-off
# container (kafka-init) after the broker is healthy. Shared bootstrap
# infra — not specific to any one bounded context, which is why it lives
# at the top level rather than under catalog-input/ (where it originated,
# before client-input had any topics of its own).
#
# No Debezium-internal topics for client-input any more (schema-history /
# DDL-change-event topics used to live here for client-input-source-
# connector) — client-input produces de.iu.Rating.V002/Watch.Event.V002
# directly (see client-input/generator.py), no MySQL CDC hop in front of
# either any more.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
PARTITIONS="${PARTITIONS:-4}"

# Compacted master-data topics (Catalog Input Service / Debezium CDC) —
# rebuildable changelogs, keep only the latest value per key. Rating.V002
# lived here 2026-08-10 through 2026-08-11 (see EVENT_TOPICS below for why
# it moved back out) — Item/User are the only entities where a fresh
# consumer replaying from offset 0 genuinely needs "the current row per
# key, forever."
COMPACTED_TOPICS=(
  "de.iu.Item.V001"
  "de.iu.User.V001"
)

# Event topics (Client Input Service) — plain cleanup.policy=delete, 2-day
# retention. Both are streams of things that happened, not changelogs of
# current state, and neither is a bootstrap/rebuild source any more: no
# consumer is meant to reconstruct "current state" by replaying either one
# from offset 0 — reporting-db is that now (see reporting-output/README.md).
# Watch.Event never was a changelog (repeated watches of the same item are
# meaningful history, not something to compact away). Rating.V002 briefly
# was compacted (2026-08-10 through 2026-08-11) for "latest rating wins,"
# but that's redundant: reporting-rating-sink-connector's own upsert on
# (user_id,item_id) already gives reporting-db "latest wins" regardless of
# topic-level compaction. 2 days is long enough to cover a connector outage/
# restart without becoming a place anything leans on for durable history.
#
# Neither topic ever carries delete/tombstone traffic: generator.py
# produces both directly (client-input/generator.py, plain
# confluent_kafka.SerializingProducer) — there's no MySQL table, trigger,
# or CASCADE upstream of either any more for a delete to come from in the
# first place. (Item/User deletes still propagate as real tombstones on
# their own compacted topics above, from catalog-db via Postgres logical
# replication — a different mechanism, unaffected by any of this.)
#
# Both V002 (bumped from V001): Watch.Event's event model changed from a
# start/heartbeat/stop trio to a single ItemFinishedEvent; Rating dropped
# its unused rating_id/session_id fields and moved to the same composite
# {user_id, item_id} key. Both are breaking schema changes, bumped rather
# than reused, matching the versioned topic-naming convention already in
# place.
#
# de.iu.Watch.Summary.V002 (added alongside watch-summary-service, a Kafka
# Streams job - see watch-summary/README.md): Watch.Event.V002 went back to
# carrying a heartbeat every tick per in-progress item, not just one message
# per watch (client-input/README.md) - a real produce/storage increase on
# that topic. watch-summary-service is the only consumer of that raw,
# heartbeat-noisy stream; it session-windows on inactivity and republishes
# exactly one settled record per watch here, same key/value shape as
# Watch.Event.V002. reporting-watch-sink-connector reads this topic instead
# of Watch.Event.V002 now, so reporting-db/the reporting SQL jobs never
# see heartbeat noise.
EVENT_TOPICS=(
  "de.iu.Watch.Event.V002"
  "de.iu.Rating.V002"
  "de.iu.Watch.Summary.V002"
)
EVENT_RETENTION_MS=172800000  # 2 days

for topic in "${COMPACTED_TOPICS[@]}"; do
  echo "Creating topic: ${topic} (partitions=${PARTITIONS}, compacted)"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.compaction.lag.ms=0 \
    --config segment.bytes=1048576 \
    --config segment.ms=60000
  # segment.bytes/segment.ms: min.compaction.lag.ms=0 alone doesn't make a
  # topic compact promptly — Kafka only ever compacts *closed* segments,
  # never the currently-active one, and the 1GB broker default means a
  # low-volume topic like this can sit in a single never-rolled segment
  # indefinitely, silently accumulating every superseded value forever.
  # Hit this live 2026-08-10: de.iu.User.V001 grew to ~150K raw messages
  # for ~10K actually-current users because nothing had ever forced a
  # roll, and a reporting job doing a full-topic batch read every trigger
  # (see reporting-output/README.md) got dramatically slower as a result
  # (one job's batch took 4+ minutes against a 60s trigger interval).
  # Small segment.bytes/segment.ms force frequent rolls so the cleaner
  # actually has something to compact.
done

for topic in "${EVENT_TOPICS[@]}"; do
  echo "Creating topic: ${topic} (partitions=${PARTITIONS}, event, retention.ms=${EVENT_RETENTION_MS})"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config retention.ms="${EVENT_RETENTION_MS}"
done

echo "Topics created:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
