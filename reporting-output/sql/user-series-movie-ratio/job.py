"""User Series/Movie Ratio job — periodic SQL over reporting-db.

de.iu.Watch.Summary.V002 (watch-summary-service's settled output, not
the raw de.iu.Watch.Event.V002 heartbeat stream) is mirrored into
reporting-db's `watch_events` table by reporting-watch-sink-connector
(../../connect/reporting-watch-sink-connector.json) — insert-only
(`insert.mode=insert`, `pk.mode=kafka`: the connector's own
`__connect_topic/partition/offset` triple as primary key, not the event's
business key), since watch events are append-only and a rewatch is a
second genuine row, not a value to overwrite the way Rating.V002's upsert
mirror works. de.iu.Watch.Summary.V002 never carries a delete/tombstone at
all (watch-summary-service only ever produces, same as generator.py
upstream of it — see ../../../watch-summary/README.md and
../../../client-input/README.md); the connector's `Filter` +
`RecordIsTombstone` predicate is defensive dead code for the same reason.
A rating/watch event whose item/user is later deleted is cleaned up
independently, in reporting-db itself, by its own
`FOREIGN KEY ... ON DELETE CASCADE`
(../../postgres/init/01_schema.sql) — not by anything Kafka-side. Any
row that predates that mechanism (or a catalog-db volume reset — see
reconcile_stale_keys.py) is still excluded at query time below by the same
INNER JOIN every other job here uses, as defense in depth.

All watch events count, not deduped to "distinct items watched" — a
rewatch is real viewing activity and should count toward the percentages
same as any other watch. Watches from a deleted item or user don't count (INNER JOIN against
live items/users), and the table is fully synced every tick (a user_id
missing from this tick's result has its row deleted, not left stale).
"""
import os
import time

import psycopg2

# `or "60"`, not `.get(..., "60")` - docker-compose sets an env var to an
# empty string (not unset) when its .env value is missing, which a plain
# default= wouldn't catch.
TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS user_series_movie_ratio (
        user_id             TEXT PRIMARY KEY,
        movie_watch_count   BIGINT NOT NULL,
        series_watch_count  BIGINT NOT NULL,
        movie_percentage    DOUBLE PRECISION,
        series_percentage   DOUBLE PRECISION,
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector (auto.create=true
# creates the table, but not these indexes) - harmless/cheap to also ensure
# them here in case this job starts first, same reasoning
# ../user-mood/job.py gives for idx_ratings_user_id.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""
ENSURE_ITEM_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""

LIVE_WATCHES_CTE = """
    live_watches AS (
        SELECT w.user_id, i.type
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
    )
"""

UPSERT_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    INSERT INTO user_series_movie_ratio
        (user_id, movie_watch_count, series_watch_count,
         movie_percentage, series_percentage, updated_at)
    SELECT
        user_id,
        count(*) FILTER (WHERE type = 'movie'),
        count(*) FILTER (WHERE type = 'series'),
        count(*) FILTER (WHERE type = 'movie')::double precision / count(*),
        count(*) FILTER (WHERE type = 'series')::double precision / count(*),
        now()
    FROM live_watches
    GROUP BY user_id
    ON CONFLICT (user_id) DO UPDATE SET
        movie_watch_count = EXCLUDED.movie_watch_count,
        series_watch_count = EXCLUDED.series_watch_count,
        movie_percentage = EXCLUDED.movie_percentage,
        series_percentage = EXCLUDED.series_percentage,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    DELETE FROM user_series_movie_ratio
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM live_watches)
"""

# to_regclass returns NULL until reporting-watch-sink-connector has written
# its first record - on a fresh environment this job can start before any
# watch event has ever landed in reporting-db.
WATCH_EVENTS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.watch_events') IS NOT NULL"


def ensure_index(conn, sql):
    """CREATE INDEX IF NOT EXISTS isn't safe against a concurrent creator -
    every job in this directory shares these watch_events indexes, so all
    four can hit a deadlock/unique-violation racing to create the same one
    on first boot (hit live the first time they all started together).
    Each index gets its own short transaction so one loser here doesn't
    roll back this tick's real work - if the index still doesn't exist
    afterward, the next tick just tries again."""
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
    except psycopg2.Error:
        pass


def run_tick():
    # Fresh connection per tick, same reasoning as ../user-rating-avg/job.py's
    # run_tick() - avoids holding a connection open across a minutes-long sleep.
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(WATCH_EVENTS_TABLE_EXISTS_SQL)
            if not cur.fetchone()[0]:
                print("watch_events table doesn't exist yet (no watch activity mirrored so far) - skipping this tick")
                return

        ensure_index(conn, ENSURE_INDEX_SQL)
        ensure_index(conn, ENSURE_ITEM_INDEX_SQL)

        with conn, conn.cursor() as cur:
            cur.execute(UPSERT_SQL)
            cur.execute(DELETE_STALE_SQL)
    finally:
        conn.close()


def main():
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ENSURE_TABLE_SQL)
    finally:
        conn.close()

    while True:
        run_tick()
        time.sleep(TRIGGER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
