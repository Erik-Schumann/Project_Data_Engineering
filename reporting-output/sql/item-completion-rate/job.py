"""Item Completion Rate job — periodic SQL over reporting-db.

de.iu.Watch.Summary.V002 (watch-summary-service's settled output, not
the raw de.iu.Watch.Event.V002 heartbeat stream) is mirrored into
reporting-db's `watch_events` table by reporting-watch-sink-connector.

**"Completed" is defined as**: `watched_seconds >= 0.85 * duration_seconds`, where
`duration_seconds` is `runtime_minutes * 60` for a movie, or a fixed
`DEFAULT_EPISODE_SECONDS` (40 minutes) assumption for a series - the
catalog has no per-episode runtime field. `completion_rate` here is the
*fraction* of an item's watches that crossed that threshold, not the
average fraction-watched - those are different numbers.

Every watch event counts (no dedup) - `watch_events` is an insert-only
mirror of an append-only topic, and a rewatch is a second real data point
about whether people who start this item tend to finish it, not a
duplicate to collapse away.

Watches for a deleted item or user don't count, and the table is fully
synced every tick - same rules ../user-series-movie-ratio/job.py applies.
"""
import os
import time

import psycopg2

TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")

# Series don't carry a per-episode runtime in the catalog schema - same
# documented simplification client-input/generator.py already makes
# (DEFAULT_EPISODE_SECONDS).
DEFAULT_EPISODE_SECONDS = 40 * 60
COMPLETION_THRESHOLD = 0.85

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS item_completion_rate (
        item_id          TEXT PRIMARY KEY,
        type             TEXT NOT NULL,
        completion_rate  DOUBLE PRECISION NOT NULL,
        watch_count      BIGINT NOT NULL,
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector - harmless/cheap
# to also ensure these indexes here in case this job starts first, same
# reasoning ../user-series-movie-ratio/job.py gives.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""
ENSURE_USER_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""

LIVE_WATCHES_CTE = """
    durations AS (
        SELECT item_id, type,
               CASE WHEN type = 'movie' THEN runtime_minutes * 60
                    ELSE %(default_episode_seconds)s END AS duration_seconds
        FROM items
    ),
    live_watches AS (
        SELECT
            w.item_id,
            d.type,
            (w.watched_seconds >= %(threshold)s * d.duration_seconds) AS completed
        FROM watch_events w
        JOIN durations d ON d.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
    )
"""

UPSERT_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    INSERT INTO item_completion_rate (item_id, type, completion_rate, watch_count, updated_at)
    SELECT item_id, type, avg(completed::int::double precision), count(*), now()
    FROM live_watches
    GROUP BY item_id, type
    ON CONFLICT (item_id) DO UPDATE SET
        type = EXCLUDED.type,
        completion_rate = EXCLUDED.completion_rate,
        watch_count = EXCLUDED.watch_count,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    DELETE FROM item_completion_rate
    WHERE item_id NOT IN (SELECT DISTINCT item_id FROM live_watches)
"""

QUERY_PARAMS = {
    "default_episode_seconds": DEFAULT_EPISODE_SECONDS,
    "threshold": COMPLETION_THRESHOLD,
}

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
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(WATCH_EVENTS_TABLE_EXISTS_SQL)
            if not cur.fetchone()[0]:
                print("watch_events table doesn't exist yet (no watch activity mirrored so far) - skipping this tick")
                return

        ensure_index(conn, ENSURE_INDEX_SQL)
        ensure_index(conn, ENSURE_USER_INDEX_SQL)

        with conn, conn.cursor() as cur:
            cur.execute(UPSERT_SQL, QUERY_PARAMS)
            cur.execute(DELETE_STALE_SQL, QUERY_PARAMS)
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
