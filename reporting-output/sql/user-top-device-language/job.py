"""User Top Device/Language job — periodic SQL over reporting-db.

Maintains, per user, their most-used **device** (`device_type`) and their
most-watched **original language** (`items.original_language`) — both
**all-time**, not a recency-windowed rank like `../user-top-genre/job.py`.
Every live watch ever recorded for the user counts toward the mode; a tie
in the count is broken by whichever candidate's *most recent* watch is
more recent, and a still-unresolved tie (identical count and identical
most-recent timestamp) falls back to the candidate's own value ascending
(`device_type`/`original_language`) - arbitrary but deterministic, same
role `../user-top-genre/job.py`'s ascending-genre tiebreak plays.

`device_type` is always present on a live watch (validated non-empty at
produce time in client-input/generator.py), so every user with at least
one live watch gets a `top_device`. `original_language` is nullable in
the catalog schema (an item can be seeded without one); a null-language
watch still counts as a watch but casts no vote toward the language mode,
so a user whose entire watch history is null-language items gets a
`top_device` but a NULL `top_language` - a real state, not a missing row
(handled with a LEFT JOIN between the two winners below, not an INNER
JOIN, since a device winner always exists but a language winner might
not).

Watches for a deleted item or user don't count (same INNER JOIN against
items/users every job in this directory uses), and the table is fully
synced every tick.
"""
import os
import time

import psycopg2

TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS user_top_device_language (
        user_id               TEXT PRIMARY KEY,
        top_device             TEXT NOT NULL,
        device_watch_count     BIGINT NOT NULL,
        top_language           TEXT,
        language_watch_count   BIGINT,
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector - harmless/cheap
# to also ensure these indexes here in case this job starts first, same
# reasoning ../user-series-movie-ratio/job.py gives.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""
ENSURE_ITEM_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""

TOP_DEVICE_LANGUAGE_CTE = """
    live_watches AS (
        SELECT w.user_id, w.device_type, i.original_language, w.session_ended_at
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
    ),
    device_counts AS (
        SELECT user_id, device_type, count(*) AS device_watch_count,
               max(session_ended_at) AS last_watched_at
        FROM live_watches
        GROUP BY user_id, device_type
    ),
    device_ranked AS (
        SELECT user_id, device_type, device_watch_count,
               row_number() OVER (
                   PARTITION BY user_id
                   ORDER BY device_watch_count DESC, last_watched_at DESC, device_type ASC
               ) AS mode_rank
        FROM device_counts
    ),
    top_device AS (
        SELECT user_id, device_type AS top_device, device_watch_count
        FROM device_ranked WHERE mode_rank = 1
    ),
    language_counts AS (
        SELECT user_id, original_language, count(*) AS language_watch_count,
               max(session_ended_at) AS last_watched_at
        FROM live_watches
        WHERE original_language IS NOT NULL
        GROUP BY user_id, original_language
    ),
    language_ranked AS (
        SELECT user_id, original_language, language_watch_count,
               row_number() OVER (
                   PARTITION BY user_id
                   ORDER BY language_watch_count DESC, last_watched_at DESC, original_language ASC
               ) AS mode_rank
        FROM language_counts
    ),
    top_language AS (
        SELECT user_id, original_language AS top_language, language_watch_count
        FROM language_ranked WHERE mode_rank = 1
    )
"""

UPSERT_SQL = f"""
    WITH {TOP_DEVICE_LANGUAGE_CTE}
    INSERT INTO user_top_device_language
        (user_id, top_device, device_watch_count, top_language, language_watch_count, updated_at)
    SELECT d.user_id, d.top_device, d.device_watch_count, l.top_language, l.language_watch_count, now()
    FROM top_device d
    LEFT JOIN top_language l ON l.user_id = d.user_id
    ON CONFLICT (user_id) DO UPDATE SET
        top_device = EXCLUDED.top_device,
        device_watch_count = EXCLUDED.device_watch_count,
        top_language = EXCLUDED.top_language,
        language_watch_count = EXCLUDED.language_watch_count,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {TOP_DEVICE_LANGUAGE_CTE}
    DELETE FROM user_top_device_language
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM top_device)
"""

# to_regclass returns NULL until reporting-watch-sink-connector has written
# its first record - on a fresh environment this job can start before any
# watch event has ever landed in reporting-db.
WATCH_EVENTS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.watch_events') IS NOT NULL"


def ensure_index(conn, sql):
    """CREATE INDEX IF NOT EXISTS isn't safe against a concurrent creator -
    every job in this directory shares these watch_events indexes, so
    several can hit a deadlock/unique-violation racing to create the same
    one on first boot. Each index gets its own short transaction so one
    loser here doesn't roll back this tick's real work - if the index
    still doesn't exist afterward, the next tick just tries again."""
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
