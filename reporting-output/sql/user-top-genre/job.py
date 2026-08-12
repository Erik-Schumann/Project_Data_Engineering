"""User Top Genre job — periodic SQL over reporting-db.

de.iu.Watch.Summary.V002 (watch-summary-service's settled output, not
the raw de.iu.Watch.Event.V002 heartbeat stream) is mirrored into
reporting-db's `watch_events` table by reporting-watch-sink-connector.

Maintains, per user, their most-watched **primary** genre (`genre_primary`
only — `genre_secondary` is ignored) over their **last N watches**
(`USER_TOP_GENRE_RECENT_N`, default 8) — a per-user recency *rank* by
`session_ended_at` (the event's own business timestamp, not Kafka offset
or ingestion time). Postgres's own window-function support
(`ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ... DESC)`) does this
directly. The window is picked from the raw watch history *before* the
genre lookup, so "last 8" always means the 8 most recent watch events,
not the 8 most recent watches that happen to carry a genre.

`genre_primary` is nullable in the catalog schema (an item can be seeded
without a genre). A null-genre watch still occupies one of the user's
last-N recency slots (so the window itself never silently reaches further
back to "find" a genre), but it contributes no vote toward the mode. A
user whose entire recent window is null-genre watches ends up with no
rows in the per-genre count and is dropped from the output table by the
full-sync delete below.

Genre mode ties (e.g. last 8 watches split 4 Drama / 4 Comedy) are broken
by `genre_primary` ascending — arbitrary but deterministic, so the result
doesn't flip between identical-looking ticks.
"""
import os
import time

import psycopg2

TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")
RECENT_WATCHES_N = int(os.environ.get("USER_TOP_GENRE_RECENT_N") or "8")

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS user_top_genre (
        user_id            TEXT PRIMARY KEY,
        top_genre          TEXT NOT NULL,
        genre_watch_count  BIGINT NOT NULL,
        watches_used       BIGINT NOT NULL,
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector - harmless/cheap
# to also ensure these indexes here in case this job starts first, same
# reasoning ../user-series-movie-ratio/job.py gives. session_ended_at is
# this job's own recency-ranking column, not shared with the other jobs.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""
ENSURE_ITEM_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""
ENSURE_SESSION_ENDED_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_session_ended_at ON watch_events (session_ended_at)
"""

TOP_GENRE_CTE = """
    live_watches AS (
        SELECT w.user_id, i.genre_primary, w.session_ended_at
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
    ),
    recent AS (
        SELECT user_id, genre_primary,
               row_number() OVER (PARTITION BY user_id ORDER BY session_ended_at DESC) AS recency_rank
        FROM live_watches
    ),
    recent_n AS (
        SELECT user_id, genre_primary FROM recent WHERE recency_rank <= %(recent_n)s
    ),
    genre_counts AS (
        SELECT user_id, genre_primary, count(*) AS genre_watch_count
        FROM recent_n
        WHERE genre_primary IS NOT NULL
        GROUP BY user_id, genre_primary
    ),
    ranked_genres AS (
        SELECT user_id, genre_primary, genre_watch_count,
               row_number() OVER (
                   PARTITION BY user_id ORDER BY genre_watch_count DESC, genre_primary ASC
               ) AS mode_rank
        FROM genre_counts
    ),
    top AS (
        SELECT user_id, genre_primary AS top_genre, genre_watch_count
        FROM ranked_genres WHERE mode_rank = 1
    ),
    watches_used AS (
        SELECT user_id, count(*) AS watches_used
        FROM recent_n
        WHERE genre_primary IS NOT NULL
        GROUP BY user_id
    )
"""

UPSERT_SQL = f"""
    WITH {TOP_GENRE_CTE}
    INSERT INTO user_top_genre (user_id, top_genre, genre_watch_count, watches_used, updated_at)
    SELECT t.user_id, t.top_genre, t.genre_watch_count, wu.watches_used, now()
    FROM top t JOIN watches_used wu ON wu.user_id = t.user_id
    ON CONFLICT (user_id) DO UPDATE SET
        top_genre = EXCLUDED.top_genre,
        genre_watch_count = EXCLUDED.genre_watch_count,
        watches_used = EXCLUDED.watches_used,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {TOP_GENRE_CTE}
    DELETE FROM user_top_genre
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM top)
"""

QUERY_PARAMS = {"recent_n": RECENT_WATCHES_N}

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
        ensure_index(conn, ENSURE_ITEM_INDEX_SQL)
        ensure_index(conn, ENSURE_SESSION_ENDED_INDEX_SQL)

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
