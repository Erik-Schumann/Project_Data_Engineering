"""User Watch Count job — periodic SQL over reporting-db.

Maintains, per user, two counts over `reporting-db.watch_events`:

- `distinct_items_watched` — how many different items they've ever
  watched (`count(distinct item_id)`).
- `total_watches` — how many watch events they've generated in total,
  rewatches included (`count(*)`).

These are deliberately two different numbers, same reasoning
`item-completion-rate/job.py` gives for "completion rate" vs. "average
fraction watched": a user who rewatches the same handful of items a lot
has a low `distinct_items_watched` but a high `total_watches`, and that
gap is itself the signal (a completionist bingeing new content looks very
different from someone re-running their comfort favorites), not something
either single number alone would show. Not split by movie/series - that
breakdown already exists in `user_series_movie_ratio`.

Watches from a deleted item or user can't exist here in the first place:
`watch_events.item_id`/`user_id` carry a `FOREIGN KEY ... ON DELETE
CASCADE` against `items`/`users` (../../postgres/init/01_schema.sql), so
no join against those tables is needed. The table is fully synced every
tick — same rule every job in this directory follows.
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
    CREATE TABLE IF NOT EXISTS user_watch_count (
        user_id                 TEXT PRIMARY KEY,
        distinct_items_watched  BIGINT NOT NULL,
        total_watches           BIGINT NOT NULL,
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector - harmless/cheap
# to also ensure these indexes here in case this job starts first, same
# reasoning every other job in this directory gives.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""
ENSURE_ITEM_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""

LIVE_WATCHES_CTE = """
    live_watches AS (
        SELECT w.user_id, w.item_id
        FROM watch_events w
    )
"""

UPSERT_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    INSERT INTO user_watch_count (user_id, distinct_items_watched, total_watches, updated_at)
    SELECT user_id, count(DISTINCT item_id), count(*), now()
    FROM live_watches
    GROUP BY user_id
    ON CONFLICT (user_id) DO UPDATE SET
        distinct_items_watched = EXCLUDED.distinct_items_watched,
        total_watches = EXCLUDED.total_watches,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {LIVE_WATCHES_CTE}
    DELETE FROM user_watch_count
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM live_watches)
"""

# to_regclass returns NULL until reporting-watch-sink-connector has written
# its first record - on a fresh environment this job can start before any
# watch event has ever landed in reporting-db.
WATCH_EVENTS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.watch_events') IS NOT NULL"


def ensure_index(conn, sql):
    """CREATE INDEX IF NOT EXISTS isn't safe against a concurrent creator -
    every job in this directory shares these watch_events indexes, so
    several can hit a deadlock/unique-violation racing to create the same
    one on first boot (hit live the first time the other four started
    together). Each index gets its own short transaction so one loser here
    doesn't roll back this tick's real work - if the index still doesn't
    exist afterward, the next tick just tries again."""
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
