"""Item Viewer Demographics job — periodic SQL over reporting-db.

Maintains, per item: the **average age** of its viewers, and a **sex
distribution** across four buckets (`pct_male`/`pct_female`/`pct_other`/
`pct_unknown`) that always sum to exactly 1.00 - `unknown` absorbs both a
NULL `users.gender` and any value outside the three named categories, so
every viewer lands in exactly one bucket by construction.

**Distinct viewers, not raw watch events - a deliberate exception to this
directory's usual "every watch event counts" rule** (see
`../item-completion-rate/job.py`/`../user-series-movie-ratio/job.py`).
Those jobs count repeat watches because a rewatch is a second real data
point about *behavior* (did they finish it again, what did they watch
again). A demographic breakdown is about *who* watched, not how many
times - counting a rewatcher's age/gender twice would double-count one
person and skew the distribution, so this job dedupes to one row per
`(item_id, user_id)` before aggregating.

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
    CREATE TABLE IF NOT EXISTS item_viewer_demographics (
        item_id       TEXT PRIMARY KEY,
        average_age   DOUBLE PRECISION,
        pct_male      DOUBLE PRECISION NOT NULL,
        pct_female    DOUBLE PRECISION NOT NULL,
        pct_other     DOUBLE PRECISION NOT NULL,
        pct_unknown   DOUBLE PRECISION NOT NULL,
        viewer_count  BIGINT NOT NULL,
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `watch_events` is owned by reporting-watch-sink-connector - harmless/cheap
# to also ensure this index here in case this job starts first, same
# reasoning ../user-series-movie-ratio/job.py gives.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)
"""
ENSURE_USER_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)
"""

VIEWER_DEMOGRAPHICS_CTE = """
    live_viewers AS (
        SELECT DISTINCT w.item_id, w.user_id, u.age, u.gender
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
    ),
    bucketed AS (
        SELECT item_id, age,
               CASE WHEN gender IN ('male', 'female', 'other') THEN gender
                    ELSE 'unknown' END AS gender_bucket
        FROM live_viewers
    )
"""

UPSERT_SQL = f"""
    WITH {VIEWER_DEMOGRAPHICS_CTE}
    INSERT INTO item_viewer_demographics
        (item_id, average_age, pct_male, pct_female, pct_other, pct_unknown, viewer_count, updated_at)
    SELECT
        item_id,
        avg(age),
        count(*) FILTER (WHERE gender_bucket = 'male')::double precision / count(*),
        count(*) FILTER (WHERE gender_bucket = 'female')::double precision / count(*),
        count(*) FILTER (WHERE gender_bucket = 'other')::double precision / count(*),
        count(*) FILTER (WHERE gender_bucket = 'unknown')::double precision / count(*),
        count(*),
        now()
    FROM bucketed
    GROUP BY item_id
    ON CONFLICT (item_id) DO UPDATE SET
        average_age = EXCLUDED.average_age,
        pct_male = EXCLUDED.pct_male,
        pct_female = EXCLUDED.pct_female,
        pct_other = EXCLUDED.pct_other,
        pct_unknown = EXCLUDED.pct_unknown,
        viewer_count = EXCLUDED.viewer_count,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {VIEWER_DEMOGRAPHICS_CTE}
    DELETE FROM item_viewer_demographics
    WHERE item_id NOT IN (SELECT DISTINCT item_id FROM live_viewers)
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
        ensure_index(conn, ENSURE_USER_INDEX_SQL)

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
