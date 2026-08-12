"""User Mood job — periodic SQL over reporting-db.

Classifies each user into **good** / **average** / **bad**, from the
average of their **last <=3 ratings** (a per-user recency rank, not a
time window). Sits on top of `ratings` (mirrored from de.iu.Rating.V002
by reporting-rating-sink-connector): Postgres's own window-function
support (`ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY rated_at
DESC)`) does the recency ranking directly.

Users with 1 or 2 ratings still get a mood (averaged over however many
they have) rather than waiting for a 3rd - a weaker signal early on that
firms up as they rate more.

Ratings for a deleted item/user can't exist here in the first place:
`ratings.item_id`/`user_id` carry a `FOREIGN KEY ... ON DELETE CASCADE`
against `items`/`users` (../../postgres/init/01_schema.sql), so no join
against those tables is needed.
"""
import os
import time

import psycopg2

# `or "<default>"`, not `.get(..., "<default>")` - docker-compose sets an
# env var to an empty string (not unset) when its .env value is missing,
# which a plain default= wouldn't catch.
TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")
RECENT_RATINGS_N = int(os.environ.get("RECENT_RATINGS_N") or "3")
GOOD_THRESHOLD = float(os.environ.get("GOOD_THRESHOLD") or "4.5")
BAD_THRESHOLD = float(os.environ.get("BAD_THRESHOLD") or "3.5")

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS user_mood (
        user_id           TEXT PRIMARY KEY,
        mood              TEXT NOT NULL,
        avg_recent_rating DOUBLE PRECISION NOT NULL,
        ratings_used      INTEGER NOT NULL,
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# Same index user-rating-avg ensures - shared by both jobs' queries below,
# harmless (and cheap) to also ensure it here in case this job starts first.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings (user_id)
"""

RECENT_RATINGS_CTE = """
    ratings_ranked AS (
        SELECT r.user_id, r.rating,
               row_number() OVER (PARTITION BY r.user_id ORDER BY r.rated_at DESC) AS recency_rank
        FROM ratings r
    ),
    recent AS (
        SELECT user_id, rating FROM ratings_ranked WHERE recency_rank <= %(recent_n)s
    )
"""

UPSERT_SQL = f"""
    WITH {RECENT_RATINGS_CTE},
    agg AS (
        SELECT user_id, avg(rating) AS avg_recent_rating, count(*) AS ratings_used
        FROM recent
        GROUP BY user_id
    )
    INSERT INTO user_mood (user_id, mood, avg_recent_rating, ratings_used, updated_at)
    SELECT
        user_id,
        CASE
            WHEN avg_recent_rating > %(good)s THEN 'good'
            WHEN avg_recent_rating >= %(bad)s THEN 'average'
            ELSE 'bad'
        END,
        avg_recent_rating, ratings_used, now()
    FROM agg
    ON CONFLICT (user_id) DO UPDATE SET
        mood = EXCLUDED.mood,
        avg_recent_rating = EXCLUDED.avg_recent_rating,
        ratings_used = EXCLUDED.ratings_used,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = f"""
    WITH {RECENT_RATINGS_CTE}
    DELETE FROM user_mood
    WHERE user_id NOT IN (SELECT DISTINCT user_id FROM recent)
"""

# to_regclass returns NULL until the sink connector has written its first
# record - on a fresh environment this job can start before any rating has
# ever landed in reporting-db.
RATINGS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.ratings') IS NOT NULL"

QUERY_PARAMS = {
    "recent_n": RECENT_RATINGS_N,
    "good": GOOD_THRESHOLD,
    "bad": BAD_THRESHOLD,
}


def run_tick():
    # Fresh connection per tick - see user-rating-avg/job.py's run_tick()
    # for why (avoids holding a connection open across a minutes-long sleep).
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(RATINGS_TABLE_EXISTS_SQL)
            if not cur.fetchone()[0]:
                print("ratings table doesn't exist yet (no rating activity mirrored so far) - skipping this tick")
                return
            cur.execute(ENSURE_INDEX_SQL)
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
