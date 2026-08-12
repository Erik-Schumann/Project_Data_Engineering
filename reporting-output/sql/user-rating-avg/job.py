"""User Rating Average job — periodic SQL over reporting-db.

de.iu.Rating.V002 is mirrored into reporting-db's `ratings` table by
reporting-rating-sink-connector (../../connect/reporting-rating-sink-connector.json),
upserting on (user_id, item_id) — Postgres's own ON CONFLICT gives
"latest per key wins" for free at write time, so "all-time average
rating per user" is a two-line SQL aggregate.

Ratings for a deleted item/user can't exist here in the first place:
`ratings.item_id`/`user_id` carry a `FOREIGN KEY ... ON DELETE CASCADE`
against `items`/`users` (../../postgres/init/01_schema.sql), so a delete
removes the referencing rating in the same transaction - no join needed
to filter them out. Full sync every tick: a user_id missing from this
tick's aggregate has its row deleted, not left stale.
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
    CREATE TABLE IF NOT EXISTS user_rating_avg (
        user_id      TEXT PRIMARY KEY,
        avg_rating   DOUBLE PRECISION NOT NULL,
        rating_count BIGINT NOT NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `ratings`/`items`/`users` are each owned by their own JDBC sink connector
# (auto.create=true creates the table, but not this index).
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings (user_id)
"""

UPSERT_SQL = """
    INSERT INTO user_rating_avg (user_id, avg_rating, rating_count, updated_at)
    SELECT r.user_id, avg(r.rating), count(*), now()
    FROM ratings r
    GROUP BY r.user_id
    ON CONFLICT (user_id) DO UPDATE SET
        avg_rating = EXCLUDED.avg_rating,
        rating_count = EXCLUDED.rating_count,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = """
    DELETE FROM user_rating_avg
    WHERE user_id NOT IN (
        SELECT DISTINCT user_id FROM ratings
    )
"""

# to_regclass returns NULL until the sink connector has written its first
# record - on a fresh environment this job can start before any rating has
# ever landed in reporting-db.
RATINGS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.ratings') IS NOT NULL"


def run_tick():
    # Fresh connection per tick - avoids holding a connection open across
    # a minutes-long sleep, which would otherwise silently go stale across
    # a reporting-db restart.
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(RATINGS_TABLE_EXISTS_SQL)
            if not cur.fetchone()[0]:
                print("ratings table doesn't exist yet (no rating activity mirrored so far) - skipping this tick")
                return
            cur.execute(ENSURE_INDEX_SQL)
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
