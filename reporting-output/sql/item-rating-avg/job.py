"""Item Rating Average job — periodic SQL over reporting-db.

The item-side mirror of ../user-rating-avg/job.py: same `ratings` table
(mirrored from de.iu.Rating.V002 by reporting-rating-sink-connector),
same FK-guaranteed-live-rows/full-sync rules, just GROUP BY item_id
instead of user_id.

Ratings for a deleted item/user can't exist here in the first place:
`ratings.item_id`/`user_id` carry a `FOREIGN KEY ... ON DELETE CASCADE`
against `items`/`users` (../../postgres/init/01_schema.sql), so a delete
removes the referencing rating in the same transaction - no join needed
to filter them out. Full sync every tick: an item_id missing from this
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
    CREATE TABLE IF NOT EXISTS item_rating_avg (
        item_id      TEXT PRIMARY KEY,
        avg_rating   DOUBLE PRECISION NOT NULL,
        rating_count BIGINT NOT NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

# `ratings`/`items`/`users` are each owned by their own JDBC sink connector
# (auto.create=true creates the table, but not this index). Same index
# user-rating-avg ensures on user_id - this one's the item-side equivalent,
# harmless (and cheap) to also ensure here in case this job starts first.
ENSURE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_ratings_item_id ON ratings (item_id)
"""

UPSERT_SQL = """
    INSERT INTO item_rating_avg (item_id, avg_rating, rating_count, updated_at)
    SELECT r.item_id, avg(r.rating), count(*), now()
    FROM ratings r
    GROUP BY r.item_id
    ON CONFLICT (item_id) DO UPDATE SET
        avg_rating = EXCLUDED.avg_rating,
        rating_count = EXCLUDED.rating_count,
        updated_at = EXCLUDED.updated_at
"""

DELETE_STALE_SQL = """
    DELETE FROM item_rating_avg
    WHERE item_id NOT IN (
        SELECT DISTINCT item_id FROM ratings
    )
"""

# to_regclass returns NULL until the sink connector has written its first
# record - on a fresh environment this job can start before any rating has
# ever landed in reporting-db.
RATINGS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.ratings') IS NOT NULL"


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
