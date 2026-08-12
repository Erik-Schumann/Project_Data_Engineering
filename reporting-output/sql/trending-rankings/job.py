"""Trending Rankings job — periodic SQL over reporting-db.

de.iu.Watch.Summary.V002 (watch-summary-service's settled output, not
the raw de.iu.Watch.Event.V002 heartbeat stream) is mirrored into
reporting-db's `watch_events` table by reporting-watch-sink-connector.

Computes, on a rolling last-WINDOW_MINUTES basis (default 10), the top
TOP_N (default 10) movies and top TOP_N series by two separate metrics:

1. **View count** — `watch_events` rows in the window, upserted into
   `item_view_ranking`.
2. **Average rating** — `ratings` rows in the window, upserted into
   `item_rating_ranking`.

Both tables are keyed by `(type, rank)` and fully replaced (`TRUNCATE` +
insert) every tick — a top-10 list has no natural per-row identity to
upsert against once the *set* of qualifying items changes, unlike the
other jobs' per-entity upsert.

Ranking via `row_number() OVER (PARTITION BY type ORDER BY metric DESC)`
— Postgres's native window-function support, same point
../user-top-genre/job.py makes for its own recency ranking.

**Ratings only count if given/updated within the window.** `ratings` is
already deduped to one row per `(user_id, item_id)` by
`reporting-rating-sink-connector`'s own upsert (latest wins), so this job
just filters `rated_at >= now() - WINDOW_MINUTES` on top — a user's
technically-current rating for an item doesn't count toward "trending now"
if they gave it outside the window, even though it counts for the
all-time per-user/per-item averages. `watch_events` needs no such dedup
(insert-only mirror of an append-only topic — every row is a genuine
distinct watch).

Ratings/watches from a deleted item or user are excluded (`INNER JOIN`
against live `items`/`users`), same as every other job here.
"""
import os
import time

import psycopg2
from psycopg2.extras import execute_values

TRIGGER_INTERVAL_SECONDS = int(os.environ.get("TRIGGER_INTERVAL_SECONDS") or "60")
WINDOW_MINUTES = int(os.environ.get("TRENDING_WINDOW_MINUTES") or "10")
TOP_N = int(os.environ.get("TRENDING_TOP_N") or "10")

PG_DSN = (
    f"host={os.environ.get('REPORTING_DB_HOST', 'reporting-db')} "
    f"port={os.environ.get('REPORTING_DB_PORT', '5432')} "
    f"dbname={os.environ.get('REPORTING_DB_NAME', 'reporting')} "
    f"user={os.environ.get('REPORTING_DB_USER', 'reporting')} "
    f"password={os.environ.get('REPORTING_DB_PASSWORD', 'reporting')}"
)

ENSURE_VIEW_RANKING_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS item_view_ranking (
        type       TEXT NOT NULL,
        rank       INTEGER NOT NULL,
        item_id    TEXT NOT NULL,
        view_count BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (type, rank)
    )
"""
ENSURE_RATING_RANKING_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS item_rating_ranking (
        type         TEXT NOT NULL,
        rank         INTEGER NOT NULL,
        item_id      TEXT NOT NULL,
        avg_rating   DOUBLE PRECISION NOT NULL,
        rating_count BIGINT NOT NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (type, rank)
    )
"""

# `watch_events`/`ratings` are each owned by their own JDBC sink connector -
# harmless/cheap to also ensure these indexes here in case this job starts
# first, same reasoning every other job in this directory gives.
ENSURE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_watch_events_item_id ON watch_events (item_id)",
    "CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_watch_events_session_ended_at ON watch_events (session_ended_at)",
    "CREATE INDEX IF NOT EXISTS idx_ratings_item_id ON ratings (item_id)",
    "CREATE INDEX IF NOT EXISTS idx_ratings_user_id ON ratings (user_id)",
]

VIEW_RANKING_SQL = """
    WITH view_counts AS (
        SELECT w.item_id, i.type, count(*) AS view_count
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
        WHERE w.session_ended_at >= now() - make_interval(mins => %(window_minutes)s)
        GROUP BY w.item_id, i.type
    ),
    ranked AS (
        SELECT item_id, type, view_count,
               row_number() OVER (PARTITION BY type ORDER BY view_count DESC) AS rank
        FROM view_counts
    )
    SELECT type, rank, item_id, view_count
    FROM ranked WHERE rank <= %(top_n)s
    ORDER BY type, rank
"""

# view_count is a tertiary tiebreaker on top of avg_rating/rating_count -
# left join + coalesce since an item can be rated in the window without a
# qualifying watch event in it too (e.g. watched just before the window
# opened, rated just after). Matches what the Grafana Trending dashboard
# actually displays.
RATING_RANKING_SQL = """
    WITH rating_avgs AS (
        SELECT r.item_id, i.type, avg(r.rating) AS avg_rating, count(*) AS rating_count
        FROM ratings r
        JOIN items i ON i.item_id = r.item_id
        JOIN users u ON u.user_id = r.user_id
        WHERE r.rated_at >= now() - make_interval(mins => %(window_minutes)s)
        GROUP BY r.item_id, i.type
    ),
    view_counts AS (
        SELECT w.item_id, count(*) AS view_count
        FROM watch_events w
        JOIN items i ON i.item_id = w.item_id
        JOIN users u ON u.user_id = w.user_id
        WHERE w.session_ended_at >= now() - make_interval(mins => %(window_minutes)s)
        GROUP BY w.item_id
    ),
    combined AS (
        SELECT ra.item_id, ra.type, ra.avg_rating, ra.rating_count,
               coalesce(vc.view_count, 0) AS view_count
        FROM rating_avgs ra
        LEFT JOIN view_counts vc ON vc.item_id = ra.item_id
    ),
    ranked AS (
        SELECT item_id, type, avg_rating, rating_count,
               row_number() OVER (
                   PARTITION BY type
                   ORDER BY avg_rating DESC, rating_count DESC, view_count DESC
               ) AS rank
        FROM combined
    )
    SELECT type, rank, item_id, avg_rating, rating_count
    FROM ranked WHERE rank <= %(top_n)s
    ORDER BY type, rank
"""

QUERY_PARAMS = {"window_minutes": WINDOW_MINUTES, "top_n": TOP_N}

# to_regclass returns NULL until the respective sink connector has written
# its first record - on a fresh environment this job can start before any
# watch/rating activity has ever landed in reporting-db.
WATCH_EVENTS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.watch_events') IS NOT NULL"
RATINGS_TABLE_EXISTS_SQL = "SELECT to_regclass('public.ratings') IS NOT NULL"


def sync_ranking_table(cur, table, columns, rows):
    """Full replace, not an upsert - a top-N list has no stable per-row
    identity to upsert against once which items qualify changes. TRUNCATE +
    insert in the same transaction as the caller, safe because both
    ranking tables are always small (<= 2 * TOP_N rows)."""
    cur.execute(f"TRUNCATE TABLE {table}")
    if rows:
        placeholders = ", ".join(["%s"] * len(columns))
        execute_values(
            cur,
            f"INSERT INTO {table} ({', '.join(columns)}, updated_at) VALUES %s",
            rows,
            template=f"({placeholders}, now())",
        )


def ensure_index(conn, sql):
    """CREATE INDEX IF NOT EXISTS isn't safe against a concurrent creator -
    every job in this directory shares these watch_events/ratings indexes,
    so all four can hit a deadlock/unique-violation racing to create the
    same one on first boot (hit live the first time they all started
    together). Each index gets its own short transaction so one loser here
    doesn't roll back this tick's real work - if the index still doesn't
    exist afterward, the next tick just tries again."""
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
            watch_events_exist = cur.fetchone()[0]
            cur.execute(RATINGS_TABLE_EXISTS_SQL)
            ratings_exist = cur.fetchone()[0]
            if not watch_events_exist or not ratings_exist:
                print("watch_events/ratings table doesn't exist yet (no activity mirrored so far) - skipping this tick")
                return

        for stmt in ENSURE_INDEX_SQL:
            ensure_index(conn, stmt)

        with conn, conn.cursor() as cur:
            cur.execute(VIEW_RANKING_SQL, QUERY_PARAMS)
            view_rows = cur.fetchall()
            sync_ranking_table(cur, "item_view_ranking", ["type", "rank", "item_id", "view_count"], view_rows)

            cur.execute(RATING_RANKING_SQL, QUERY_PARAMS)
            rating_rows = cur.fetchall()
            sync_ranking_table(
                cur, "item_rating_ranking", ["type", "rank", "item_id", "avg_rating", "rating_count"], rating_rows
            )
    finally:
        conn.close()


def main():
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ENSURE_VIEW_RANKING_TABLE_SQL)
            cur.execute(ENSURE_RATING_RANKING_TABLE_SQL)
    finally:
        conn.close()

    while True:
        run_tick()
        time.sleep(TRIGGER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
