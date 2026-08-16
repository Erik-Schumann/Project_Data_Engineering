"""Correctness tests for the 10 sql/*/job.py aggregate jobs - the actual
GROUP BY/window-function/tiebreak logic living in each job's SQL strings,
not covered by test_connector_dlq_integration.py (which is about Connect's
own error-handling, not these jobs at all). Each job's aggregate table is
created here directly via that job's own ENSURE_TABLE_SQL and its
run_tick() is called in-process (no docker container for the job itself
needed) against real reporting-db rows seeded straight into items/users/
ratings/watch_events - the same tables the real JDBC sink connectors
mirror into, just written here directly rather than round-tripped through
Kafka, since it's the SQL's correctness under test, not the connector
pipeline (already covered by test_connector_dlq_integration.py).

One test per job, each aimed at that job's single most distinctive/
highest-risk piece of logic (a window filter, a tiebreak rule, a rounding
boundary, a dedup rule) rather than exhaustively re-deriving every job's
full behavior - see conftest.py's docstring for why "integration" here
means a real Postgres, not real threads/timing like client-input's.

Every test cleans up exactly what it created (items/users, cascading to
ratings/watch_events via FK, plus the job's own aggregate-table row) -
this is the same real, shared reporting-db test_connector_dlq_integration.py
already writes to."""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from conftest import load_job

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("require_reporting_db")]


def _uid():
    return uuid.uuid4().hex[:10]


def insert_item(conn, item_id, **overrides):
    row = {
        "item_id": item_id, "type": "movie", "title": f"zz-test-{item_id}",
        "runtime_minutes": 100, "genre_primary": None, "original_language": None,
    }
    row.update(overrides)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO items (item_id, type, title, runtime_minutes, genre_primary, original_language) "
            "VALUES (%(item_id)s, %(type)s, %(title)s, %(runtime_minutes)s, %(genre_primary)s, %(original_language)s)",
            row,
        )


def insert_user(conn, user_id, **overrides):
    row = {"user_id": user_id, "age": 30, "gender": "female"}
    row.update(overrides)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (user_id, age, gender) VALUES (%(user_id)s, %(age)s, %(gender)s)", row)


def insert_rating(conn, user_id, item_id, rating, rated_at):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ratings (user_id, item_id, rating, rated_at) VALUES (%s, %s, %s, %s)",
            (user_id, item_id, rating, rated_at),
        )


def insert_watch(conn, user_id, item_id, watched_seconds, device_type, session_ended_at):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO watch_events (user_id, item_id, watched_seconds, device_type, session_ended_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (user_id, item_id, watched_seconds, device_type, session_ended_at),
        )


# ------------------------------------------------------------ user-rating-avg --

def test_user_rating_avg_computes_average_and_sweeps_stale_rows(reporting_db):
    job = load_job("user-rating-avg")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id, item1, item2 = f"zz-u-{_uid()}", f"zz-i-{_uid()}", f"zz-i-{_uid()}"
    insert_user(reporting_db, user_id)
    insert_item(reporting_db, item1)
    insert_item(reporting_db, item2)
    now = datetime.now(timezone.utc)
    insert_rating(reporting_db, user_id, item1, 4, now)
    insert_rating(reporting_db, user_id, item2, 2, now)
    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_rating_avg WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["avg_rating"] == pytest.approx(3.0)
        assert row["rating_count"] == 2

        # Deleting the items cascades their ratings away (FK ON DELETE
        # CASCADE) - a full sync on the next tick must then sweep the now-
        # stale aggregate row too, not leave it behind forever.
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([item1, item2],))
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_rating_avg WHERE user_id = %s", (user_id,))
            assert cur.fetchone() is None
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_rating_avg WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([item1, item2],))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


# ------------------------------------------------------------ item-rating-avg --

def test_item_rating_avg_computes_average_per_item(reporting_db):
    job = load_job("item-rating-avg")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    item_id, user1, user2 = f"zz-i-{_uid()}", f"zz-u-{_uid()}", f"zz-u-{_uid()}"
    insert_item(reporting_db, item_id)
    insert_user(reporting_db, user1)
    insert_user(reporting_db, user2)
    now = datetime.now(timezone.utc)
    insert_rating(reporting_db, user1, item_id, 5, now)
    insert_rating(reporting_db, user2, item_id, 3, now)
    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM item_rating_avg WHERE item_id = %s", (item_id,))
            row = cur.fetchone()
        assert row["avg_rating"] == pytest.approx(4.0)
        assert row["rating_count"] == 2
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM item_rating_avg WHERE item_id = %s", (item_id,))
            cur.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
            cur.execute("DELETE FROM users WHERE user_id = ANY(%s)", ([user1, user2],))


# ------------------------------------------------------------ trending-rankings --

def test_trending_rankings_filters_by_window_and_ranks_by_view_count(reporting_db, monkeypatch):
    monkeypatch.setenv("TRENDING_WINDOW_MINUTES", "1")
    monkeypatch.setenv("TRENDING_TOP_N", "10")
    job = load_job("trending-rankings")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_VIEW_RANKING_TABLE_SQL)
        cur.execute(job.ENSURE_RATING_RANKING_TABLE_SQL)

    popular, quiet, old = f"zz-i-{_uid()}", f"zz-i-{_uid()}", f"zz-i-{_uid()}"
    users = [f"zz-u-{_uid()}" for _ in range(3)]
    for item_id in (popular, quiet, old):
        insert_item(reporting_db, item_id, type="movie")
    for u in users:
        insert_user(reporting_db, u)

    now = datetime.now(timezone.utc)
    stale = now - timedelta(minutes=5)  # outside the 1-minute window
    insert_watch(reporting_db, users[0], popular, 100, "mobile", now)
    insert_watch(reporting_db, users[1], popular, 100, "mobile", now)
    insert_watch(reporting_db, users[2], quiet, 100, "mobile", now)
    insert_watch(reporting_db, users[0], old, 100, "mobile", stale)  # outside the window - must not appear at all

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM item_view_ranking WHERE type = 'movie' ORDER BY rank")
            rows = cur.fetchall()
        ranked_ids = [r["item_id"] for r in rows]
        assert ranked_ids[:2] == [popular, quiet]  # ranked by view_count desc
        assert old not in ranked_ids  # excluded entirely - it's outside the window
        assert rows[0]["view_count"] == 2
        assert rows[1]["view_count"] == 1
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM item_view_ranking WHERE item_id = ANY(%s)", ([popular, quiet, old],))
            cur.execute("DELETE FROM item_rating_ranking WHERE item_id = ANY(%s)", ([popular, quiet, old],))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([popular, quiet, old],))
            cur.execute("DELETE FROM users WHERE user_id = ANY(%s)", (users,))


# ------------------------------------------------------------ user-top-genre --

def test_user_top_genre_recency_window_excludes_older_watches(reporting_db, monkeypatch):
    monkeypatch.setenv("USER_TOP_GENRE_RECENT_N", "2")
    job = load_job("user-top-genre")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id = f"zz-u-{_uid()}"
    horror_items = [f"zz-i-{_uid()}" for _ in range(3)]
    comedy_items = [f"zz-i-{_uid()}" for _ in range(2)]
    insert_user(reporting_db, user_id)
    for item_id in horror_items + comedy_items:
        insert_item(reporting_db, item_id, genre_primary="Horror" if item_id in horror_items else "Comedy")

    now = datetime.now(timezone.utc)
    # Oldest-to-newest: Horror, Horror, Horror, Comedy, Comedy. With
    # RECENT_N=2, only the last 2 (Comedy, Comedy) count - Comedy wins.
    # If the window didn't apply, Horror (3 watches) would win instead -
    # that's what this test actually proves.
    insert_watch(reporting_db, user_id, horror_items[0], 100, "mobile", now - timedelta(minutes=5))
    insert_watch(reporting_db, user_id, horror_items[1], 100, "mobile", now - timedelta(minutes=4))
    insert_watch(reporting_db, user_id, horror_items[2], 100, "mobile", now - timedelta(minutes=3))
    insert_watch(reporting_db, user_id, comedy_items[0], 100, "mobile", now - timedelta(minutes=2))
    insert_watch(reporting_db, user_id, comedy_items[1], 100, "mobile", now - timedelta(minutes=1))

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_top_genre WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["top_genre"] == "Comedy"
        assert row["genre_watch_count"] == 2
        assert row["watches_used"] == 2  # the window size, not the total watch count (5)
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_top_genre WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", (horror_items + comedy_items,))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


def test_user_top_genre_ties_broken_by_genre_name_ascending(reporting_db, monkeypatch):
    monkeypatch.setenv("USER_TOP_GENRE_RECENT_N", "4")
    job = load_job("user-top-genre")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id = f"zz-u-{_uid()}"
    drama_items = [f"zz-i-{_uid()}" for _ in range(2)]
    comedy_items = [f"zz-i-{_uid()}" for _ in range(2)]
    insert_user(reporting_db, user_id)
    for item_id in drama_items:
        insert_item(reporting_db, item_id, genre_primary="Drama")
    for item_id in comedy_items:
        insert_item(reporting_db, item_id, genre_primary="Comedy")

    now = datetime.now(timezone.utc)
    # Last 4 watches: 2 Drama, 2 Comedy - a genuine 2-2 tie. "Comedy" sorts
    # before "Drama" ascending, so it must win.
    insert_watch(reporting_db, user_id, drama_items[0], 100, "mobile", now - timedelta(minutes=4))
    insert_watch(reporting_db, user_id, comedy_items[0], 100, "mobile", now - timedelta(minutes=3))
    insert_watch(reporting_db, user_id, drama_items[1], 100, "mobile", now - timedelta(minutes=2))
    insert_watch(reporting_db, user_id, comedy_items[1], 100, "mobile", now - timedelta(minutes=1))

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_top_genre WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["top_genre"] == "Comedy"
        assert row["genre_watch_count"] == 2
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_top_genre WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", (drama_items + comedy_items,))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


# ------------------------------------------------------------ item-completion-rate --

def test_item_completion_rate_threshold_boundary_for_movie_and_series(reporting_db):
    job = load_job("item-completion-rate")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    movie_id, series_id = f"zz-i-{_uid()}", f"zz-i-{_uid()}"
    users = [f"zz-u-{_uid()}" for _ in range(4)]
    insert_item(reporting_db, movie_id, type="movie", runtime_minutes=100)  # duration 6000s, threshold 5100s
    insert_item(reporting_db, series_id, type="series")  # duration DEFAULT_EPISODE_SECONDS=2400s, threshold 2040s
    for u in users:
        insert_user(reporting_db, u)

    now = datetime.now(timezone.utc)
    insert_watch(reporting_db, users[0], movie_id, 5100, "mobile", now)   # exactly at threshold - completed
    insert_watch(reporting_db, users[1], movie_id, 5099, "mobile", now)   # 1s short - not completed
    insert_watch(reporting_db, users[2], series_id, 2040, "mobile", now)  # exactly at threshold - completed
    insert_watch(reporting_db, users[3], series_id, 2039, "mobile", now)  # 1s short - not completed

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM item_completion_rate WHERE item_id = %s", (movie_id,))
            movie_row = cur.fetchone()
            cur.execute("SELECT * FROM item_completion_rate WHERE item_id = %s", (series_id,))
            series_row = cur.fetchone()
        assert movie_row["completion_rate"] == pytest.approx(0.5)
        assert movie_row["watch_count"] == 2
        assert series_row["completion_rate"] == pytest.approx(0.5)
        assert series_row["watch_count"] == 2
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM item_completion_rate WHERE item_id = ANY(%s)", ([movie_id, series_id],))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([movie_id, series_id],))
            cur.execute("DELETE FROM users WHERE user_id = ANY(%s)", (users,))


# ------------------------------------------------------------ item-viewer-demographics --

def test_item_viewer_demographics_dedupes_rewatches_and_percentages_sum_to_one(reporting_db):
    job = load_job("item-viewer-demographics")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    item_id = f"zz-i-{_uid()}"
    male_user, female_user, other_gender_user = (f"zz-u-{_uid()}" for _ in range(3))
    insert_item(reporting_db, item_id)
    insert_user(reporting_db, male_user, age=20, gender="male")
    insert_user(reporting_db, female_user, age=40, gender="female")
    insert_user(reporting_db, other_gender_user, age=30, gender="nonbinary")  # not male/female/other -> "unknown" bucket

    now = datetime.now(timezone.utc)
    insert_watch(reporting_db, male_user, item_id, 100, "mobile", now)
    insert_watch(reporting_db, male_user, item_id, 200, "mobile", now)  # rewatch - must not double-count this viewer
    insert_watch(reporting_db, female_user, item_id, 100, "mobile", now)
    insert_watch(reporting_db, other_gender_user, item_id, 100, "mobile", now)

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM item_viewer_demographics WHERE item_id = %s", (item_id,))
            row = cur.fetchone()
        assert row["viewer_count"] == 3  # 3 distinct viewers, not the 4 watch events
        assert row["average_age"] == pytest.approx((20 + 40 + 30) / 3)
        assert row["pct_male"] == pytest.approx(1 / 3)
        assert row["pct_female"] == pytest.approx(1 / 3)
        assert row["pct_other"] == pytest.approx(0.0)
        assert row["pct_unknown"] == pytest.approx(1 / 3)  # "nonbinary" isn't one of the 3 named buckets
        total = row["pct_male"] + row["pct_female"] + row["pct_other"] + row["pct_unknown"]
        assert total == pytest.approx(1.0)
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM item_viewer_demographics WHERE item_id = %s", (item_id,))
            cur.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
            cur.execute(
                "DELETE FROM users WHERE user_id = ANY(%s)",
                ([male_user, female_user, other_gender_user],),
            )


# ------------------------------------------------------------ user-mood --

def test_user_mood_boundary_asymmetry_and_recency_limit(reporting_db):
    job = load_job("user-mood")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    now = datetime.now(timezone.utc)
    created_items = []
    created_users = []

    def rated_user(values):
        """A fresh user with one rating per value in `values` (oldest
        first), each on its own item - ratings are PK'd (user_id, item_id),
        so a single user can't carry two ratings for the same item."""
        user_id = f"zz-u-{_uid()}"
        insert_user(reporting_db, user_id)
        created_users.append(user_id)
        for i, v in enumerate(values):
            item_id = f"zz-i-{_uid()}"
            insert_item(reporting_db, item_id)
            created_items.append(item_id)
            insert_rating(reporting_db, user_id, item_id, v, now - timedelta(minutes=len(values) - i))
        return user_id

    good_user = rated_user([5, 5, 5])          # avg 5.0 > 4.5 -> good
    boundary_high_user = rated_user([4, 5])    # avg exactly 4.5 -> NOT good (`>` is strict) -> average
    boundary_low_user = rated_user([3, 4])     # avg exactly 3.5 -> average (`>=` is inclusive)
    bad_user = rated_user([1, 2])              # avg 1.5 -> bad
    # 5 ratings oldest->newest: 1,1,1,5,5 - only the last 3 (1,5,5) should
    # count (avg 3.667 -> average). If all 5 counted instead, avg would be
    # 2.6 -> bad - that's what proves the recency limit is really applied.
    recency_user = rated_user([1, 1, 1, 5, 5])

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            def mood_of(user_id):
                cur.execute("SELECT * FROM user_mood WHERE user_id = %s", (user_id,))
                return cur.fetchone()

            assert mood_of(good_user)["mood"] == "good"
            assert mood_of(boundary_high_user)["mood"] == "average"
            assert mood_of(boundary_low_user)["mood"] == "average"
            assert mood_of(bad_user)["mood"] == "bad"
            recency_row = mood_of(recency_user)
            assert recency_row["ratings_used"] == 3
            assert recency_row["avg_recent_rating"] == pytest.approx((1 + 5 + 5) / 3)
            assert recency_row["mood"] == "average"
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_mood WHERE user_id = ANY(%s)", (created_users,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", (created_items,))
            cur.execute("DELETE FROM users WHERE user_id = ANY(%s)", (created_users,))


# ------------------------------------------------------------ user-series-movie-ratio --

def test_user_series_movie_ratio_computes_percentage_split(reporting_db):
    job = load_job("user-series-movie-ratio")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id = f"zz-u-{_uid()}"
    movie_id, series_id = f"zz-i-{_uid()}", f"zz-i-{_uid()}"
    insert_user(reporting_db, user_id)
    insert_item(reporting_db, movie_id, type="movie")
    insert_item(reporting_db, series_id, type="series")

    now = datetime.now(timezone.utc)
    insert_watch(reporting_db, user_id, movie_id, 100, "mobile", now)
    insert_watch(reporting_db, user_id, movie_id, 100, "mobile", now)  # rewatch counts again here (unlike demographics)
    insert_watch(reporting_db, user_id, movie_id, 100, "mobile", now)
    insert_watch(reporting_db, user_id, series_id, 100, "mobile", now)

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_series_movie_ratio WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["movie_watch_count"] == 3
        assert row["series_watch_count"] == 1
        assert row["movie_percentage"] == pytest.approx(0.75)
        assert row["series_percentage"] == pytest.approx(0.25)
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_series_movie_ratio WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([movie_id, series_id],))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


# ------------------------------------------------------------ user-top-device-language --

def test_user_top_device_language_device_tie_broken_by_recency_and_null_language_excluded_from_vote(reporting_db):
    job = load_job("user-top-device-language")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id = f"zz-u-{_uid()}"
    en_item, es_item, no_lang_item = (f"zz-i-{_uid()}" for _ in range(3))
    insert_user(reporting_db, user_id)
    insert_item(reporting_db, en_item, original_language="en")
    insert_item(reporting_db, es_item, original_language="es")
    insert_item(reporting_db, no_lang_item, original_language=None)

    now = datetime.now(timezone.utc)
    insert_watch(reporting_db, user_id, en_item, 100, "mobile", now - timedelta(minutes=4))
    insert_watch(reporting_db, user_id, en_item, 100, "mobile", now - timedelta(minutes=3))
    insert_watch(reporting_db, user_id, es_item, 100, "desktop", now - timedelta(minutes=2))
    insert_watch(reporting_db, user_id, no_lang_item, 100, "desktop", now - timedelta(minutes=1))

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_top_device_language WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        # mobile and desktop are tied 2-2 on raw count - desktop's most
        # recent watch (1 min ago) is later than mobile's (3 min ago), so
        # the recency tiebreak picks desktop over the raw-count tie.
        assert row["top_device"] == "desktop"
        assert row["device_watch_count"] == 2
        # en gets 2 votes (both en_item watches), es gets 1 - the
        # no_lang_item watch counted toward device above but cast no
        # language vote, so en wins outright.
        assert row["top_language"] == "en"
        assert row["language_watch_count"] == 2
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_top_device_language WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([en_item, es_item, no_lang_item],))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


# ------------------------------------------------------------ user-watch-count --

def test_user_watch_count_distinguishes_distinct_items_from_total_watches(reporting_db):
    job = load_job("user-watch-count")
    with reporting_db.cursor() as cur:
        cur.execute(job.ENSURE_TABLE_SQL)

    user_id = f"zz-u-{_uid()}"
    item1, item2 = f"zz-i-{_uid()}", f"zz-i-{_uid()}"
    insert_user(reporting_db, user_id)
    insert_item(reporting_db, item1)
    insert_item(reporting_db, item2)

    now = datetime.now(timezone.utc)
    insert_watch(reporting_db, user_id, item1, 100, "mobile", now)
    insert_watch(reporting_db, user_id, item1, 100, "mobile", now)  # rewatch of item1
    insert_watch(reporting_db, user_id, item1, 100, "mobile", now)  # a third watch of item1
    insert_watch(reporting_db, user_id, item2, 100, "mobile", now)

    try:
        job.run_tick()
        with reporting_db.cursor() as cur:
            cur.execute("SELECT * FROM user_watch_count WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["distinct_items_watched"] == 2  # item1 and item2, regardless of rewatches
        assert row["total_watches"] == 4            # every watch event counts, rewatches included
    finally:
        with reporting_db.cursor() as cur:
            cur.execute("DELETE FROM user_watch_count WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM items WHERE item_id = ANY(%s)", ([item1, item2],))
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
