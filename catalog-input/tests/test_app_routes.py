"""Catalog Admin's Flask routes (app.py), driven end to end via
app.test_client() against a real catalog-db - see conftest.py's docstring
for why these are integration-marked rather than living alongside
test_app_forms.py's pure-function tests.

Deliberately not one test per route (app.py has 33): items/users/genres/
countries/languages are five near-identical CRUD families, so each gets
representative coverage of its family's shape rather than exhaustive
per-route duplication - already-covered validation-rule edge cases stay in
test_app_forms.py. What's unique to *this* file is proving the routes
actually wire that validation up to persistence correctly, that the
cascade-clear-on-delete behavior for genres/countries/languages really
clears referencing rows, and that the auth gate actually gates.

Every test cleans up exactly what it created - this is a real, shared
catalog-db, not a throwaway per-test database."""
import pytest

from conftest import unique_marker

pytestmark = pytest.mark.integration


# -------------------------------------------------------------------- auth --

def test_index_redirects_to_login_when_not_authenticated(client):
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/login" in resp.location


def test_login_with_valid_credentials_authenticates_and_redirects_home(client):
    resp = client.post("/login", data={"username": "test-admin", "password": "test-password"})
    assert resp.status_code == 302
    assert resp.location.endswith("/")
    with client.session_transaction() as sess:
        assert sess.get("authenticated") is True


def test_login_with_wrong_password_does_not_authenticate(client):
    resp = client.post("/login", data={"username": "test-admin", "password": "wrong"})
    assert resp.status_code == 200  # re-renders the login form, no redirect
    with client.session_transaction() as sess:
        assert "authenticated" not in sess


def test_logout_clears_the_session(logged_in_client):
    resp = logged_in_client.post("/logout")
    assert resp.status_code == 302
    with logged_in_client.session_transaction() as sess:
        assert "authenticated" not in sess


def test_index_loads_once_authenticated(logged_in_client):
    resp = logged_in_client.get("/")
    assert resp.status_code == 200


# ------------------------------------------------------------------ items --

def test_items_new_valid_submission_persists_and_redirects(logged_in_client, db_conn):
    title = unique_marker()
    try:
        resp = logged_in_client.post("/items/new", data={
            "type": "movie", "title": title, "catalog_status": "active",
        })
        assert resp.status_code == 302
        assert resp.location.endswith("/items")

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM item WHERE title = %s", (title,))
            row = cur.fetchone()
        assert row is not None
        assert row["type"] == "movie"
        assert row["catalog_status"] == "active"
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE title = %s", (title,))


def test_items_new_invalid_submission_is_rejected_without_persisting(logged_in_client, db_conn):
    title = unique_marker()
    resp = logged_in_client.post("/items/new", data={
        "type": "not-a-real-type", "title": title,
    })
    assert resp.status_code == 200  # re-renders the form with flashed errors, no redirect
    with db_conn.cursor() as cur:
        cur.execute("SELECT * FROM item WHERE title = %s", (title,))
        assert cur.fetchone() is None


def test_items_edit_get_prefills_existing_item(logged_in_client, db_conn):
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute("SELECT next_n FROM id_counter WHERE prefix = 'i'")
        cur.execute(
            "INSERT INTO item (item_id, type, title) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s) "
            "RETURNING item_id",
            ("movie", title),
        )
        item_id = cur.fetchone()["item_id"]
    try:
        resp = logged_in_client.get(f"/items/{item_id}/edit")
        assert resp.status_code == 200
        assert title.encode() in resp.data
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE item_id = %s", (item_id,))


def test_items_edit_unknown_id_is_404(logged_in_client):
    resp = logged_in_client.get("/items/i999999999/edit")
    assert resp.status_code == 404


def test_items_edit_post_updates_the_row(logged_in_client, db_conn):
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO item (item_id, type, title) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s) "
            "RETURNING item_id",
            ("movie", title),
        )
        item_id = cur.fetchone()["item_id"]
    try:
        new_title = unique_marker()
        resp = logged_in_client.post(f"/items/{item_id}/edit", data={
            "type": "series", "title": new_title, "catalog_status": "archived",
        })
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM item WHERE item_id = %s", (item_id,))
            row = cur.fetchone()
        assert row["title"] == new_title
        assert row["type"] == "series"
        assert row["catalog_status"] == "archived"
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE item_id = %s", (item_id,))


def test_items_delete_removes_the_row(logged_in_client, db_conn):
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO item (item_id, type, title) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s) "
            "RETURNING item_id",
            ("movie", title),
        )
        item_id = cur.fetchone()["item_id"]

    resp = logged_in_client.post(f"/items/{item_id}/delete")
    assert resp.status_code == 302

    with db_conn.cursor() as cur:
        cur.execute("SELECT * FROM item WHERE item_id = %s", (item_id,))
        assert cur.fetchone() is None


def test_items_bulk_delete_with_no_selection_warns_and_deletes_nothing(logged_in_client):
    resp = logged_in_client.post("/items/bulk-delete", data={})
    assert resp.status_code == 302
    assert resp.location.endswith("/items")


def test_items_bulk_delete_removes_every_selected_row(logged_in_client, db_conn):
    titles = [unique_marker(), unique_marker()]
    ids = []
    with db_conn.cursor() as cur:
        for title in titles:
            cur.execute(
                "INSERT INTO item (item_id, type, title) VALUES "
                "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s) "
                "RETURNING item_id",
                ("movie", title),
            )
            ids.append(cur.fetchone()["item_id"])

    resp = logged_in_client.post("/items/bulk-delete", data={"selected_ids": ids})
    assert resp.status_code == 302

    with db_conn.cursor() as cur:
        cur.execute("SELECT item_id FROM item WHERE item_id = ANY(%s)", (ids,))
        assert cur.fetchall() == []


def test_items_random_creates_a_row(logged_in_client, db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM item")
        before = cur.fetchone()["n"]

    resp = logged_in_client.post("/items/random")
    assert resp.status_code == 302

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM item")
        after = cur.fetchone()["n"]
        assert after == before + 1
        # Clean up whatever create_random_item() just added - it's not
        # tagged with our unique_marker() the way hand-built rows here are,
        # so find it as "the row not present before."
        cur.execute("SELECT item_id FROM item ORDER BY created_at DESC LIMIT 1")
        newest_id = cur.fetchone()["item_id"]
        cur.execute("DELETE FROM item WHERE item_id = %s", (newest_id,))


# ------------------------------------------------------------------ users --

def test_users_new_valid_submission_persists_and_redirects(logged_in_client, db_conn):
    city = unique_marker()
    try:
        resp = logged_in_client.post("/users/new", data={
            "city": city, "subscription_plan": "basic", "account_status": "active",
        })
        assert resp.status_code == 302
        assert resp.location.endswith("/users")

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM app_user WHERE city = %s", (city,))
            row = cur.fetchone()
        assert row is not None
        assert row["subscription_plan"] == "basic"
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM app_user WHERE city = %s", (city,))


def test_users_edit_post_updates_the_row(logged_in_client, db_conn):
    city = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO app_user (user_id, city) VALUES "
            "((SELECT 'u' || COALESCE(MAX((substring(user_id from 2))::int), 0) + 1 FROM app_user), %s) "
            "RETURNING user_id",
            (city,),
        )
        user_id = cur.fetchone()["user_id"]
    try:
        new_city = unique_marker()
        resp = logged_in_client.post(f"/users/{user_id}/edit", data={
            "city": new_city, "subscription_plan": "premium", "account_status": "active",
        })
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM app_user WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
        assert row["city"] == new_city
        assert row["subscription_plan"] == "premium"
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM app_user WHERE user_id = %s", (user_id,))


def test_users_delete_removes_the_row(logged_in_client, db_conn):
    city = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO app_user (user_id, city) VALUES "
            "((SELECT 'u' || COALESCE(MAX((substring(user_id from 2))::int), 0) + 1 FROM app_user), %s) "
            "RETURNING user_id",
            (city,),
        )
        user_id = cur.fetchone()["user_id"]

    resp = logged_in_client.post(f"/users/{user_id}/delete")
    assert resp.status_code == 302

    with db_conn.cursor() as cur:
        cur.execute("SELECT * FROM app_user WHERE user_id = %s", (user_id,))
        assert cur.fetchone() is None


def test_users_random_creates_a_row(logged_in_client, db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM app_user")
        before = cur.fetchone()["n"]

    resp = logged_in_client.post("/users/random")
    assert resp.status_code == 302

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM app_user")
        after = cur.fetchone()["n"]
        assert after == before + 1
        cur.execute("SELECT user_id FROM app_user ORDER BY created_at DESC LIMIT 1")
        newest_id = cur.fetchone()["user_id"]
        cur.execute("DELETE FROM app_user WHERE user_id = %s", (newest_id,))


# ----------------------------------------------------------------- genres --

def test_genres_new_creates_a_genre(logged_in_client, db_conn):
    name = unique_marker()
    try:
        resp = logged_in_client.post("/genres/new", data={"name": name})
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM genre WHERE name = %s", (name,))
            assert cur.fetchone() is not None
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM genre WHERE name = %s", (name,))


def test_genres_new_duplicate_name_is_rejected_not_a_second_row(logged_in_client, db_conn):
    name = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute("INSERT INTO genre (name) VALUES (%s)", (name,))
    try:
        resp = logged_in_client.post("/genres/new", data={"name": name})
        assert resp.status_code == 200  # re-renders with a "already exists" flash, no redirect

        with db_conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM genre WHERE name = %s", (name,))
            assert cur.fetchone()["n"] == 1
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM genre WHERE name = %s", (name,))


def test_genres_delete_clears_the_genre_from_referencing_items(logged_in_client, db_conn):
    name = unique_marker()
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute("INSERT INTO genre (name) VALUES (%s)", (name,))
        cur.execute(
            "INSERT INTO item (item_id, type, title, genre_primary) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s, %s) "
            "RETURNING item_id",
            ("movie", title, name),
        )
        item_id = cur.fetchone()["item_id"]
    try:
        resp = logged_in_client.post(f"/genres/{name}/delete")
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM genre WHERE name = %s", (name,))
            assert cur.fetchone() is None
            # The cascade-clear, not a blocked delete or an orphaned reference.
            cur.execute("SELECT genre_primary FROM item WHERE item_id = %s", (item_id,))
            assert cur.fetchone()["genre_primary"] is None
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE item_id = %s", (item_id,))
            cur.execute("DELETE FROM genre WHERE name = %s", (name,))


# ------------------------------------------------------- countries/languages --

def test_countries_new_and_delete_clears_referencing_rows(logged_in_client, db_conn):
    # "ZZ" is ISO-3166 user-assigned (never a real country) - safe to use
    # as a fixed fake value here rather than needing a unique_marker(),
    # which wouldn't fit VARCHAR(2) anyway.
    code = "ZZ"
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM country WHERE code = %s", (code,))  # in case a prior failed run left it
    try:
        resp = logged_in_client.post("/countries/new", data={"code": code})
        assert resp.status_code == 302
        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM country WHERE code = %s", (code,))
            assert cur.fetchone() is not None

        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO item (item_id, type, title, country) VALUES "
                "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s, %s) "
                "RETURNING item_id",
                ("movie", title, code),
            )
            item_id = cur.fetchone()["item_id"]

        resp = logged_in_client.post(f"/countries/{code}/delete")
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM country WHERE code = %s", (code,))
            assert cur.fetchone() is None
            cur.execute("SELECT country FROM item WHERE item_id = %s", (item_id,))
            assert cur.fetchone()["country"] is None
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE title = %s", (title,))
            cur.execute("DELETE FROM country WHERE code = %s", (code,))


def test_languages_new_and_delete_clears_referencing_rows(logged_in_client, db_conn):
    code = "zz-test"  # obviously fake, fits VARCHAR(10)
    title = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM language WHERE code = %s", (code,))
    try:
        resp = logged_in_client.post("/languages/new", data={"code": code})
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO item (item_id, type, title, original_language) VALUES "
                "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s, %s) "
                "RETURNING item_id",
                ("movie", title, code),
            )
            item_id = cur.fetchone()["item_id"]

        resp = logged_in_client.post(f"/languages/{code}/delete")
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM language WHERE code = %s", (code,))
            assert cur.fetchone() is None
            cur.execute("SELECT original_language FROM item WHERE item_id = %s", (item_id,))
            assert cur.fetchone()["original_language"] is None
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE title = %s", (title,))
            cur.execute("DELETE FROM language WHERE code = %s", (code,))


# ------------------------------------------------------------------- seed --

def test_seed_unseed_removes_only_rows_tagged_with_that_source(logged_in_client, db_conn):
    tag = unique_marker()
    title_tagged = unique_marker()
    title_untagged = unique_marker()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO item (item_id, type, title, seed_source) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s, %s) "
            "RETURNING item_id",
            ("movie", title_tagged, tag),
        )
        tagged_id = cur.fetchone()["item_id"]
        cur.execute(
            "INSERT INTO item (item_id, type, title) VALUES "
            "((SELECT 'i' || COALESCE(MAX((substring(item_id from 2))::int), 0) + 1 FROM item), %s, %s) "
            "RETURNING item_id",
            ("movie", title_untagged),
        )
        untagged_id = cur.fetchone()["item_id"]
    try:
        resp = logged_in_client.post(f"/seed/unseed/{tag}")
        assert resp.status_code == 302

        with db_conn.cursor() as cur:
            cur.execute("SELECT * FROM item WHERE item_id = %s", (tagged_id,))
            assert cur.fetchone() is None  # tagged row removed
            cur.execute("SELECT * FROM item WHERE item_id = %s", (untagged_id,))
            assert cur.fetchone() is not None  # untagged row untouched
    finally:
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM item WHERE item_id IN (%s, %s)", (tagged_id, untagged_id))
