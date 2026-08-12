"""Catalog Admin — small Flask/Jinja/Bootstrap frontend for the Catalog
Input Service. Lets an authenticated admin add/edit/delete rows directly in
catalog-db (item/app_user); changes flow through Debezium into Kafka
exactly like the seed script's inserts do — this is just another writer to
the same tables, not a separate data path.

Added at the user's request as a hands-on way to poke at catalog data
without psql — not part of the original architecture plan.
"""
import hmac
import os
from datetime import timedelta
from functools import wraps

import psycopg2
from psycopg2 import sql as psql
from flask import Flask, abort, flash, redirect, render_template, request, session, url_for
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError

from db import get_conn
import seed_catalog
from seed_catalog import next_id

app = Flask(__name__)
app.secret_key = os.environ.get("FRONTEND_SECRET_KEY", "dev-secret-change-me")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.permanent_session_lifetime = timedelta(hours=8)

csrf = CSRFProtect(app)

ADMIN_USERNAME = os.environ.get("FRONTEND_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("FRONTEND_ADMIN_PASSWORD", "changeme")

GENDERS = ["male", "female", "other", "unknown"]
SUBSCRIPTION_PLANS = ["basic", "standard", "premium", "premium_plus"]
PRIMARY_DEVICES = ["Desktop", "Laptop", "Tablet", "Mobile", "Smart TV", "Gaming Console"]
ACCOUNT_STATUSES = ["active", "suspended", "cancelled"]
ITEM_TYPES = ["movie", "series"]
CATALOG_STATUSES = ["active", "coming_soon", "archived"]
# Countries/languages are DB-driven (the `country`/`language` tables, same
# managed-vocabulary pattern as `genre` — see countries_languages_list())
# rather than a fixed module constant, since they're now admin-editable via
# the Countries & Languages page.

# Column-sort whitelists: user-supplied ?sort= values are matched against
# these before ever reaching SQL (psycopg2 can't parameterize identifiers,
# so an unvalidated column name would be a real injection vector).
ITEMS_SORT_COLUMNS = {
    # ids are prefix + digits (i5, i81, ...) — sort by the numeric part, not
    # lexically (lexical sort puts "i9" after "i81" since '8' < '9' as characters).
    "item_id": "(substring(i.item_id from 2))::int", "title": "i.title", "type": "i.type",
    "release_year": "i.release_year", "country": "i.country", "content_rating": "i.content_rating",
    "imdb_score": "i.imdb_score", "catalog_status": "i.catalog_status",
    "genre_primary": "i.genre_primary",
}
USERS_SORT_COLUMNS = {
    "user_id": "(substring(user_id from 2))::int", "age": "age", "gender": "gender", "occupation": "occupation",
    "country": "country", "subscription_plan": "subscription_plan", "account_status": "account_status",
}


def sort_params(sort_columns: dict, default: str):
    """Validates ?sort=/?dir= against a whitelist, returns (sort_key, sql_expr, direction)."""
    sort_key = request.args.get("sort", default)
    if sort_key not in sort_columns:
        sort_key = default
    direction = request.args.get("dir", "asc")
    if direction not in ("asc", "desc"):
        direction = "asc"
    return sort_key, sort_columns[sort_key], direction

# Mirrors seed/seed_catalog.py's OCCUPATION_MAP values (MovieLens 1M occupation
# codes) — duplicated here rather than imported since the seeder and this
# frontend are separate containers/services with no shared package.
OCCUPATIONS = [
    "other", "academic/educator", "artist", "clerical/admin", "college/grad student",
    "customer service", "doctor/health care", "executive/managerial", "farmer",
    "homemaker", "K-12 student", "lawyer", "programmer", "retired", "sales/marketing",
    "scientist", "self-employed", "technician/engineer", "tradesman/craftsman",
    "unemployed", "writer",
]

ITEM_INSERT_SQL = """
    INSERT INTO item (item_id, type, title, description, release_year, date_added,
        runtime_minutes, season_count, episode_count, content_rating, country,
        original_language, imdb_score, is_netflix_original, content_warning,
        genre_primary, genre_secondary, catalog_status)
    VALUES (%(item_id)s, %(type)s, %(title)s, %(description)s, %(release_year)s, %(date_added)s,
        %(runtime_minutes)s, %(season_count)s, %(episode_count)s, %(content_rating)s, %(country)s,
        %(original_language)s, %(imdb_score)s, %(is_netflix_original)s, %(content_warning)s,
        %(genre_primary)s, %(genre_secondary)s, %(catalog_status)s)
"""

ITEM_UPDATE_SQL = """
    UPDATE item SET type=%(type)s, title=%(title)s, description=%(description)s,
        release_year=%(release_year)s, date_added=%(date_added)s, runtime_minutes=%(runtime_minutes)s,
        season_count=%(season_count)s, episode_count=%(episode_count)s, content_rating=%(content_rating)s,
        country=%(country)s, original_language=%(original_language)s, imdb_score=%(imdb_score)s,
        is_netflix_original=%(is_netflix_original)s, content_warning=%(content_warning)s,
        genre_primary=%(genre_primary)s, genre_secondary=%(genre_secondary)s,
        catalog_status=%(catalog_status)s, updated_at=now()
    WHERE item_id = %(item_id)s
"""

USER_INSERT_SQL = """
    INSERT INTO app_user (user_id, age, gender, occupation, country,
        preferred_language, signup_date, subscription_plan, account_status,
        email, first_name, last_name, state_province, city,
        monthly_spend_hours, primary_device)
    VALUES (%(user_id)s, %(age)s, %(gender)s, %(occupation)s, %(country)s,
        %(preferred_language)s, %(signup_date)s, %(subscription_plan)s, %(account_status)s,
        %(email)s, %(first_name)s, %(last_name)s, %(state_province)s, %(city)s,
        %(monthly_spend_hours)s, %(primary_device)s)
"""

USER_UPDATE_SQL = """
    UPDATE app_user SET age=%(age)s, gender=%(gender)s, occupation=%(occupation)s,
        country=%(country)s, preferred_language=%(preferred_language)s,
        signup_date=%(signup_date)s, subscription_plan=%(subscription_plan)s,
        account_status=%(account_status)s, email=%(email)s, first_name=%(first_name)s,
        last_name=%(last_name)s, state_province=%(state_province)s, city=%(city)s,
        monthly_spend_hours=%(monthly_spend_hours)s, primary_device=%(primary_device)s,
        updated_at=now()
    WHERE user_id = %(user_id)s
"""


# ---------------------------------------------------------------- helpers --

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


def parse_int(raw):
    raw = (raw or "").strip()
    if not raw:
        return None, True
    try:
        return int(raw), True
    except ValueError:
        return None, False


def parse_float(raw):
    raw = (raw or "").strip()
    if not raw:
        return None, True
    try:
        return float(raw), True
    except ValueError:
        return None, False


def read_item_form(form, valid_genres, valid_countries, valid_languages):
    errors = []
    data = {"type": form.get("type", "").strip()}
    if data["type"] not in ITEM_TYPES:
        errors.append("Type must be movie or series.")

    data["title"] = form.get("title", "").strip()
    if not data["title"]:
        errors.append("Title is required.")

    data["description"] = form.get("description", "").strip() or None

    for field in ("release_year", "runtime_minutes", "season_count", "episode_count"):
        value, ok = parse_int(form.get(field))
        if not ok:
            errors.append(f"{field.replace('_', ' ').title()} must be a whole number.")
        data[field] = value

    data["date_added"] = form.get("date_added") or None
    data["content_rating"] = form.get("content_rating", "").strip() or None
    data["country"] = form.get("country", "").strip().upper() or None
    if data["country"] and data["country"] not in valid_countries:
        errors.append("Invalid country — pick one from the list or add it under Countries & Languages first.")
    data["original_language"] = form.get("original_language", "").strip().lower() or None
    if data["original_language"] and data["original_language"] not in valid_languages:
        errors.append("Invalid original language — pick one from the list or add it under Countries & Languages first.")

    value, ok = parse_float(form.get("imdb_score"))
    if not ok:
        errors.append("Imdb Score must be a number.")
    data["imdb_score"] = value

    data["is_netflix_original"] = form.get("is_netflix_original") == "on"
    data["content_warning"] = form.get("content_warning") == "on"

    data["genre_primary"] = form.get("genre_primary", "").strip() or None
    if data["genre_primary"] and data["genre_primary"] not in valid_genres:
        errors.append("Invalid primary genre — pick one from the list or add it under Genres first.")
    data["genre_secondary"] = form.get("genre_secondary", "").strip() or None
    if data["genre_secondary"] and data["genre_secondary"] not in valid_genres:
        errors.append("Invalid secondary genre — pick one from the list or add it under Genres first.")

    data["catalog_status"] = form.get("catalog_status", "active").strip()
    if data["catalog_status"] not in CATALOG_STATUSES:
        errors.append("Invalid catalog status.")

    return data, errors


def read_user_form(form, valid_countries, valid_languages):
    errors = []
    data = {}

    age, ok = parse_int(form.get("age"))
    if not ok:
        errors.append("Age must be a whole number.")
    data["age"] = age

    data["gender"] = form.get("gender", "").strip() or None
    data["occupation"] = form.get("occupation", "").strip() or None
    data["country"] = form.get("country", "").strip().upper() or None
    if data["country"] and data["country"] not in valid_countries:
        errors.append("Invalid country — pick one from the list or add it under Countries & Languages first.")
    data["preferred_language"] = form.get("preferred_language", "").strip().lower() or None
    if data["preferred_language"] and data["preferred_language"] not in valid_languages:
        errors.append("Invalid preferred language — pick one from the list or add it under Countries & Languages first.")
    data["signup_date"] = form.get("signup_date") or None

    data["subscription_plan"] = form.get("subscription_plan", "basic").strip()
    if data["subscription_plan"] not in SUBSCRIPTION_PLANS:
        errors.append("Invalid subscription plan.")

    data["email"] = form.get("email", "").strip() or None
    data["first_name"] = form.get("first_name", "").strip() or None
    data["last_name"] = form.get("last_name", "").strip() or None
    data["state_province"] = form.get("state_province", "").strip() or None
    data["city"] = form.get("city", "").strip() or None
    data["primary_device"] = form.get("primary_device", "").strip() or None

    value, ok = parse_float(form.get("monthly_spend_hours"))
    if not ok:
        errors.append("Monthly Spend Hours must be a number.")
    data["monthly_spend_hours"] = value

    data["account_status"] = form.get("account_status", "active").strip()
    if data["account_status"] not in ACCOUNT_STATUSES:
        errors.append("Invalid account status.")

    return data, errors


# ------------------------------------------------------------------- auth --

@app.errorhandler(CSRFError)
def handle_csrf_error(_e):
    flash("Your session expired — please sign in again.", "warning")
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if hmac.compare_digest(username, ADMIN_USERNAME) and hmac.compare_digest(password, ADMIN_PASSWORD):
            session.clear()
            session["authenticated"] = True
            session.permanent = True
            return redirect(request.args.get("next") or url_for("index"))
        flash("Invalid username or password.", "danger")
    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("Signed out.", "success")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) AS n FROM item")
        item_count = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM app_user")
        user_count = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM item WHERE catalog_status = 'active'")
        active_item_count = cur.fetchone()["n"]
        cur.execute(
            "SELECT type, count(*) AS n FROM item GROUP BY type ORDER BY type"
        )
        items_by_type = {row["type"]: row["n"] for row in cur.fetchall()}
        cur.execute("SELECT count(*) AS n FROM app_user WHERE account_status = 'active'")
        active_user_count = cur.fetchone()["n"]
        cur.execute(
            "SELECT gender, count(*) AS n FROM app_user GROUP BY gender ORDER BY gender"
        )
        users_by_gender = {(row["gender"] or "unspecified"): row["n"] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()
    return render_template(
        "dashboard.html",
        item_count=item_count, user_count=user_count,
        active_item_count=active_item_count, items_by_type=items_by_type,
        active_user_count=active_user_count, users_by_gender=users_by_gender,
    )


# ------------------------------------------------------------------ items --

@app.route("/items")
@login_required
def items_list():
    id_filter = request.args.get("id", "").strip()
    genre_filter = request.args.get("genre", "")
    type_filter = request.args.get("type", "")
    status_filter = request.args.get("status", "")
    sort_key, sort_expr, direction = sort_params(ITEMS_SORT_COLUMNS, "title")

    where_clauses = []
    params = []
    if id_filter:
        where_clauses.append("i.item_id ILIKE %s")
        params.append(f"%{id_filter}%")
    if type_filter in ITEM_TYPES:
        where_clauses.append("i.type = %s")
        params.append(type_filter)
    if status_filter in CATALOG_STATUSES:
        where_clauses.append("i.catalog_status = %s")
        params.append(status_filter)
    if genre_filter:
        where_clauses.append("(i.genre_primary = %s OR i.genre_secondary = %s)")
        params.extend([genre_filter, genre_filter])
    where_sql = psql.SQL("WHERE " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

    query = psql.SQL("SELECT i.* FROM item i {where} ORDER BY {sort} {dir}").format(
        where=where_sql, sort=psql.SQL(sort_expr), dir=psql.SQL(direction)
    )

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        items = cur.fetchall()

        # Distinct genre names (from either flat column) for the filter
        # dropdown, and per-option counts — always over the full unfiltered
        # table, not the current result set, so picking a filter doesn't
        # make the other dropdowns' counts a moving target.
        cur.execute(
            """SELECT genre, COUNT(DISTINCT item_id) AS n FROM (
                   SELECT item_id, genre_primary AS genre FROM item WHERE genre_primary IS NOT NULL
                   UNION ALL
                   SELECT item_id, genre_secondary AS genre FROM item WHERE genre_secondary IS NOT NULL
               ) g GROUP BY genre ORDER BY genre"""
        )
        genre_rows = cur.fetchall()
        all_genres = [{"name": row["genre"]} for row in genre_rows]
        genre_counts = {row["genre"]: row["n"] for row in genre_rows}
        cur.execute("SELECT type, COUNT(*) AS n FROM item GROUP BY type")
        type_counts = {row["type"]: row["n"] for row in cur.fetchall()}
        cur.execute("SELECT catalog_status, COUNT(*) AS n FROM item GROUP BY catalog_status")
        status_counts = {row["catalog_status"]: row["n"] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()
    return render_template(
        "items/list.html", items=items, all_genres=all_genres,
        id_filter=id_filter, genre_filter=genre_filter, type_filter=type_filter, status_filter=status_filter,
        item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES,
        sort=sort_key, direction=direction,
        genre_counts=genre_counts, type_counts=type_counts, status_counts=status_counts,
    )


@app.route("/items/new", methods=["GET", "POST"])
@login_required
def items_new():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM genre ORDER BY name")
        genre_names = [row["name"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM country ORDER BY code")
        country_codes = [row["code"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM language ORDER BY code")
        language_codes = [row["code"] for row in cur.fetchall()]

        if request.method == "POST":
            data, errors = read_item_form(request.form, set(genre_names), set(country_codes), set(language_codes))
            if errors:
                for e in errors:
                    flash(e, "danger")
                return render_template(
                    "items/form.html", item=data, mode="new",
                    item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
                )
            data["item_id"] = next_id(cur, "item", "item_id", "i")
            try:
                cur.execute(ITEM_INSERT_SQL, data)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                flash(f"Could not save item: {e.pgerror or e}", "danger")
                return render_template(
                    "items/form.html", item=data, mode="new",
                    item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
                )
            flash(f'Item "{data["title"]}" added ({data["item_id"]}).', "success")
            return redirect(url_for("items_list"))

        return render_template(
            "items/form.html", item={}, mode="new",
            item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
        )
    finally:
        cur.close()
        conn.close()


@app.route("/items/<item_id>/edit", methods=["GET", "POST"])
@login_required
def items_edit(item_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM genre ORDER BY name")
        genre_names = [row["name"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM country ORDER BY code")
        country_codes = [row["code"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM language ORDER BY code")
        language_codes = [row["code"] for row in cur.fetchall()]

        if request.method == "POST":
            data, errors = read_item_form(request.form, set(genre_names), set(country_codes), set(language_codes))
            data["item_id"] = item_id
            if errors:
                for e in errors:
                    flash(e, "danger")
                return render_template(
                    "items/form.html", item=data, mode="edit",
                    item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
                )
            try:
                cur.execute(ITEM_UPDATE_SQL, data)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                flash(f"Could not save item: {e.pgerror or e}", "danger")
                return render_template(
                    "items/form.html", item=data, mode="edit",
                    item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
                )
            flash(f'Item "{data["title"]}" updated.', "success")
            return redirect(url_for("items_list"))

        cur.execute("SELECT * FROM item WHERE item_id = %s", (item_id,))
        item = cur.fetchone()
        if item is None:
            abort(404)
        return render_template(
            "items/form.html", item=item, mode="edit",
            item_types=ITEM_TYPES, catalog_statuses=CATALOG_STATUSES, genres=genre_names,
                    countries=country_codes, languages=language_codes,
        )
    finally:
        cur.close()
        conn.close()


@app.route("/items/<item_id>/delete", methods=["POST"])
@login_required
def items_delete(item_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM item WHERE item_id = %s", (item_id,))
        conn.commit()
        flash(f"Item {item_id} deleted.", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("items_list"))


@app.route("/items/bulk-delete", methods=["POST"])
@login_required
def items_bulk_delete():
    ids = request.form.getlist("selected_ids")
    if not ids:
        flash("No items selected.", "warning")
        return redirect(url_for("items_list"))
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM item WHERE item_id = ANY(%s)", (ids,))
        deleted = cur.rowcount
        conn.commit()
        flash(f"Deleted {deleted} item(s).", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("items_list"))


@app.route("/items/random", methods=["POST"])
@login_required
def items_random():
    # Synthetic — not from the seed()/AlreadySeededError path, since a
    # one-off random item is meant to be repeatable on every click, same as
    # every other admin action on this page.
    conn = get_conn()
    cur = conn.cursor()
    try:
        item_id, title, genre_count = seed_catalog.create_random_item(cur)
        conn.commit()
        genre_note = f", {genre_count} new genre(s)" if genre_count else ""
        flash(f'Random item "{title}" added ({item_id}){genre_note}.', "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Could not create random item: {e.pgerror or e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("items_list"))


# ----------------------------------------------------------------- genres --
# Managed vocabulary powering the item form's genre_primary/genre_secondary
# dropdowns (those stay flat text columns on item — see 01_schema.sql). No
# edit/rename route — a genre is just its name, so renaming is delete + add.

@app.route("/genres")
@login_required
def genres_list():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT g.name, COUNT(DISTINCT i.item_id) AS item_count
               FROM genre g
               LEFT JOIN item i ON i.genre_primary = g.name OR i.genre_secondary = g.name
               GROUP BY g.name ORDER BY g.name"""
        )
        genres = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return render_template("genres/list.html", genres=genres)


@app.route("/genres/new", methods=["GET", "POST"])
@login_required
def genres_new():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if not name:
            flash("Genre name is required.", "danger")
            return render_template("genres/form.html", genre={"name": name})
        if len(name) > 50:
            flash("Genre name must be 50 characters or fewer.", "danger")
            return render_template("genres/form.html", genre={"name": name})
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO genre (name) VALUES (%s)", (name,))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            flash(f'Genre "{name}" already exists.', "warning")
            return render_template("genres/form.html", genre={"name": name})
        except psycopg2.Error as e:
            conn.rollback()
            flash(f"Could not save genre: {e.pgerror or e}", "danger")
            return render_template("genres/form.html", genre={"name": name})
        finally:
            cur.close()
            conn.close()
        flash(f'Genre "{name}" added.', "success")
        return redirect(url_for("genres_list"))
    return render_template("genres/form.html", genre={})


def _delete_genres(cur, names):
    """Deletes the given genre names and clears genre_primary/genre_secondary
    on every item that referenced them — confirmed 2026-08-10: a genre
    delete is a bulk cleanup, not blocked by in-use items and not a silent
    orphan (items don't keep an off-list genre value behind the scenes)."""
    cur.execute("UPDATE item SET genre_primary = NULL WHERE genre_primary = ANY(%s)", (names,))
    cur.execute("UPDATE item SET genre_secondary = NULL WHERE genre_secondary = ANY(%s)", (names,))
    cur.execute("DELETE FROM genre WHERE name = ANY(%s)", (names,))
    return cur.rowcount


@app.route("/genres/<name>/delete", methods=["POST"])
@login_required
def genres_delete(name):
    conn = get_conn()
    cur = conn.cursor()
    try:
        _delete_genres(cur, [name])
        conn.commit()
        flash(f'Genre "{name}" deleted (cleared from any items that used it).', "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("genres_list"))


@app.route("/genres/bulk-delete", methods=["POST"])
@login_required
def genres_bulk_delete():
    names = request.form.getlist("selected_names")
    if not names:
        flash("No genres selected.", "warning")
        return redirect(url_for("genres_list"))
    conn = get_conn()
    cur = conn.cursor()
    try:
        deleted = _delete_genres(cur, names)
        conn.commit()
        flash(f"Deleted {deleted} genre(s) (cleared from any items that used them).", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("genres_list"))


# ------------------------------------------------------- countries/languages --
# Same managed-vocabulary pattern as genres above, but country/language show
# up on both item and app_user (item.country/original_language,
# app_user.country/preferred_language) rather than just item, so deletes
# clear the field on both tables. No edit/rename route, same reasoning as
# genres — a code is just its code, renaming is delete + add.

@app.route("/countries-languages")
@login_required
def countries_languages_list():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT c.code,
                      (SELECT COUNT(*) FROM item WHERE country = c.code)
                      + (SELECT COUNT(*) FROM app_user WHERE country = c.code) AS usage_count
               FROM country c ORDER BY c.code"""
        )
        countries = cur.fetchall()
        cur.execute(
            """SELECT l.code,
                      (SELECT COUNT(*) FROM item WHERE original_language = l.code)
                      + (SELECT COUNT(*) FROM app_user WHERE preferred_language = l.code) AS usage_count
               FROM language l ORDER BY l.code"""
        )
        languages = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return render_template("countries_languages/list.html", countries=countries, languages=languages)


@app.route("/countries/new", methods=["GET", "POST"])
@login_required
def countries_new():
    if request.method == "POST":
        code = request.form.get("code", "").strip().upper()
        if not code:
            flash("Country code is required.", "danger")
            return render_template("countries_languages/country_form.html", code=code)
        if len(code) > 2:
            flash("Country code must be 2 characters or fewer.", "danger")
            return render_template("countries_languages/country_form.html", code=code)
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO country (code) VALUES (%s)", (code,))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            flash(f'Country "{code}" already exists.', "warning")
            return render_template("countries_languages/country_form.html", code=code)
        except psycopg2.Error as e:
            conn.rollback()
            flash(f"Could not save country: {e.pgerror or e}", "danger")
            return render_template("countries_languages/country_form.html", code=code)
        finally:
            cur.close()
            conn.close()
        flash(f'Country "{code}" added.', "success")
        return redirect(url_for("countries_languages_list"))
    return render_template("countries_languages/country_form.html", code="")


@app.route("/languages/new", methods=["GET", "POST"])
@login_required
def languages_new():
    if request.method == "POST":
        code = request.form.get("code", "").strip().lower()
        if not code:
            flash("Language code is required.", "danger")
            return render_template("countries_languages/language_form.html", code=code)
        if len(code) > 10:
            flash("Language code must be 10 characters or fewer.", "danger")
            return render_template("countries_languages/language_form.html", code=code)
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO language (code) VALUES (%s)", (code,))
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            flash(f'Language "{code}" already exists.', "warning")
            return render_template("countries_languages/language_form.html", code=code)
        except psycopg2.Error as e:
            conn.rollback()
            flash(f"Could not save language: {e.pgerror or e}", "danger")
            return render_template("countries_languages/language_form.html", code=code)
        finally:
            cur.close()
            conn.close()
        flash(f'Language "{code}" added.', "success")
        return redirect(url_for("countries_languages_list"))
    return render_template("countries_languages/language_form.html", code="")


def _delete_countries(cur, codes):
    """Deletes the given country codes and clears country on every item/
    app_user row that referenced them — same bulk-cleanup convention as
    _delete_genres()."""
    cur.execute("UPDATE item SET country = NULL WHERE country = ANY(%s)", (codes,))
    cur.execute("UPDATE app_user SET country = NULL WHERE country = ANY(%s)", (codes,))
    cur.execute("DELETE FROM country WHERE code = ANY(%s)", (codes,))
    return cur.rowcount


def _delete_languages(cur, codes):
    cur.execute("UPDATE item SET original_language = NULL WHERE original_language = ANY(%s)", (codes,))
    cur.execute("UPDATE app_user SET preferred_language = NULL WHERE preferred_language = ANY(%s)", (codes,))
    cur.execute("DELETE FROM language WHERE code = ANY(%s)", (codes,))
    return cur.rowcount


@app.route("/countries/<code>/delete", methods=["POST"])
@login_required
def countries_delete(code):
    conn = get_conn()
    cur = conn.cursor()
    try:
        _delete_countries(cur, [code])
        conn.commit()
        flash(f'Country "{code}" deleted (cleared from any items/users that used it).', "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("countries_languages_list"))


@app.route("/languages/<code>/delete", methods=["POST"])
@login_required
def languages_delete(code):
    conn = get_conn()
    cur = conn.cursor()
    try:
        _delete_languages(cur, [code])
        conn.commit()
        flash(f'Language "{code}" deleted (cleared from any items/users that used it).', "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("countries_languages_list"))


@app.route("/countries/bulk-delete", methods=["POST"])
@login_required
def countries_bulk_delete():
    codes = request.form.getlist("selected_codes")
    if not codes:
        flash("No countries selected.", "warning")
        return redirect(url_for("countries_languages_list"))
    conn = get_conn()
    cur = conn.cursor()
    try:
        deleted = _delete_countries(cur, codes)
        conn.commit()
        flash(f"Deleted {deleted} countr{'y' if deleted == 1 else 'ies'} (cleared from any items/users that used them).", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("countries_languages_list"))


@app.route("/languages/bulk-delete", methods=["POST"])
@login_required
def languages_bulk_delete():
    codes = request.form.getlist("selected_codes")
    if not codes:
        flash("No languages selected.", "warning")
        return redirect(url_for("countries_languages_list"))
    conn = get_conn()
    cur = conn.cursor()
    try:
        deleted = _delete_languages(cur, codes)
        conn.commit()
        flash(f"Deleted {deleted} language(s) (cleared from any items/users that used them).", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("countries_languages_list"))


# ------------------------------------------------------------------ users --

@app.route("/users")
@login_required
def users_list():
    id_filter = request.args.get("id", "").strip()
    gender_filter = request.args.get("gender", "")
    plan_filter = request.args.get("plan", "")
    status_filter = request.args.get("status", "")
    sort_key, sort_expr, direction = sort_params(USERS_SORT_COLUMNS, "user_id")

    where_clauses = []
    params = []
    if id_filter:
        where_clauses.append("user_id ILIKE %s")
        params.append(f"%{id_filter}%")
    if gender_filter in GENDERS:
        where_clauses.append("gender = %s")
        params.append(gender_filter)
    if plan_filter in SUBSCRIPTION_PLANS:
        where_clauses.append("subscription_plan = %s")
        params.append(plan_filter)
    if status_filter in ACCOUNT_STATUSES:
        where_clauses.append("account_status = %s")
        params.append(status_filter)
    where_sql = psql.SQL("WHERE " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

    query = psql.SQL("SELECT * FROM app_user {where} ORDER BY {sort} {dir}").format(
        where=where_sql, sort=psql.SQL(sort_expr), dir=psql.SQL(direction)
    )

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        users = cur.fetchall()

        # Per-option counts for the filter dropdowns — over the full
        # unfiltered table, same reasoning as items_list.
        cur.execute("SELECT gender, COUNT(*) AS n FROM app_user GROUP BY gender")
        gender_counts = {row["gender"]: row["n"] for row in cur.fetchall()}
        cur.execute("SELECT subscription_plan, COUNT(*) AS n FROM app_user GROUP BY subscription_plan")
        plan_counts = {row["subscription_plan"]: row["n"] for row in cur.fetchall()}
        cur.execute("SELECT account_status, COUNT(*) AS n FROM app_user GROUP BY account_status")
        status_counts = {row["account_status"]: row["n"] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()
    return render_template(
        "users/list.html", users=users, id_filter=id_filter,
        gender_filter=gender_filter, plan_filter=plan_filter, status_filter=status_filter,
        genders=GENDERS, plans=SUBSCRIPTION_PLANS, statuses=ACCOUNT_STATUSES,
        sort=sort_key, direction=direction,
        gender_counts=gender_counts, plan_counts=plan_counts, status_counts=status_counts,
    )


@app.route("/users/new", methods=["GET", "POST"])
@login_required
def users_new():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT code FROM country ORDER BY code")
        country_codes = [row["code"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM language ORDER BY code")
        language_codes = [row["code"] for row in cur.fetchall()]
        form_kwargs = dict(genders=GENDERS, occupations=OCCUPATIONS,
                            plans=SUBSCRIPTION_PLANS, statuses=ACCOUNT_STATUSES, devices=PRIMARY_DEVICES,
                            countries=country_codes, languages=language_codes)
        if request.method == "POST":
            data, errors = read_user_form(request.form, set(country_codes), set(language_codes))
            if errors:
                for e in errors:
                    flash(e, "danger")
                return render_template("users/form.html", user=data, mode="new", **form_kwargs)
            try:
                data["user_id"] = next_id(cur, "app_user", "user_id", "u")
                cur.execute(USER_INSERT_SQL, data)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                flash(f"Could not save user: {e.pgerror or e}", "danger")
                return render_template("users/form.html", user=data, mode="new", **form_kwargs)
            flash(f'User added ({data["user_id"]}).', "success")
            return redirect(url_for("users_list"))
        return render_template("users/form.html", user={}, mode="new", **form_kwargs)
    finally:
        cur.close()
        conn.close()


@app.route("/users/<user_id>/edit", methods=["GET", "POST"])
@login_required
def users_edit(user_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT code FROM country ORDER BY code")
        country_codes = [row["code"] for row in cur.fetchall()]
        cur.execute("SELECT code FROM language ORDER BY code")
        language_codes = [row["code"] for row in cur.fetchall()]
        form_kwargs = dict(genders=GENDERS, occupations=OCCUPATIONS,
                            plans=SUBSCRIPTION_PLANS, statuses=ACCOUNT_STATUSES, devices=PRIMARY_DEVICES,
                            countries=country_codes, languages=language_codes)
        if request.method == "POST":
            data, errors = read_user_form(request.form, set(country_codes), set(language_codes))
            data["user_id"] = user_id
            if errors:
                for e in errors:
                    flash(e, "danger")
                return render_template("users/form.html", user=data, mode="edit", **form_kwargs)
            try:
                cur.execute(USER_UPDATE_SQL, data)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                flash(f"Could not save user: {e.pgerror or e}", "danger")
                return render_template("users/form.html", user=data, mode="edit", **form_kwargs)
            flash("User updated.", "success")
            return redirect(url_for("users_list"))

        cur.execute("SELECT * FROM app_user WHERE user_id = %s", (user_id,))
        user = cur.fetchone()
        if user is None:
            abort(404)
        return render_template("users/form.html", user=user, mode="edit", **form_kwargs)
    finally:
        cur.close()
        conn.close()


@app.route("/users/<user_id>/delete", methods=["POST"])
@login_required
def users_delete(user_id):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM app_user WHERE user_id = %s", (user_id,))
        conn.commit()
        flash(f"User {user_id} deleted.", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("users_list"))


@app.route("/users/bulk-delete", methods=["POST"])
@login_required
def users_bulk_delete():
    ids = request.form.getlist("selected_ids")
    if not ids:
        flash("No users selected.", "warning")
        return redirect(url_for("users_list"))
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM app_user WHERE user_id = ANY(%s)", (ids,))
        deleted = cur.rowcount
        conn.commit()
        flash(f"Deleted {deleted} user(s).", "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("users_list"))


@app.route("/users/random", methods=["POST"])
@login_required
def users_random():
    conn = get_conn()
    cur = conn.cursor()
    try:
        user_id = seed_catalog.create_random_user(cur)
        conn.commit()
        flash(f"Random user {user_id} added.", "success")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Could not create random user: {e.pgerror or e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("users_list"))


# ------------------------------------------------------------------- seed --
# Seeding is deliberately NOT automatic — it's an explicit action triggered
# from here, so the admin has full control over what data exists (per the
# user's request: "activate a seed so it's not always there by default").

@app.route("/seed")
@login_required
def seed_page():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) AS n FROM item")
        item_count = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM app_user")
        user_count = cur.fetchone()["n"]
        history = seed_catalog.get_seed_history(cur)
        # Per seed_source item/user counts — power the "Unseed" button (only
        # shown/enabled for a source that actually has rows right now).
        cur.execute(
            "SELECT seed_source, COUNT(*) AS n FROM item WHERE seed_source IS NOT NULL GROUP BY seed_source"
        )
        seeded_item_counts = {row["seed_source"]: row["n"] for row in cur.fetchall()}
        cur.execute(
            "SELECT seed_source, COUNT(*) AS n FROM app_user WHERE seed_source IS NOT NULL GROUP BY seed_source"
        )
        seeded_user_counts = {row["seed_source"]: row["n"] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    seed_groups = seed_catalog.categorized_available_seeds()
    named_seeds = seed_catalog.available_seeds()
    seed_previews = {name: seed_catalog.preview_named_seed(name) for name in named_seeds}
    real_preview = seed_catalog.preview_real_data()

    return render_template(
        "seed.html",
        item_count=item_count, user_count=user_count,
        seed_groups=seed_groups,
        seed_previews=seed_previews,
        real_preview=real_preview,
        real_data_available=real_preview is not None,
        history=history,
        seeded_user_counts=seeded_user_counts,
        seeded_item_counts=seeded_item_counts,
    )


@app.route("/seed/real", methods=["POST"])
@login_required
def seed_real():
    force = request.form.get("confirm") == "yes"
    conn = get_conn()
    cur = conn.cursor()
    try:
        item_count, skipped_count, user_count, genre_count = seed_catalog.seed(cur, real=True, force=force)
        conn.commit()
        skip_note = f" ({skipped_count} already in catalog, skipped)" if skipped_count else ""
        genre_note = f", {genre_count} new genre(s)" if genre_count else ""
        flash(f"Seeded from real data: {item_count} items{skip_note}, {user_count} users{genre_note}.", "success")
    except seed_catalog.AlreadySeededError as e:
        conn.rollback()
        flash(str(e), "warning")
    except FileNotFoundError as e:
        conn.rollback()
        flash(str(e), "danger")
    except Exception as e:
        conn.rollback()
        flash(f"Seeding failed: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


@app.route("/seed/named/<name>", methods=["POST"])
@login_required
def seed_named(name):
    if name not in seed_catalog.available_seeds():
        abort(404)
    force = request.form.get("confirm") == "yes"
    conn = get_conn()
    cur = conn.cursor()
    try:
        item_count, skipped_count, user_count, genre_count = seed_catalog.seed(cur, name=name, force=force)
        conn.commit()
        skip_note = f" ({skipped_count} already in catalog, skipped)" if skipped_count else ""
        genre_note = f", {genre_count} new genre(s)" if genre_count else ""
        flash(f'Seeded from "{name}": {item_count} items{skip_note}, {user_count} users{genre_note}.', "success")
    except seed_catalog.AlreadySeededError as e:
        conn.rollback()
        flash(str(e), "warning")
    except Exception as e:
        conn.rollback()
        flash(f"Seeding failed: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


@app.route("/seed/named/<name>/<mode>", methods=["POST"])
@login_required
def seed_named_mode(name, mode):
    if name not in seed_catalog.available_seeds() or mode not in ("items", "users"):
        abort(404)
    force = request.form.get("confirm") == "yes"
    conn = get_conn()
    cur = conn.cursor()
    try:
        item_count, skipped_count, user_count, genre_count = seed_catalog.seed(cur, name=name, force=force, mode=mode)
        conn.commit()
        skip_note = f" ({skipped_count} already in catalog, skipped)" if skipped_count else ""
        genre_note = f", {genre_count} new genre(s)" if genre_count else ""
        flash(f'Seeded {mode} from "{name}": {item_count} items{skip_note}, {user_count} users{genre_note}.', "success")
    except seed_catalog.AlreadySeededError as e:
        conn.rollback()
        flash(str(e), "warning")
    except FileNotFoundError as e:
        conn.rollback()
        flash(str(e), "danger")
    except Exception as e:
        conn.rollback()
        flash(f"Seeding failed: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


@app.route("/seed/random_users", methods=["POST"])
@login_required
def seed_random_users_route():
    raw_count = request.form.get("count", "").strip()
    count = int(raw_count) if raw_count.isdigit() else None
    conn = get_conn()
    cur = conn.cursor()
    try:
        user_count = seed_catalog.seed_random_users(cur, count=count)
        conn.commit()
        flash(f"Generated {user_count} random synthetic users.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Seeding failed: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


@app.route("/seed/random_items", methods=["POST"])
@login_required
def seed_random_items_route():
    raw_count = request.form.get("count", "").strip()
    count = int(raw_count) if raw_count.isdigit() else None
    conn = get_conn()
    cur = conn.cursor()
    try:
        item_count, genre_count = seed_catalog.seed_random_items(cur, count=count)
        conn.commit()
        genre_note = f", {genre_count} new genre(s)" if genre_count else ""
        flash(f"Generated {item_count} random synthetic items{genre_note}.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Seeding failed: {e}", "danger")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


@app.route("/seed/unseed/<key>", methods=["POST"])
@login_required
def seed_unseed(key):
    """Removes every item/user tagged seed_source=<key> (a pack name, "real",
    "random_items", or "random_users" — see seed_catalog.py's
    seed_items()/create_random_item()/create_random_user()) and forgets
    that source was ever loaded, so it can be reseeded from scratch without
    the AlreadySeededError guard or leftover rows. Doesn't touch hand-added
    rows (seed_source NULL) or other sources' rows."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM item WHERE seed_source = %s", (key,))
        deleted_items = cur.rowcount
        cur.execute("DELETE FROM app_user WHERE seed_source = %s", (key,))
        deleted_users = cur.rowcount
        cur.execute(
            "DELETE FROM seed_log WHERE seed_name IN (%s, %s, %s)",
            (key, f"{key}:items", f"{key}:users"),
        )
        conn.commit()
        flash(f'Removed {deleted_items} item(s) and {deleted_users} user(s) seeded from "{key}".', "success")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("seed_page"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
