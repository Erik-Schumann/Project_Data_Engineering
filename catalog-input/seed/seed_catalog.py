"""Seeds the Catalog Database (item/app_user).

Two ways to seed, both importable by the Catalog Admin frontend
(catalog-input/frontend/app.py) so seeding is an explicit, on-demand admin
action from the UI rather than something that always runs automatically:

- seed_from_real_data(): the 2 Kaggle catalog datasets + MovieLens 1M,
  mounted at /data (see catalog-input/README.md for exact download
  commands).
- seed_from_named_set(cur, name): a curated, versioned seed set checked
  into this repo under seeds/<name>/ — a general set (seeds/large/, ~30
  titles, Soeiro/MovieLens shape) plus the "Netflix pack" family generated
  from data/movies.csv / data/users.csv: seeds/netflix_full/ (every row,
  unfiltered) and single-dimension filtered packs on top of it (genre,
  release year, duration, language, movie vs. series, content warning,
  user age). Real static/generated-once files, not runtime-generated data —
  each file's content is fixed and reviewable. available_seeds() lists
  what's on disk; categorize_seeds() groups each by whether it defines
  items, users, or both (see catalog-input/README.md).

IDs are DB-state-derived (next_id(): "highest existing number for this
prefix, plus one"). This is deliberate: with two independent writers (this
script and the frontend) potentially touching the same tables, ids MUST be
derived from actual DB state rather than each writer keeping its own
counter, or they could assign colliding ids.

Tradeoff this implies: app_user rows are NOT deduplicated across repeated
runs — loading the same seed set twice adds a second full batch of users
rather than updating the existing rows. item rows ARE deduplicated, but by a
natural key (title, release_year) rather than by source id, since seed packs
are expected to overlap in titles (see seed_items()/existing_item_keys()) —
activating a pack that shares titles with an already-seeded one just skips
those, it doesn't duplicate them. That's a deliberate simplification
matching this project's actual usage — seeding is an explicit,
admin-triggered action ("full control over the data"), not an idempotent
background sync.

Because users still aren't deduplicated, double-seeding still needs an
explicit guard: seed_log (a Postgres table, NOT part of the CDC'd domain
model — see 01_schema.sql) records every load, and seed() — the single
entry point both the CLI and the frontend actually call — raises
AlreadySeededError if that *exact* source was already loaded, unless
force=True. This is a separate check from item-level title dedup above —
see seed()'s docstring for how the two interact. preview_named_seed()/
preview_real_data() read a source's row/genre counts without touching the
DB, for showing "contains N items, M genres, K users" before the admin
commits to loading it (genres here means distinct genre names in the source
file, not a stored entity — see seed_items()).
"""
import argparse
import ast
import csv
import datetime
import os
import random

import psycopg2
from faker import Faker
from psycopg2 import sql

DATA_DIR = os.environ.get("DATA_DIR", "/data")
SEEDS_DIR = os.path.join(os.path.dirname(__file__), "seeds")

SOEIRO_PATH = os.path.join(DATA_DIR, "soeiro", "titles.csv")
SHIVAMB_PATH = os.path.join(DATA_DIR, "shivamb", "netflix_titles.csv")
MOVIELENS_PATH = os.path.join(DATA_DIR, "movielens", "users.dat")

# Neither Kaggle catalog source has a language field. original_language is
# derived from the item's primary production country as a rough proxy —
# documented simplification, not real per-title language data.
COUNTRY_LANGUAGE = {
    "US": "en", "GB": "en", "CA": "en", "AU": "en", "IE": "en",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es",
    "DE": "de", "AT": "de",
    "FR": "fr",
    "KR": "ko",
    "JP": "ja",
    "IN": "hi",
    "BR": "pt", "PT": "pt",
    "IT": "it",
}

# data/movies.csv and data/users.csv (see catalog-input/README.md's "Netflix
# pack" section) spell out country/language/gender/plan as free text rather
# than codes — these map that text onto the same vocab everything else in
# this module already uses (COUNTRY_LANGUAGE's ISO2 keys, SUBSCRIPTION_PLANS,
# the frontend's GENDERS list). Built from the exact distinct values found
# in those two files, not a general-purpose gazetteer — extend if a new
# source/pack introduces a country or language not covered here.
COUNTRY_NAME_TO_ISO2 = {
    "usa": "US", "uk": "GB", "canada": "CA", "south korea": "KR",
    "japan": "JP", "germany": "DE", "france": "FR", "india": "IN",
}
LANGUAGE_NAME_TO_CODE = {
    "english": "en", "spanish": "es", "french": "fr", "hindi": "hi",
    "japanese": "ja", "italian": "it", "korean": "ko", "german": "de",
}
GENDER_NAME_MAP = {
    "male": "male", "female": "female", "other": "other", "prefer not to say": "unknown",
}

# https://files.grouplens.org/datasets/movielens/ml-1m-README.txt
OCCUPATION_MAP = {
    "0": "other", "1": "academic/educator", "2": "artist", "3": "clerical/admin",
    "4": "college/grad student", "5": "customer service", "6": "doctor/health care",
    "7": "executive/managerial", "8": "farmer", "9": "homemaker", "10": "K-12 student",
    "11": "lawyer", "12": "programmer", "13": "retired", "14": "sales/marketing",
    "15": "scientist", "16": "self-employed", "17": "technician/engineer",
    "18": "tradesman/craftsman", "19": "unemployed", "20": "writer",
}

# --------------------------------------------------------- random generation --
# Synthetic data not backed by any real dataset — unlike everything above,
# which comes from a file (Kaggle CSVs or a checked-in seeds/<name>/ set).
# Used by the "random item"/"random user" buttons on the Catalog Admin
# frontend and the Seed Data page's bulk random-users generator. Deliberately
# NOT routed through seed()'s AlreadySeededError guard — repeating these
# actions is the point (same "click adds one, click again for another" model
# as client-input's "Open sessions now"), not a mistake to warn about.

RANDOM_USER_COUNT_RANGE = (10, 50)
RANDOM_ITEM_COUNT_RANGE = (5, 20)

GENDERS = ["male", "female", "other", "unknown"]
SUBSCRIPTION_PLANS = ["basic", "standard", "premium", "premium_plus"]
PRIMARY_DEVICES = ["Desktop", "Laptop", "Tablet", "Mobile", "Smart TV", "Gaming Console"]
ITEM_TYPES = ["movie", "series"]
CONTENT_RATINGS = ["G", "PG", "PG-13", "R", "NC-17", "TV-Y", "TV-PG", "TV-14", "TV-MA"]
GENRE_NAME_POOL = [
    "drama", "comedy", "thriller", "action", "documentary", "romance",
    "horror", "sci-fi", "crime", "fantasy", "animation", "family",
]

# Faker for free-text fields (title, description) — genuinely varied,
# plausible-reading text beats a small hardcoded word list. Everything else
# (gender, occupation, country, ...) stays random.choice() over this
# module's existing controlled vocabularies, since those need to match what
# the rest of the app (filter dropdowns, CHECK constraints) expects.
_fake = Faker()


def random_user_fields(cur) -> dict:
    """Plausible-but-synthetic app_user field values (no user_id — the
    caller assigns that via next_id() at insert time, same as every other
    writer in this module). country/preferred_language are picked
    independently from the managed country/language tables (see
    available_countries()/available_languages()) rather than paired via
    COUNTRY_LANGUAGE — same reasoning random_item_fields() below already
    applies to genre_primary/genre_secondary: item.country and
    item.original_language (and the app_user equivalents) are separate,
    unenforced columns, so there's nothing to keep paired."""
    return {
        "age": random.randint(13, 85),
        "gender": random.choice(GENDERS),
        "occupation": random.choice(list(OCCUPATION_MAP.values())),
        "country": random.choice(available_countries(cur)),
        "preferred_language": random.choice(available_languages(cur)),
        "signup_date": datetime.date.today() - datetime.timedelta(days=random.randint(0, 1095)),
        "subscription_plan": random.choice(SUBSCRIPTION_PLANS),
        "account_status": "active",
        "email": _fake.email(),
        "first_name": _fake.first_name(),
        "last_name": _fake.last_name(),
        "state_province": _fake.state(),
        "city": _fake.city(),
        "monthly_spend_hours": round(random.uniform(0, 60), 2),
        "primary_device": random.choice(PRIMARY_DEVICES),
        "seed_source": "random_users",
    }


def available_genres(cur) -> list:
    """Genre names the random item generator may pick from: whatever's
    already in the managed `genre` table (Catalog Admin's Genres page),
    not a separate hardcoded list that could disagree with it — that's
    exactly how a random item previously ended up with genre_primary
    "action" (GENRE_NAME_POOL, lowercase) while every seeded item used
    "Action" (capitalized, from the CSV packs), registering as two
    distinct genres instead of one. Falls back to GENRE_NAME_POOL only
    when the table is genuinely empty (a fresh, unseeded database — the
    random generator still needs to work standalone). Cursor-agnostic,
    same reasoning as next_id()'s docstring above."""
    cur.execute("SELECT name FROM genre ORDER BY name")
    names = [row["name"] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
    return names or GENRE_NAME_POOL


def available_countries(cur) -> list:
    """Same reasoning as available_genres() above, for the managed `country`
    table. Falls back to COUNTRY_LANGUAGE's keys only when the table is
    genuinely empty."""
    cur.execute("SELECT code FROM country ORDER BY code")
    codes = [row["code"] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
    return codes or list(COUNTRY_LANGUAGE.keys())


def available_languages(cur) -> list:
    """Same reasoning as available_genres() above, for the managed
    `language` table. Falls back to COUNTRY_LANGUAGE's values only when the
    table is genuinely empty."""
    cur.execute("SELECT code FROM language ORDER BY code")
    codes = [row["code"] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
    return codes or list(set(COUNTRY_LANGUAGE.values()))


def random_item_fields(cur) -> dict:
    """Plausible-but-synthetic item field values (no item_id — assigned by
    the caller). Title/description come from Faker — this is for
    exercising the pipeline with volume, not a content source, so "real-
    reading" text matters more than "real" text."""
    item_type = random.choice(ITEM_TYPES)
    # Faker has no dedicated "movie title" provider; catch_phrase() gives
    # short, varied, title-cased phrases that read closer to a title than
    # any of Faker's other text providers.
    title = _fake.catch_phrase()
    genre_pool = available_genres(cur)
    genre_choices = random.sample(genre_pool, k=min(random.randint(1, 2), len(genre_pool)))
    return {
        "type": item_type,
        "title": title,
        "description": _fake.paragraph(nb_sentences=3),
        "release_year": random.randint(1980, 2026),
        "date_added": datetime.date.today() - datetime.timedelta(days=random.randint(0, 1095)),
        "runtime_minutes": random.randint(75, 180) if item_type == "movie" else None,
        "season_count": random.randint(1, 8) if item_type == "series" else None,
        "episode_count": random.randint(6, 24) if item_type == "series" else None,
        "content_rating": random.choice(CONTENT_RATINGS),
        "country": random.choice(available_countries(cur)),
        "original_language": random.choice(available_languages(cur)),
        "imdb_score": round(random.uniform(3.0, 9.5), 1),
        "is_netflix_original": random.choice([True, False]),
        "content_warning": random.choice([True, False]),
        "genre_primary": genre_choices[0],
        "genre_secondary": genre_choices[1] if len(genre_choices) > 1 else None,
        "catalog_status": "active",
        "seed_source": "random_items",
    }


def ensure_genres(cur, names) -> int:
    """Upserts each name into the managed `genre` table (Catalog Admin's
    Genres page / the item form's genre dropdowns — see 01_schema.sql).
    Called wherever an item gets a genre_primary/genre_secondary value, so
    the dropdown never lags behind what seeding actually introduced. Falsy
    entries (None from an item with no secondary genre) and duplicates are
    both fine — filtered here / handled by ON CONFLICT respectively.
    Returns how many were newly added (cur.rowcount after an ON CONFLICT DO
    NOTHING insert only counts rows actually inserted), so callers can
    report it alongside item/user counts."""
    names = [n for n in names if n]
    if not names:
        return 0
    cur.execute(
        "INSERT INTO genre (name) SELECT unnest(%s::varchar[]) ON CONFLICT (name) DO NOTHING",
        (names,),
    )
    return cur.rowcount


def sync_genres_from_items(cur) -> int:
    """Upserts every genre_primary/genre_secondary value currently on any
    item into `genre`. Used after the CSV-driven seed paths
    (seed_items()/seed_items_from_movies_csv(), reached via seed() below)
    instead of threading genre bookkeeping through their row loops — the
    item table is small enough (portfolio scale) that a full distinct-value
    scan after seeding is cheap. create_random_item() calls ensure_genres()
    directly instead, since it already knows the exact 1-2 names it used.
    Returns how many genres were newly added — see ensure_genres()."""
    cur.execute(
        """INSERT INTO genre (name)
           SELECT DISTINCT genre FROM (
               SELECT genre_primary AS genre FROM item WHERE genre_primary IS NOT NULL
               UNION
               SELECT genre_secondary AS genre FROM item WHERE genre_secondary IS NOT NULL
           ) g
           ON CONFLICT (name) DO NOTHING"""
    )
    return cur.rowcount


def ensure_countries(cur, codes) -> int:
    """Same as ensure_genres() above, for the managed `country` table."""
    codes = [c for c in codes if c]
    if not codes:
        return 0
    cur.execute(
        "INSERT INTO country (code) SELECT unnest(%s::varchar[]) ON CONFLICT (code) DO NOTHING",
        (codes,),
    )
    return cur.rowcount


def ensure_languages(cur, codes) -> int:
    """Same as ensure_genres() above, for the managed `language` table."""
    codes = [c for c in codes if c]
    if not codes:
        return 0
    cur.execute(
        "INSERT INTO language (code) SELECT unnest(%s::varchar[]) ON CONFLICT (code) DO NOTHING",
        (codes,),
    )
    return cur.rowcount


def sync_countries_languages_from_data(cur) -> tuple:
    """Upserts every country/language value currently on any item or
    app_user row into the `country`/`language` tables — same reasoning as
    sync_genres_from_items() above, but scanning both tables since
    country/language appear on item (country, original_language) AND
    app_user (country, preferred_language), unlike genre which is item-only.
    Returns (new_country_count, new_language_count)."""
    cur.execute(
        """INSERT INTO country (code)
           SELECT DISTINCT code FROM (
               SELECT country AS code FROM item WHERE country IS NOT NULL
               UNION
               SELECT country AS code FROM app_user WHERE country IS NOT NULL
           ) c
           ON CONFLICT (code) DO NOTHING"""
    )
    country_count = cur.rowcount
    cur.execute(
        """INSERT INTO language (code)
           SELECT DISTINCT code FROM (
               SELECT original_language AS code FROM item WHERE original_language IS NOT NULL
               UNION
               SELECT preferred_language AS code FROM app_user WHERE preferred_language IS NOT NULL
           ) l
           ON CONFLICT (code) DO NOTHING"""
    )
    language_count = cur.rowcount
    return country_count, language_count


def make_id_counter(cur, table: str, id_column: str, prefix: str):
    """Returns a callable that hands out sequential `prefix`+N ids from a
    persistent counter (id_counter, see postgres/init/01_schema.sql) rather
    than rescanning `table` for the current max on every call. Two reasons
    that used to be one:

    1. Performance — next_id()'s old approach did one full scan per id,
       O(n) per call, O(n^2) per batch: what made seeding netflix_full's
       10,300 users take ~90s. Scans the *counter* once here (a single-row
       lookup, not a table scan), then increments in memory.
    2. No id reuse — a scan-based max(existing)+1 reissues a previously-used
       id the moment the row holding that id gets deleted (e.g. `i5` deleted
       -> next scan's max drops below 5 -> `i5` handed out again to a
       completely different item). id_counter only ever increments, so a
       deleted id is retired for good, even across a full delete-all and
       reseed.

    Concurrency-safe the same way next_id() was meant to be but wasn't:
    takes a `pg_advisory_xact_lock` keyed on `prefix`, held until the
    calling transaction commits or rolls back. Without it, two concurrent
    writers (the CLI seeder and the frontend, or two overlapping seed
    requests) can both read the same counter value and hand out the same
    id — hit exactly this as a duplicate-key error on `item_pkey` when a
    background CLI seed and a frontend seed request ran at the same time.
    With the lock, the second writer blocks until the first commits, then
    its own read sees the first writer's incremented counter."""
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (prefix,))
    cur.execute(
        "CREATE TABLE IF NOT EXISTS id_counter (prefix TEXT PRIMARY KEY, next_n BIGINT NOT NULL DEFAULT 0)"
    )
    cur.execute("SELECT next_n FROM id_counter WHERE prefix = %s", (prefix,))
    row = cur.fetchone()
    if row is None:
        # First time this prefix has ever used the counter - bootstrap it
        # from whatever's already in `table`, exactly once, so it can't
        # reissue an id that already exists right now. Every call after
        # this one only ever reads/increments id_counter, never `table`.
        cur.execute(
            sql.SQL("SELECT {col} FROM {tbl} WHERE {col} LIKE %s").format(
                col=sql.Identifier(id_column), tbl=sql.Identifier(table)
            ),
            (f"{prefix}%",),
        )
        max_n = 0
        for existing_row in cur.fetchall():
            raw_id = existing_row[id_column] if isinstance(existing_row, dict) else existing_row[0]
            suffix = raw_id[len(prefix):]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        cur.execute(
            "INSERT INTO id_counter (prefix, next_n) VALUES (%s, %s)", (prefix, max_n)
        )
        state = {"n": max_n}
    else:
        state = {"n": row["next_n"] if isinstance(row, dict) else row[0]}

    def _next() -> str:
        state["n"] += 1
        cur.execute(
            "UPDATE id_counter SET next_n = %s WHERE prefix = %s", (state["n"], prefix)
        )
        return f"{prefix}{state['n']}"

    return _next


def next_id(cur, table: str, id_column: str, prefix: str) -> str:
    """Next sequential id for `prefix` (e.g. "i4"), derived from current DB
    state. Imported directly by the frontend (catalog-input/frontend/app.py)
    for its one-row-per-request admin forms. Batch callers (seed_items() and
    friends) use make_id_counter() instead — see its docstring for why.
    Cursor-agnostic: works with both a plain cursor (this script's own
    connections) and a RealDictCursor (the frontend's)."""
    return make_id_counter(cur, table, id_column, prefix)()


def create_random_user(cur, next_user_id=None) -> str:
    """Inserts one synthetic user with plausible-but-random field values.
    Returns the new user_id. Used by both the Catalog Admin frontend's
    "Random user" button (one at a time — `next_user_id` omitted, derives
    its own id via next_id()) and seed_random_users() (a batch — passes a
    shared make_id_counter() to avoid rescanning the table per row)."""
    user_id = (next_user_id or (lambda: next_id(cur, "app_user", "user_id", "u")))()
    fields = random_user_fields(cur)
    fields["user_id"] = user_id
    cur.execute(
        """INSERT INTO app_user (
            user_id, age, gender, occupation, country,
            preferred_language, signup_date, subscription_plan, account_status,
            email, first_name, last_name, state_province, city,
            monthly_spend_hours, primary_device, seed_source
        ) VALUES (
            %(user_id)s, %(age)s, %(gender)s, %(occupation)s, %(country)s,
            %(preferred_language)s, %(signup_date)s, %(subscription_plan)s, %(account_status)s,
            %(email)s, %(first_name)s, %(last_name)s, %(state_province)s, %(city)s,
            %(monthly_spend_hours)s, %(primary_device)s, %(seed_source)s
        )""",
        fields,
    )
    ensure_countries(cur, [fields["country"]])
    ensure_languages(cur, [fields["preferred_language"]])
    return user_id


def create_random_item(cur, next_item_id=None) -> tuple:
    """Inserts one synthetic item with plausible-but-random field values.
    Returns (item_id, title, new_genre_count). `next_item_id` follows the
    same one-off-vs-batch pattern as create_random_user()'s `next_user_id`
    — see there."""
    item_id = (next_item_id or (lambda: next_id(cur, "item", "item_id", "i")))()
    fields = random_item_fields(cur)
    fields["item_id"] = item_id
    cur.execute(
        """INSERT INTO item (
            item_id, type, title, description, release_year, date_added,
            runtime_minutes, season_count, episode_count, content_rating, country,
            original_language, imdb_score, is_netflix_original, content_warning,
            genre_primary, genre_secondary, catalog_status, seed_source
        ) VALUES (
            %(item_id)s, %(type)s, %(title)s, %(description)s, %(release_year)s, %(date_added)s,
            %(runtime_minutes)s, %(season_count)s, %(episode_count)s, %(content_rating)s, %(country)s,
            %(original_language)s, %(imdb_score)s, %(is_netflix_original)s, %(content_warning)s,
            %(genre_primary)s, %(genre_secondary)s, %(catalog_status)s, %(seed_source)s
        )""",
        fields,
    )
    new_genre_count = ensure_genres(cur, [fields["genre_primary"], fields["genre_secondary"]])
    ensure_countries(cur, [fields["country"]])
    ensure_languages(cur, [fields["original_language"]])
    return item_id, fields["title"], new_genre_count


def seed_random_users(cur, count: int = None) -> int:
    """Generates `count` synthetic users (see random_user_fields()). If
    count isn't given, picks a random batch size from
    RANDOM_USER_COUNT_RANGE. Logged to seed_log (key "random_users") so its
    history shows on the Seed Data page, but never gated by
    AlreadySeededError — see module note above create_random_user()."""
    if count is None:
        count = random.randint(*RANDOM_USER_COUNT_RANGE)
    next_user_id = make_id_counter(cur, "app_user", "user_id", "u")
    for _ in range(count):
        create_random_user(cur, next_user_id)
    record_seed(cur, "random_users", 0, count)
    return count


def seed_random_items(cur, count: int = None) -> tuple:
    """Generates `count` synthetic items — see random_item_fields(). If
    count isn't given, picks a random batch size from
    RANDOM_ITEM_COUNT_RANGE. Logged to seed_log (key "random_items"), same
    repeatable/non-gated model as seed_random_users(). Returns
    (count, new_genre_count)."""
    if count is None:
        count = random.randint(*RANDOM_ITEM_COUNT_RANGE)
    next_item_id = make_id_counter(cur, "item", "item_id", "i")
    genre_count = 0
    for _ in range(count):
        _, _, new_genres = create_random_item(cur, next_item_id)
        genre_count += new_genres
    record_seed(cur, "random_items", count, 0)
    return count, genre_count


def available_seeds():
    """Named seed sets under seeds/, e.g. ["large", "netflix_full"] — any of
    3 shapes: titles.csv (Soeiro, see seed_items()), movies.csv (see
    seed_items_from_movies_csv()), or a users-only pack with no items file
    at all (users.dat/users.csv only — e.g. seeds/young_adults/, filtered
    purely on user attributes). See categorize_seeds() for grouping these
    by items/users/both, used by the Seed Data page's sections."""
    if not os.path.isdir(SEEDS_DIR):
        return []
    return sorted(
        d for d in os.listdir(SEEDS_DIR)
        if os.path.isfile(os.path.join(SEEDS_DIR, d, "titles.csv"))
        or os.path.isfile(os.path.join(SEEDS_DIR, d, "movies.csv"))
        or os.path.isfile(os.path.join(SEEDS_DIR, d, "users.dat"))
        or os.path.isfile(os.path.join(SEEDS_DIR, d, "users.csv"))
    )


def categorize_seed(name: str) -> str:
    """"general" (defines both items and users), "items" (items only), or
    "users" (users only) for a named seed set — same file-presence checks
    seed_from_named_set() uses to pick a loader. Drives the Seed Data
    page's General/Items/Users sections (see categorized_available_seeds())."""
    seed_dir = os.path.join(SEEDS_DIR, name)
    has_items = (
        os.path.isfile(os.path.join(seed_dir, "titles.csv"))
        or os.path.isfile(os.path.join(seed_dir, "movies.csv"))
    )
    has_users = (
        os.path.isfile(os.path.join(seed_dir, "users.dat"))
        or os.path.isfile(os.path.join(seed_dir, "users.csv"))
    )
    if has_items and has_users:
        return "general"
    return "items" if has_items else "users"


def categorized_available_seeds() -> dict:
    """available_seeds() grouped by categorize_seed():
    {"general": [...], "items": [...], "users": [...]}."""
    groups = {"general": [], "items": [], "users": []}
    for name in available_seeds():
        groups[categorize_seed(name)].append(name)
    return groups


def parse_list_field(raw: str):
    """soeiro's genres/production_countries columns look like \"['drama', 'crime']\"."""
    if not raw:
        return []
    try:
        parsed = ast.literal_eval(raw)
        if isinstance(parsed, (list, tuple)):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except (ValueError, SyntaxError):
        pass
    return [p.strip(" '\"[]") for p in raw.split(",") if p.strip(" '\"[]")]


def load_soeiro(path: str):
    """Raises ValueError early (naming the row) if a row has the wrong field
    count — almost always an unquoted comma inside a text field (e.g.
    `description`) shifting every later column over by one. Caught this the
    hard way once already: a shifted `genres` value landed in `country` and
    Postgres rejected it with an opaque "value too long for character
    varying(2)" pointing nowhere near the real problem. Fail at the source
    instead."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for i, row in enumerate(rows, start=2):  # +1 for 1-indexing, +1 for the header row
        if None in row:
            raise ValueError(
                f"{path} line {i}: too many fields for title {row.get('title')!r} — "
                f"likely an unquoted comma in a text field (wrap it in \"...\")"
            )
        if any(v is None for v in row.values()):
            raise ValueError(f"{path} line {i}: too few fields for title {row.get('title')!r}")
    return rows


def load_shivamb_lookup(path: str):
    """Keyed by (normalized title, release_year) -> operational metadata."""
    lookup = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["title"].strip().lower(), row.get("release_year", "").strip())
            date_added = None
            raw_date = (row.get("date_added") or "").strip()
            if raw_date:
                try:
                    date_added = datetime.datetime.strptime(raw_date, "%B %d, %Y").date()
                except ValueError:
                    date_added = None
            lookup[key] = {
                "date_added": date_added,
                "content_rating": (row.get("rating") or "").strip() or None,
            }
    return lookup


def preview_seed_files(soeiro_path: str, movielens_path: str):
    """Counts what a seed source would add — pure file reads, no DB access —
    for showing "contains N items, M genres, K users" in the UI/CLI before
    committing to load it. Returns None if the primary file isn't present."""
    if not os.path.isfile(soeiro_path):
        return None
    rows = load_soeiro(soeiro_path)
    item_count = sum(1 for r in rows if r.get("id", "").strip())
    genre_names = set()
    for r in rows:
        genre_names.update(g.strip().lower() for g in parse_list_field(r.get("genres", "")))
    user_count = 0
    if os.path.isfile(movielens_path):
        with open(movielens_path, encoding="utf-8") as f:
            user_count = sum(1 for line in f if len(line.strip().split("::")) == 5)
    # NOT "items"/"genres"/"users" as keys: Jinja resolves `dict.items` to the
    # actual dict.items() *method* (attribute lookup wins over __getitem__),
    # which silently renders as "<built-in method items of dict ...>" instead
    # of erroring — easy to miss. _count suffix sidesteps the whole class of
    # dict-builtin-name collisions (items/keys/values/get/...).
    return {"item_count": item_count, "genre_count": len(genre_names), "user_count": user_count}


def preview_named_seed(name: str):
    seed_dir = os.path.join(SEEDS_DIR, name)
    if os.path.isfile(os.path.join(seed_dir, "movies.csv")) or os.path.isfile(os.path.join(seed_dir, "users.csv")):
        return preview_movies_csv_pack(seed_dir)
    return preview_seed_files(os.path.join(seed_dir, "titles.csv"), os.path.join(seed_dir, "users.dat"))


def preview_real_data():
    return preview_seed_files(SOEIRO_PATH, MOVIELENS_PATH)


def to_int(value):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except (ValueError, OverflowError):
        # float(value) accepts "inf"/"-inf" and parses them fine, but the
        # subsequent int() on an infinite float raises OverflowError, not
        # ValueError - a bare `except ValueError` misses it and crashes the
        # seeding run on a malformed numeric CSV cell instead of skipping it.
        return None


def to_float(value):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def existing_item_keys(cur) -> set:
    """(lower(title), release_year) for every item already in the catalog —
    the natural key seed_items() dedupes against, since seed packs can (and
    are expected to) overlap in titles: reseeding one pack after another
    that already loaded the same title should just skip it, not add a
    second row for the same movie. One query up front rather than one
    per row."""
    cur.execute("SELECT title, release_year FROM item")
    keys = set()
    for row in cur.fetchall():
        title = row["title"] if isinstance(row, dict) else row[0]
        release_year = row["release_year"] if isinstance(row, dict) else row[1]
        keys.add((title.strip().lower(), release_year))
    return keys


def seed_items(cur, soeiro_rows, shivamb_lookup, source_key: str) -> tuple:
    """genre_primary/genre_secondary take the first two entries of the
    source's `genres` list column (flat, capped at 2 — see 01_schema.sql);
    any further genres in that list are dropped rather than modeled.

    Deduplicates by (title, release_year) against what's already in the
    catalog (see existing_item_keys()) — unlike app_user, item IS
    deduplicated across repeated/overlapping seed loads, so activating a
    pack that shares titles with an already-seeded one only ingests the
    titles not yet present. Returns (item_count, skipped_count).

    `source_key` (the seed pack name, or "real") is stamped onto every
    inserted row's `seed_source` column — lets the Seed Data page's "Unseed"
    button find and remove exactly the items a given source added, without
    touching hand-added rows (seed_source NULL) or other sources' rows."""
    existing = existing_item_keys(cur)
    next_item_id = make_id_counter(cur, "item", "item_id", "i")
    item_count = 0
    skipped_count = 0

    for row in soeiro_rows:
        source_id = row.get("id", "").strip()
        if not source_id:
            continue
        item_type = "series" if row.get("type", "").strip().upper() == "SHOW" else "movie"
        release_year = to_int(row.get("release_year"))
        title = row.get("title", "").strip()

        key = (title.lower(), release_year)
        if key in existing:
            skipped_count += 1
            continue
        existing.add(key)  # also guards against duplicate titles within the same source file
        item_id = next_item_id()

        enrichment = shivamb_lookup.get((title.lower(), str(release_year) if release_year else ""), {})
        countries = parse_list_field(row.get("production_countries", ""))
        country = countries[0] if countries else None
        original_language = COUNTRY_LANGUAGE.get(country) if country else None
        genres = parse_list_field(row.get("genres", ""))

        cur.execute(
            """
            INSERT INTO item (
                item_id, type, title, description, release_year, date_added,
                runtime_minutes, season_count, content_rating, country, original_language,
                imdb_score, genre_primary, genre_secondary, catalog_status, seed_source
            ) VALUES (
                %(item_id)s, %(type)s, %(title)s, %(description)s, %(release_year)s, %(date_added)s,
                %(runtime_minutes)s, %(season_count)s, %(content_rating)s, %(country)s, %(original_language)s,
                %(imdb_score)s, %(genre_primary)s, %(genre_secondary)s, 'active', %(seed_source)s
            )
            """,
            {
                "item_id": item_id,
                "seed_source": source_key,
                "type": item_type,
                "title": title,
                "description": row.get("description") or None,
                "release_year": release_year,
                "date_added": enrichment.get("date_added"),
                "runtime_minutes": to_int(row.get("runtime")) if item_type == "movie" else None,
                "season_count": to_int(row.get("seasons")) if item_type == "series" else None,
                "content_rating": enrichment.get("content_rating") or (row.get("age_certification") or None),
                "country": country,
                "original_language": original_language,
                "imdb_score": to_float(row.get("imdb_score")),
                "genre_primary": genres[0] if len(genres) > 0 else None,
                "genre_secondary": genres[1] if len(genres) > 1 else None,
            },
        )
        item_count += 1

    return item_count, skipped_count


def seed_users(cur, movielens_path: str, source_key: str):
    count = 0
    next_user_id = make_id_counter(cur, "app_user", "user_id", "u")
    with open(movielens_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("::")
            if len(parts) != 5:
                continue
            user_ref, gender, age, occupation_code, _zip_code = parts
            user_id = next_user_id()
            offset = int(user_ref) if user_ref.isdigit() else 0

            cur.execute(
                """
                INSERT INTO app_user (
                    user_id, age, gender, occupation, country,
                    preferred_language, signup_date, subscription_plan, account_status,
                    seed_source
                ) VALUES (
                    %(user_id)s, %(age)s, %(gender)s, %(occupation)s, 'US',
                    'en', %(signup_date)s, %(subscription_plan)s, 'active',
                    %(seed_source)s
                )
                """,
                {
                    "user_id": user_id,
                    # MovieLens 1M encodes age as a bucket code (1,18,25,35,45,50,56), not exact age.
                    "age": to_int(age),
                    "gender": {"M": "male", "F": "female"}.get(gender, "unknown"),
                    "occupation": OCCUPATION_MAP.get(occupation_code, "other"),
                    # zip_code (the 5th MovieLens field) isn't modeled — app_user has no
                    # zip_code column, unlike the earlier prefixed-id schema version.
                    "signup_date": datetime.date(2023, 1, 1) + datetime.timedelta(days=offset % 700),
                    "subscription_plan": ["basic", "standard", "premium"][offset % 3],
                    "seed_source": source_key,
                },
            )
            count += 1
    return count


def load_csv_rows(path: str):
    """Plain CSV -> list of dict rows. Unlike load_soeiro(), no strict field-
    count validation — used for data/movies.csv, data/users.csv, and packs
    derived from them, which are either the source files themselves or
    machine-generated subsets of them (not hand-edited, so the
    unquoted-comma failure mode load_soeiro() guards against doesn't apply)."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_bool(value) -> bool:
    return (value or "").strip().lower() == "true"


def seed_items_from_movies_csv(cur, rows, source_key: str) -> tuple:
    """Loads items from the movies.csv-shaped dataset (data/movies.csv — see
    catalog-input/README.md's "Netflix pack" section) or a filtered pack
    derived from it under seeds/<name>/movies.csv. Unlike titles.csv (Soeiro
    shape, see seed_items()), this format already matches the item schema
    almost 1:1 — genre_primary/genre_secondary, is_netflix_original, and
    content_warning are native columns here, not derived or defaulted.
    `type` still isn't a source column: movies.csv's `content_type` mixes in
    values like "Stand-up Comedy"/"Documentary" that aren't movie/series, so
    type is inferred from whether number_of_seasons/number_of_episodes are
    populated, same signal seed_items() uses for the Soeiro shape. Same
    (title, release_year) dedup as seed_items() — see existing_item_keys().
    production_budget/box_office_revenue aren't modeled, same treatment as
    `cast`/`director` in the Soeiro path. Returns (item_count,
    skipped_count)."""
    existing = existing_item_keys(cur)
    next_item_id = make_id_counter(cur, "item", "item_id", "i")
    item_count = 0
    skipped_count = 0

    for row in rows:
        title = (row.get("title") or "").strip()
        if not title:
            continue
        release_year = to_int(row.get("release_year"))

        key = (title.lower(), release_year)
        if key in existing:
            skipped_count += 1
            continue
        existing.add(key)
        item_id = next_item_id()

        is_series = bool((row.get("number_of_seasons") or "").strip() or (row.get("number_of_episodes") or "").strip())
        item_type = "series" if is_series else "movie"

        date_added = None
        raw_added = (row.get("added_to_platform") or "").strip()
        if raw_added:
            try:
                date_added = datetime.datetime.strptime(raw_added, "%Y-%m-%d").date()
            except ValueError:
                date_added = None

        country = COUNTRY_NAME_TO_ISO2.get((row.get("country_of_origin") or "").strip().lower())
        original_language = LANGUAGE_NAME_TO_CODE.get((row.get("language") or "").strip().lower())

        cur.execute(
            """
            INSERT INTO item (
                item_id, type, title, release_year, date_added,
                runtime_minutes, season_count, episode_count, content_rating,
                country, original_language, imdb_score, is_netflix_original,
                content_warning, genre_primary, genre_secondary, catalog_status,
                seed_source
            ) VALUES (
                %(item_id)s, %(type)s, %(title)s, %(release_year)s, %(date_added)s,
                %(runtime_minutes)s, %(season_count)s, %(episode_count)s, %(content_rating)s,
                %(country)s, %(original_language)s, %(imdb_score)s, %(is_netflix_original)s,
                %(content_warning)s, %(genre_primary)s, %(genre_secondary)s, 'active',
                %(seed_source)s
            )
            """,
            {
                "item_id": item_id,
                "type": item_type,
                "title": title,
                "release_year": release_year,
                "date_added": date_added,
                "runtime_minutes": to_int(row.get("duration_minutes")) if item_type == "movie" else None,
                "season_count": to_int(row.get("number_of_seasons")) if item_type == "series" else None,
                "episode_count": to_int(row.get("number_of_episodes")) if item_type == "series" else None,
                "content_rating": (row.get("rating") or "").strip() or None,
                "country": country,
                "original_language": original_language,
                "imdb_score": to_float(row.get("imdb_rating")),
                "is_netflix_original": to_bool(row.get("is_netflix_original")),
                "content_warning": to_bool(row.get("content_warning")),
                "genre_primary": (row.get("genre_primary") or "").strip() or None,
                "genre_secondary": (row.get("genre_secondary") or "").strip() or None,
                "seed_source": source_key,
            },
        )
        item_count += 1

    return item_count, skipped_count


def seed_users_from_users_csv(cur, rows, source_key: str) -> int:
    """Loads users from the users.csv-shaped dataset (data/users.csv) or a
    filtered pack derived from it under seeds/<name>/users.csv. Unlike the
    MovieLens shape (seed_users()), this format already matches app_user
    almost 1:1 — email/first_name/last_name/state_province/city/
    monthly_spend_hours/primary_device are native columns here, not
    defaulted. household_size isn't modeled (no app_user analogue), same
    "not every source column becomes a column" treatment as movies.csv's
    production_budget/box_office_revenue. age is sanity-clipped to 0-100
    (the raw file has a handful of garbage values, e.g. negative ages)
    rather than trusted as-is."""
    count = 0
    next_user_id = make_id_counter(cur, "app_user", "user_id", "u")
    for row in rows:
        age = to_int(row.get("age"))
        if age is not None and not (0 <= age <= 100):
            age = None
        gender = GENDER_NAME_MAP.get((row.get("gender") or "").strip().lower())
        country = COUNTRY_NAME_TO_ISO2.get((row.get("country") or "").strip().lower())
        plan_raw = (row.get("subscription_plan") or "").strip().lower().replace("+", "_plus")
        subscription_plan = plan_raw if plan_raw in SUBSCRIPTION_PLANS else "basic"
        # is_active is a plain bool; account_status also has "suspended" —
        # no source signal distinguishes "suspended" from "cancelled" here.
        account_status = "active" if to_bool(row.get("is_active")) else "cancelled"

        signup_date = None
        raw_signup = (row.get("subscription_start_date") or "").strip()
        if raw_signup:
            try:
                signup_date = datetime.datetime.strptime(raw_signup, "%Y-%m-%d").date()
            except ValueError:
                signup_date = None

        user_id = next_user_id()
        cur.execute(
            """INSERT INTO app_user (
                user_id, age, gender, country, preferred_language,
                signup_date, subscription_plan, account_status,
                email, first_name, last_name, state_province, city,
                monthly_spend_hours, primary_device, seed_source
            ) VALUES (
                %(user_id)s, %(age)s, %(gender)s, %(country)s, %(preferred_language)s,
                %(signup_date)s, %(subscription_plan)s, %(account_status)s,
                %(email)s, %(first_name)s, %(last_name)s, %(state_province)s, %(city)s,
                %(monthly_spend_hours)s, %(primary_device)s, %(seed_source)s
            )""",
            {
                "user_id": user_id,
                "age": age,
                "gender": gender,
                "country": country,
                "preferred_language": COUNTRY_LANGUAGE.get(country),
                "signup_date": signup_date,
                "subscription_plan": subscription_plan,
                "account_status": account_status,
                "seed_source": source_key,
                "email": (row.get("email") or "").strip() or None,
                "first_name": (row.get("first_name") or "").strip() or None,
                "last_name": (row.get("last_name") or "").strip() or None,
                "state_province": (row.get("state_province") or "").strip() or None,
                "city": (row.get("city") or "").strip() or None,
                "monthly_spend_hours": to_float(row.get("monthly_spend_hours")),
                "primary_device": (row.get("primary_device") or "").strip() or None,
            },
        )
        count += 1
    return count


def preview_movies_csv_pack(seed_dir: str):
    """preview_seed_files() equivalent for a movies.csv-shaped pack (see
    seed_items_from_movies_csv()) — counts without touching the DB. Also
    covers users-only packs (no movies.csv at all, e.g. seeds/young_adults/
    — filtered purely on user attributes), which just report item_count=0."""
    movies_path = os.path.join(seed_dir, "movies.csv")
    users_path = os.path.join(seed_dir, "users.csv")
    if not os.path.isfile(movies_path) and not os.path.isfile(users_path):
        return None
    item_count = 0
    genre_names = set()
    if os.path.isfile(movies_path):
        rows = load_csv_rows(movies_path)
        item_count = sum(1 for r in rows if (r.get("title") or "").strip())
        for r in rows:
            for col in ("genre_primary", "genre_secondary"):
                name = (r.get(col) or "").strip().lower()
                if name:
                    genre_names.add(name)
    user_count = 0
    if os.path.isfile(users_path):
        user_count = sum(1 for r in load_csv_rows(users_path) if (r.get("user_id") or "").strip())
    return {"item_count": item_count, "genre_count": len(genre_names), "user_count": user_count}


def _seed_from_paths(cur, soeiro_path, shivamb_path, movielens_path, source_key):
    if not os.path.isfile(soeiro_path):
        raise FileNotFoundError(f"{soeiro_path} not found")
    soeiro_rows = load_soeiro(soeiro_path)
    shivamb_lookup = load_shivamb_lookup(shivamb_path) if os.path.isfile(shivamb_path) else {}
    item_count, skipped_count = seed_items(cur, soeiro_rows, shivamb_lookup, source_key)
    user_count = seed_users(cur, movielens_path, source_key) if os.path.isfile(movielens_path) else 0
    return item_count, skipped_count, user_count


def seed_from_real_data(cur):
    """Loads from /data (real downloaded Kaggle datasets — see
    catalog-input/README.md). Raises FileNotFoundError with a clear message
    if they're not mounted; deliberately no silent fallback here, unlike the
    old behavior, since seeding is now an explicit admin choice from a menu —
    if "real data" is picked but isn't there, say so rather than
    substituting something else."""
    if not os.path.isfile(SOEIRO_PATH):
        raise FileNotFoundError(
            f"No real data mounted at {SOEIRO_PATH} — download the Kaggle datasets into ./data "
            f"(see catalog-input/README.md) or pick a bundled seed set instead."
        )
    return _seed_from_paths(cur, SOEIRO_PATH, SHIVAMB_PATH, MOVIELENS_PATH, "real")


def seed_from_named_set(cur, name: str, mode: str = "full"):
    """Loads a curated seed set checked into seeds/<name>/ (see
    available_seeds()). mode="full" (default) loads items and, if
    present, users — original behavior. mode="items" loads items
    only, skipping the users file even if the pack has one. mode="users"
    loads only the users file, skipping items entirely — for packs that
    bundle both but an admin wants just one half.

    Two pack shapes, picked by which items file is present (see
    available_seeds()): titles.csv (Soeiro shape, seed_items()) pairs with
    users.dat (MovieLens shape, seed_users()); movies.csv (native shape,
    seed_items_from_movies_csv()) pairs with users.csv (native shape,
    seed_users_from_users_csv()) — the "Netflix pack" family generated from
    data/movies.csv / data/users.csv, see catalog-input/README.md."""
    seed_dir = os.path.join(SEEDS_DIR, name)
    soeiro_path = os.path.join(seed_dir, "titles.csv")
    shivamb_path = os.path.join(seed_dir, "netflix_titles.csv")
    movielens_path = os.path.join(seed_dir, "users.dat")
    movies_path = os.path.join(seed_dir, "movies.csv")
    users_csv_path = os.path.join(seed_dir, "users.csv")

    if mode == "users":
        if os.path.isfile(movielens_path):
            return 0, 0, seed_users(cur, movielens_path, name)
        if os.path.isfile(users_csv_path):
            return 0, 0, seed_users_from_users_csv(cur, load_csv_rows(users_csv_path), name)
        raise FileNotFoundError(f"Seed set '{name}' has no users file to load users-only from")

    if os.path.isfile(movies_path):
        item_count, skipped_count = seed_items_from_movies_csv(cur, load_csv_rows(movies_path), name)
        user_count = 0
        if mode != "items" and os.path.isfile(users_csv_path):
            user_count = seed_users_from_users_csv(cur, load_csv_rows(users_csv_path), name)
        return item_count, skipped_count, user_count

    if os.path.isfile(soeiro_path):
        if mode == "items":
            soeiro_rows = load_soeiro(soeiro_path)
            shivamb_lookup = load_shivamb_lookup(shivamb_path) if os.path.isfile(shivamb_path) else {}
            item_count, skipped_count = seed_items(cur, soeiro_rows, shivamb_lookup, name)
            return item_count, skipped_count, 0
        return _seed_from_paths(cur, soeiro_path, shivamb_path, movielens_path, name)

    # No items file at all — a users-only pack (e.g. seeds/young_adults/,
    # filtered purely on user attributes with no movies.csv/titles.csv).
    if mode != "items":
        if os.path.isfile(movielens_path):
            return 0, 0, seed_users(cur, movielens_path, name)
        if os.path.isfile(users_csv_path):
            return 0, 0, seed_users_from_users_csv(cur, load_csv_rows(users_csv_path), name)
    raise FileNotFoundError(f"Unknown seed set '{name}' (no titles.csv, movies.csv, or a users file)")


class AlreadySeededError(Exception):
    """Raised by seed() when this source was already loaded and force=False."""

    def __init__(self, key: str, count: int, last_loaded_at):
        self.key = key
        self.count = count
        self.last_loaded_at = last_loaded_at
        super().__init__(
            f'"{key}" was already loaded {count} time(s), last at {last_loaded_at}. '
            f"Loading it again will add duplicate rows, not update the existing ones."
        )


def get_seed_history(cur):
    """{seed_key: {"count": n, "last_loaded_at": ts}} from seed_log. Cursor-
    agnostic (see next_id())."""
    cur.execute("SELECT seed_name, count(*) AS n, max(loaded_at) AS last FROM seed_log GROUP BY seed_name")
    history = {}
    for row in cur.fetchall():
        if isinstance(row, dict):
            name, n, last = row["seed_name"], row["n"], row["last"]
        else:
            name, n, last = row
        history[name] = {"count": n, "last_loaded_at": last}
    return history


def record_seed(cur, key: str, item_count: int, user_count: int):
    cur.execute(
        "INSERT INTO seed_log (seed_name, item_count, user_count) VALUES (%s, %s, %s)",
        (key, item_count, user_count),
    )


def seed(cur, name: str = None, real: bool = False, force: bool = False, mode: str = "full"):
    """Single entry point for both the CLI and the frontend. Pass `name` for
    a seeds/<name>/ set, or real=True for /data. `mode` ("full"/"items"/
    "users") only applies to named sets — see seed_from_named_set(); items-
    only and users-only loads of the same pack are tracked as distinct
    history keys ("<name>:items"/"<name>:users") so they're warned about
    independently of the full bundle and each other. Raises
    AlreadySeededError if this exact source was already loaded and force
    isn't set (repeat of the *same* named/real source — see module
    docstring). That guard is orthogonal to item-level dedup: two
    *different* sources sharing titles (packs are allowed to overlap) is
    handled by seed_items()'s (title, release_year) dedup, not this guard —
    activating a second, different pack after the first never raises here,
    it just skips whatever titles the first pack already loaded. Logs to
    seed_log on success. Returns (item_count, skipped_count, user_count,
    new_genre_count)."""
    key = "real" if real else (name if mode == "full" else f"{name}:{mode}")
    history = get_seed_history(cur)
    if key in history and not force:
        h = history[key]
        raise AlreadySeededError(key, h["count"], h["last_loaded_at"])

    if real:
        item_count, skipped_count, user_count = seed_from_real_data(cur)
    else:
        item_count, skipped_count, user_count = seed_from_named_set(cur, name, mode=mode)

    genre_count = sync_genres_from_items(cur)
    sync_countries_languages_from_data(cur)
    record_seed(cur, key, item_count, user_count)
    return item_count, skipped_count, user_count, genre_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--real", action="store_true", help="load from /data (real Kaggle datasets)")
    group.add_argument("--seed", metavar="NAME", help=f"load a bundled seed set: {available_seeds()}")
    group.add_argument("--random-users", nargs="?", const=-1, type=int, metavar="N",
                        help="generate N synthetic random users (omit N for a random batch size), "
                             "not from any file")
    group.add_argument("--random-items", nargs="?", const=-1, type=int, metavar="N",
                        help="generate N synthetic random items (omit N for a random batch size), "
                             "not from any file")
    parser.add_argument("--mode", choices=["full", "items", "users"], default="full",
                         help="for --seed NAME: load items+users (full, default), items only, or users only")
    parser.add_argument("--force", action="store_true",
                         help="load even if this source was already loaded before (adds duplicates)")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "catalog-db"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "catalog"),
        user=os.environ.get("PGUSER", "catalog"),
        password=os.environ.get("PGPASSWORD", "catalog"),
    )
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if args.random_users is not None:
                count = None if args.random_users == -1 else args.random_users
                print("Generating random synthetic users ...")
                user_count = seed_random_users(cur, count=count)
                print(f"  inserted {user_count} users")
                conn.commit()
                print("Seeding complete.")
                return
            if args.random_items is not None:
                count = None if args.random_items == -1 else args.random_items
                print("Generating random synthetic items ...")
                item_count, genre_count = seed_random_items(cur, count=count)
                genre_note = f", {genre_count} new genre(s)" if genre_count else ""
                print(f"  inserted {item_count} items{genre_note}")
                conn.commit()
                print("Seeding complete.")
                return
            try:
                if args.real:
                    print("Loading from real data (/data) ...")
                    item_count, skipped_count, user_count, genre_count = seed(cur, real=True, force=args.force)
                else:
                    seed_name = args.seed or "large"
                    print(f"Loading seed set '{seed_name}' (mode={args.mode}) ...")
                    item_count, skipped_count, user_count, genre_count = seed(
                        cur, name=seed_name, force=args.force, mode=args.mode
                    )
            except AlreadySeededError as e:
                conn.rollback()
                print(f"Refusing to seed: {e}\nRe-run with --force to load anyway.")
                return
            skip_note = f" ({skipped_count} already in catalog, skipped)" if skipped_count else ""
            genre_note = f", {genre_count} new genre(s)" if genre_count else ""
            print(f"  inserted {item_count} items{skip_note}, {user_count} users{genre_note}")
        conn.commit()
        print("Seeding complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
