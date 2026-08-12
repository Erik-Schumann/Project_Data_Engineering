-- Catalog Database schema — source of CDC for the Catalog Input Service.
-- Requires wal_level=logical (set via the postgres container command in
-- docker-compose.yml).

-- IDs are app-generated (not DB-default): prefixed + a sequential counter
-- per prefix (see seed/seed_catalog.py: make_id_counter()/next_id()) - e.g.
-- i1, i2, u1, u37. The counter lives in id_counter below (never derived by
-- scanning item/app_user for the current max), so a deleted id is never
-- reissued - even a full delete-all-and-reseed keeps counting up from
-- wherever it left off, rather than restarting at 1.
CREATE TABLE id_counter (
    prefix TEXT PRIMARY KEY,
    next_n BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE item (
    item_id           TEXT PRIMARY KEY CHECK (item_id ~ '^i[0-9]+$'),
    type              VARCHAR(10) NOT NULL CHECK (type IN ('movie','series')),
    title             TEXT NOT NULL,
    description       TEXT,
    release_year      SMALLINT,
    date_added        DATE,
    runtime_minutes   INTEGER,          -- movies
    season_count      SMALLINT,         -- series
    episode_count     SMALLINT,         -- series
    content_rating    VARCHAR(10),      -- age_certification / rating (e.g. TV-MA)
    country           VARCHAR(2),        -- primary production country (flat - one column, not a junction table: no report needs multi-country grouping)
    original_language VARCHAR(10),       -- derived from country, see seed_catalog.py COUNTRY_LANGUAGE
    imdb_score        NUMERIC(3,1),
    is_netflix_original BOOLEAN NOT NULL DEFAULT false,
    content_warning   BOOLEAN NOT NULL DEFAULT false,
    genre_primary     VARCHAR(50),       -- flat, not normalized - same reasoning as country above
    genre_secondary   VARCHAR(50),
    catalog_status    VARCHAR(15) NOT NULL DEFAULT 'active'
                        CHECK (catalog_status IN ('active','coming_soon','archived')),
    seed_source       VARCHAR(50),       -- which seed pack/"real"/"random_items" loaded this row;
                                          -- NULL for rows added by hand via Catalog Admin. Powers
                                          -- the Seed Data page's "Unseed" button (see seed_catalog.py)
                                          -- and item-level dedup (existing_item_keys()).
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE app_user (
    user_id             TEXT PRIMARY KEY CHECK (user_id ~ '^u[0-9]+$'),
    age                 SMALLINT,
    gender              VARCHAR(10),
    occupation          VARCHAR(50),
    country             VARCHAR(2),
    preferred_language  VARCHAR(10),
    signup_date         DATE,
    subscription_plan   VARCHAR(15) NOT NULL DEFAULT 'basic'
                          CHECK (subscription_plan IN ('basic','standard','premium','premium_plus')),
    account_status      VARCHAR(10) NOT NULL DEFAULT 'active'
                          CHECK (account_status IN ('active','suspended','cancelled')),
    email               TEXT,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    state_province      VARCHAR(50),
    city                VARCHAR(50),
    monthly_spend_hours NUMERIC(6,2),
    primary_device      VARCHAR(20),
    seed_source         VARCHAR(50),       -- which seed pack/"random_users" loaded this row;
                                            -- NULL for rows added by hand. Same purpose as
                                            -- item.seed_source — see its comment above.
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Managed genre vocabulary powering the Catalog Admin item form's genre
-- dropdowns. item.genre_primary/genre_secondary above stay flat text
-- columns, unchanged. Deliberately excluded from CDC, same reasoning as
-- seed_log below: an admin-UI concern, not part of the domain model
-- Kafka downstream consumers see.
CREATE TABLE genre (
    name       VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Managed country/language vocabularies, same role and reasoning as genre
-- above: power the Catalog Admin item/user form dropdowns
-- (country/original_language/preferred_language stay flat text columns),
-- deliberately excluded from CDC. Independent lists, not a paired
-- country->language mapping — item.country and item.original_language
-- (and the app_user equivalents) are already separate, unenforced columns,
-- so there's nothing to keep in sync between the two tables. Supersedes
-- seed_catalog.py's hardcoded COUNTRY_LANGUAGE dict as the live source of
-- truth once seeded; that dict remains as the fallback when both tables
-- are empty (a fresh, unseeded database).
CREATE TABLE country (
    code       VARCHAR(2) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE language (
    code       VARCHAR(10) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium (pgoutput plugin) needs a REPLICA IDENTITY that includes enough
-- info to build a full before/after image; FULL is the simplest correct
-- choice for a portfolio-scale table (no huge TOAST columns here).
ALTER TABLE item REPLICA IDENTITY FULL;
ALTER TABLE app_user REPLICA IDENTITY FULL;

-- Operational bookkeeping for the Catalog Admin frontend's Seed Data page —
-- NOT part of the domain model, deliberately excluded from CDC (the
-- connector's table.include.list in catalog-input/connect/catalog-source-connector.json
-- names the 2 tables above explicitly, not a wildcard, so this table is
-- invisible to Kafka). Tracks what's been seeded so the UI/CLI can warn
-- before creating duplicates (seeding is not idempotent — see seed_catalog.py).
CREATE TABLE seed_log (
    id          SERIAL PRIMARY KEY,
    seed_name   TEXT NOT NULL,
    item_count  INTEGER NOT NULL,
    user_count  INTEGER NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
