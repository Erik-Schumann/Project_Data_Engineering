-- reporting-db schema — the raw mirror tables the 4 JDBC sink connectors
-- write into (see ../connect/*.json), hand-written here instead of left to
-- auto.create/auto.evolve. That's a deliberate change (2026-08-12): letting
-- Connect create tables/foreign keys implicitly meant "does reporting-db
-- have the tables it needs" depended on connect-register having already
-- run *and* real traffic having already flowed through at least once
-- (auto.create only fires on a connector's first write) - exactly the
-- startup-ordering fragility that made connect-register/reporting-db-migrate
-- failures hard to diagnose. This file runs once via Postgres's own
-- docker-entrypoint-initdb.d, same mechanism catalog-input/postgres/init
-- already uses, so the tables (and their foreign keys) exist before any
-- connector ever needs to write to them - no race, no waiting.
--
-- items/users mirror catalog-db.item/app_user exactly (see
-- catalog-input/postgres/init/01_schema.sql) - same columns/types, since
-- these are just CDC copies. ratings/watch_events mirror the hand-authored
-- event schemas in client-input/schemas/*.avsc.
--
-- The 4 connectors (../connect/*.json) now run with auto.create=false,
-- auto.evolve=false: a future Avro schema change needs a matching edit
-- here, same "reset the volume on schema changes" expectation this
-- project already has for client-input-db/catalog-db (see memory / other
-- services' own init scripts) - docker-entrypoint-initdb.d only ever runs
-- once, on a fresh, empty data directory.

CREATE TABLE items (
    item_id             TEXT PRIMARY KEY,
    type                TEXT NOT NULL,
    title               TEXT NOT NULL,
    description         TEXT,
    release_year        SMALLINT,
    date_added          DATE,
    runtime_minutes     INTEGER,
    season_count        SMALLINT,
    episode_count       SMALLINT,
    content_rating      TEXT,
    country             TEXT,
    original_language   TEXT,
    imdb_score          NUMERIC(3,1),
    is_netflix_original BOOLEAN NOT NULL DEFAULT false,
    content_warning     BOOLEAN NOT NULL DEFAULT false,
    genre_primary       TEXT,
    genre_secondary     TEXT,
    catalog_status      TEXT NOT NULL DEFAULT 'active',
    seed_source         TEXT,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ
);

CREATE TABLE users (
    user_id             TEXT PRIMARY KEY,
    age                 SMALLINT,
    gender              TEXT,
    occupation          TEXT,
    country             TEXT,
    preferred_language  TEXT,
    signup_date         DATE,
    subscription_plan   TEXT NOT NULL DEFAULT 'basic',
    account_status      TEXT NOT NULL DEFAULT 'active',
    email               TEXT,
    first_name          TEXT,
    last_name           TEXT,
    state_province      TEXT,
    city                TEXT,
    monthly_spend_hours NUMERIC(6,2),
    primary_device      TEXT,
    seed_source         TEXT,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ
);

-- One row per (user_id, item_id) - a re-rating upserts, enforced by
-- reporting-rating-sink-connector's pk.mode=record_key/insert.mode=upsert,
-- not by anything in this table beyond the PK itself.
CREATE TABLE ratings (
    user_id  TEXT NOT NULL,
    item_id  TEXT NOT NULL,
    rating   SMALLINT NOT NULL,
    rated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, item_id)
);

-- Append-only - a rewatch is a second genuine row, so no PK on
-- (user_id, item_id). Surrogate id is never referenced by the connector
-- (pk.mode=none - see reporting-watch-sink-connector.json), Postgres
-- assigns it on insert.
CREATE TABLE watch_events (
    id               BIGSERIAL PRIMARY KEY,
    user_id          TEXT NOT NULL,
    item_id          TEXT NOT NULL,
    watched_seconds  INTEGER NOT NULL,
    device_type      TEXT NOT NULL,
    session_ended_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_watch_events_user_id ON watch_events (user_id);
CREATE INDEX idx_watch_events_item_id ON watch_events (item_id);
CREATE INDEX idx_watch_events_session_ended_at ON watch_events (session_ended_at);

-- FK ON DELETE CASCADE, baked in from the start - what
-- reporting-db-migrate/add-fk-constraints.sh used to add after the fact,
-- once auto.create had already run (that service is gone now, this
-- replaces it entirely). A rating/watch event referencing a since-deleted
-- item/user is cleaned up the instant reporting-item-sink-connector/
-- reporting-user-sink-connector delete that row - see
-- reporting-output/README.md's "Retention" section.
ALTER TABLE ratings
    ADD CONSTRAINT fk_ratings_item FOREIGN KEY (item_id) REFERENCES items (item_id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_ratings_user FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE;

ALTER TABLE watch_events
    ADD CONSTRAINT fk_watch_item FOREIGN KEY (item_id) REFERENCES items (item_id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_watch_user FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE;
