-- client-input-db's schema. Runs once, on first container start
-- (docker-entrypoint-initdb.d), same role catalog-input/postgres/init
-- plays for catalog-db.
--
-- items/users are intentionally minimal (only the columns generator.py's
-- pool-selection queries actually read), and this is the *only* thing
-- client-input-db is for now: a locally-queryable mirror of the active
-- item/user pool (kept in sync from de.iu.Item.V001/User.V001 by
-- client-input-item-sink-connector/client-input-user-sink-connector), not
-- a full Item/User mirror the way reporting-db.items/users is. Unlike that
-- Postgres mirror, this can't just be "auto.create the PK, auto.evolve
-- fills in the rest": MySQL's ALTER TABLE ADD COLUMN refuses a non-
-- optional field with no default (Item.V001's `type` is exactly that),
-- and separately refuses a default value on any TEXT/BLOB column at all
-- (hit both live, first deploy). The two sink connectors' fields.whitelist
-- keeps them from ever attempting to write - and therefore auto.evolve-
-- ALTER for - anything outside this exact column list.
--
-- ratings/watch_events used to live here too (client-input's own domain
-- data, written directly by generator.py, CDC'd out to Kafka by
-- client-input-source-connector — a Debezium-sourced outbox, the same
-- shape catalog-db already uses for Item/User) — see ../../ARCHITECTURE.md
-- for that design and why it was tried. Reverted: neither table was ever
-- read back by anything in this service, so the durability/atomicity that
-- an outbox buys you was never actually needed here, and it brought a real
-- cost — the whole FK CASCADE / delete-suppression story two levels of
-- documentation used to cover. generator.py now produces
-- de.iu.Rating.V002/de.iu.Watch.Event.V002 directly (see ../README.md's
-- "How events actually reach Kafka now"), so there's no local ratings/
-- watch_events table for a catalog item/user delete to clean up here at
-- all any more — items/users below have no children, nothing references
-- them, plain DELETE (no FK, no CASCADE needed).

CREATE TABLE IF NOT EXISTS items (
    item_id         VARCHAR(64) NOT NULL PRIMARY KEY,
    type            VARCHAR(16) NOT NULL,
    runtime_minutes INT NULL,
    catalog_status  VARCHAR(16) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS users (
    user_id        VARCHAR(64) NOT NULL PRIMARY KEY,
    account_status VARCHAR(16) NULL
) ENGINE=InnoDB;
