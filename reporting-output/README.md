# Reporting Output Service

Postgres sink + Grafana dashboards over the catalog's `Item`/`User`
topics (and `Rating`/`Watch.Summary`), plus ten jobs computing metrics
from them — all ten plain scheduled SQL against `reporting-db`, since
mirroring every source topic into a real indexed Postgres table is
enough for a `GROUP BY` or a window-function ranking to handle every one
of them (see "The SQL jobs" below). The item/user/rating/watch-event
mirror is still a raw CDC-style copy (JDBC sink connectors, no
aggregation); the SQL jobs are the computed layer here.

**`reporting-watch-sink-connector` reads `de.iu.Watch.Summary.V002`, not
`de.iu.Watch.Event.V002` directly** — see `../watch-summary/README.md`.
`Watch.Event.V002` carries a heartbeat every tick per in-progress item,
not one message per watch — `watch-summary-service` is the only consumer of that raw
stream; it session-windows on inactivity and republishes exactly one
settled record per watch to `Watch.Summary.V002`, same key/value schema.
This connector, and everything downstream of `reporting-db.watch_events`
(the SQL jobs that read it, all four Grafana dashboards), never sees the
heartbeat noise — only a clean fact per watch, `SESSION_GAP_SECONDS`
(10s) after it actually happened.

See `../ARCHITECTURE.md` for how this fits into the whole system (topic
catalog, full data-flow diagram) and `../ENTERPRISE_ARCHITECTURE.md` for
what running this at production scale would actually require.

## What it does

Four JDBC sink connectors (running on the shared `connect` Kafka Connect
worker — see `../connect/`) consume `de.iu.Item.V001`, `de.iu.User.V001`,
`de.iu.Rating.V002`, and `de.iu.Watch.Summary.V002`, and mirror them into
`reporting-db`, a dedicated Postgres instance:

- `reporting-item-sink-connector` → `items` table, keyed by `item_id`
- `reporting-user-sink-connector` → `users` table, keyed by `user_id`
- `reporting-rating-sink-connector` → `ratings` table, keyed by
  `(user_id, item_id)`
- `reporting-watch-sink-connector` → `watch_events` table, insert-only

All four run with `auto.create=false`/`auto.evolve=false` —
`reporting-output/postgres/init/01_schema.sql` hand-writes `items`/
`users`/`ratings`/`watch_events` (including the FK `ON DELETE CASCADE`
constraints, see "Retention" below) instead, mounted into `reporting-db`'s
own `docker-entrypoint-initdb.d` the same way `catalog-input/postgres/init`
already does for `catalog-db`. This means "does reporting-db have what it
needs" is guaranteed from the moment `reporting-db` itself is healthy, not
dependent on `connect-register` having already run and real traffic
having already flowed through at least once.

`reporting-item-sink-connector`/`reporting-user-sink-connector` consume
`de.iu.Item.V001`/`de.iu.User.V001`, which are Debezium CDC output
(`catalog-input/catalog-source-connector.json`), not hand-produced Avro.
Two type mismatches need explicit handling between what Debezium emits
and what `01_schema.sql` declares:

- `date_added`/`signup_date`: fixed at the source, not here —
  `catalog-source-connector.json` now sets `"time.precision.mode": "connect"`
  so Debezium emits the standard Kafka Connect `Date` logical type instead
  of its own `io.debezium.time.Date`, which the JDBC sink didn't
  recognize and would otherwise bind as a raw epoch-day integer against a
  real `DATE` column. See `catalog-input/README.md`'s "Registered Avro
  schemas" section for the full explanation.
- `created_at`/`updated_at`: Debezium always emits its `ZonedTimestamp`
  logical type as an ISO-8601 string, regardless of `time.precision.mode`
  — there's no source-side fix available. Both connectors' `connection.url`
  instead carries `?stringtype=unspecified`, which makes the Postgres JDBC
  driver bind string parameters as `unknown` rather than explicit
  `varchar`, letting Postgres apply its normal implicit-cast-to-`timestamptz`
  the same way it would for an untyped string literal in plain SQL.

All four connectors also use `pk.mode=record_key`-or-`none` +
`insert.mode=upsert`-or-`insert`, but the delete story splits three ways
rather than being uniform:

- `reporting-item-sink-connector`/`reporting-user-sink-connector`:
  `pk.mode=record_key` + `insert.mode=upsert` + `delete.enabled=true`.
  `Item.V001`/`User.V001` are genuinely compacted changelogs and their
  deletes are real tombstones (`catalog-db` → Postgres logical replication
  → `catalog-source-connector`), so a tombstone here correctly deletes the
  row — `reporting-db.items`/`users` stay a true mirror, not an
  ever-growing log.
- `reporting-rating-sink-connector`: `pk.mode=record_key` +
  `insert.mode=upsert`, no `delete.enabled`. `Rating.V002` is a plain
  event topic (see `client-input/README.md`) that never carries a delete —
  `generator.py` produces it directly, no database or CDC hop upstream for
  a delete to originate from in the first place. The
  upsert-on-`(user_id,item_id)` still matters (a re-rating still
  overwrites, not accumulates); there's just nothing for `delete.enabled`
  to ever act on.
- `reporting-watch-sink-connector`: `pk.mode=none` (`watch_events.id` is a
  plain `BIGSERIAL` the connector never references — Postgres assigns it
  on insert; `Watch.Summary.V002` is append-only, rewatches legitimately
  repeat `{user_id, item_id}`, so there's nothing to upsert against
  anyway) + `insert.mode=insert` — every settled watch becomes a new row.
  Its `Filter`+`RecordIsTombstone` transform (dropping any tombstone
  rather than letting it land here as a null row) is defensive dead code
  today for the same reason: nothing produces a `Watch.Summary.V002`
  tombstone either — `watch-summary-service` only ever produces, same as
  `generator.py` upstream of it.

None of that leaves `reporting-db.ratings`/`watch_events` on their own when
an item/user is deleted, though — see "Retention" below for how that's
handled now, independently of Kafka.

`ratings`/`watch_events` existing as real, queryable, indexed Postgres
tables (not Kafka topics that have to be replayed and deduped/windowed by
hand on every read) is what lets all ten jobs below be plain SQL — see
"The SQL jobs" below.

## The SQL jobs

All ten jobs are a fresh `psycopg2` connection every
`TRIGGER_INTERVAL_SECONDS` (default 60) — no Kafka client, no Avro. Each
does a full recompute against `reporting-db` (a `GROUP BY` or a
window-function ranking) and a full-sync upsert (delete anything missing
from this tick's result, not just append/update). `ratings`/`watch_events`
carry a `FOREIGN KEY ... ON DELETE CASCADE` back to `items`/`users`, so a
deleted item/user's rows are already gone by the time any job queries
them — no job needs to filter for that. Six of the ten also join
`items`/`users` anyway, to pull a column their aggregation needs
(`type`/`genre_primary`/`original_language`/`age`/`gender`) — see
`../ARCHITECTURE.md`'s "Reporting pipeline detail" for which.

| Job | Computes | README |
|---|---|---|
| `user-rating-avg` | All-time average rating per user | [`sql/user-rating-avg/README.md`](sql/user-rating-avg/README.md) |
| `item-rating-avg` | All-time average rating per item | [`sql/item-rating-avg/README.md`](sql/item-rating-avg/README.md) |
| `user-mood` | good/average/bad per user, from their last ≤3 ratings | [`sql/user-mood/README.md`](sql/user-mood/README.md) |
| `trending-rankings` | Rolling top-10 series/movies, by views and by rating, last `TRENDING_WINDOW_MINUTES` | [`sql/trending-rankings/README.md`](sql/trending-rankings/README.md) |
| `user-series-movie-ratio` | All-time movie/series watch percentage per user | [`sql/user-series-movie-ratio/README.md`](sql/user-series-movie-ratio/README.md) |
| `item-completion-rate` | All-time completion rate per item (movie or series) | [`sql/item-completion-rate/README.md`](sql/item-completion-rate/README.md) |
| `user-top-genre` | Most-watched primary genre per user, over their last `USER_TOP_GENRE_RECENT_N` watches | [`sql/user-top-genre/README.md`](sql/user-top-genre/README.md) |
| `user-watch-count` | Distinct items watched + total watches (rewatches included) per user | [`sql/user-watch-count/README.md`](sql/user-watch-count/README.md) |
| `user-top-device-language` | Most-used device + most-watched original language per user, all-time | [`sql/user-top-device-language/README.md`](sql/user-top-device-language/README.md) |
| `item-viewer-demographics` | Average viewer age + male/female/other/unknown sex distribution per item | [`sql/item-viewer-demographics/README.md`](sql/item-viewer-demographics/README.md) |

`user-rating-avg`/`item-rating-avg`/`user-mood` only ever need `ratings`
(compacted — one row per `(user_id, item_id)`, latest wins), so
`reporting-rating-sink-connector` alone is enough for them.
`trending-rankings`, `user-series-movie-ratio`, `item-completion-rate`,
`user-top-genre`, `user-top-device-language`, and
`item-viewer-demographics` need `watch_events` too (a rolling time
window or an all-time count/mode over `Watch.Summary.V002`'s append-only
history, not a snapshot of current state) — see each one's own README
for the exact query, and, for `trending-rankings`/`user-top-genre`/
`user-top-device-language` specifically, how Postgres's native
window-function support (`ROW_NUMBER() OVER (PARTITION BY ...)`)
computes the ranking directly. `item-viewer-demographics` is the one
job in this directory that deliberately dedupes to distinct viewers
rather than counting every watch event — see its own README for why a
demographic breakdown needs that.

## Grafana dashboards

Four dashboards:

- **Summary** — pure aggregates, no filters: total items/users, overall
  average rating, items by type, users by subscription plan.
- **Items** — `item_id`/`item_type` filters, item count, Item Detail table
  (`LEFT JOIN`ed against `item_rating_avg`, `item_completion_rate`, and
  `item_viewer_demographics` - `avg_rating`/`rating_count`/
  `completion_rate`/`watch_count`/`average_age`/`pct_male`/`pct_female`/
  `pct_other`/`pct_unknown`/`viewer_count` are `NULL` for an item missing
  the underlying activity), Avg Rating, and Avg Completion Rate. No
  separate per-item rating/completion-rate/demographics tables - they'd
  duplicate Item Detail with no extra filtering that view doesn't already
  offer (same reasoning the Users dashboard's per-user tables followed).
- **Users** — `user_id`/`user_plan` filters, user count, users by country,
  **Average Rating per User** (from `user_rating_avg`), **Users by Mood**
  (from `user_mood`), **Users by Top Genre** (from `user_top_genre`), and
  User Detail (`LEFT JOIN`ed against `user_rating_avg`, `user_mood`,
  `user_series_movie_ratio`, `user_top_genre`, and
  `user_top_device_language` — the added columns are `NULL` for a user
  missing the underlying activity, a real state, not an
  error). Series/movie percentage and top-genre detail live only on User
  Detail now, not as separate per-user tables — they duplicated it with no
  extra filtering the combined view didn't already offer.
- **Trending** — 4 tables from `trending-rankings`: top 10 series/movies
  by views, top 10 series/movies by rating, each over the rolling last
  10 minutes, each joined to `items` for title.

Each `item_id`/`user_id` drill-down and `item_type`/`user_plan` attribute
filter defaults to "All" via a value baked directly into the dropdown's
own query (`SELECT 'All' AS __text, '' AS __value UNION ALL ...`) rather
than Grafana's built-in "Include All" option — that option's `sqlstring`
formatting expands "All" to the full comma-joined list of every option
instead of respecting a custom empty-string override, which produced
malformed SQL Postgres parsed as an accidental row constructor. Panel
`WHERE` clauses follow the same `(${var:sqlstring} = '' OR column =
${var:sqlstring})` pattern throughout, safe now that each variable is
always a genuine single scalar. The "by type"/"by subscription plan"
breakdown panels deliberately ignore their own matching variable —
filtering a panel by the exact thing it breaks down would just collapse
it to one bar.

## Bring it up

```bash
docker compose up -d reporting-db grafana connect connect-register \
  user-rating-service item-rating-service trending-rankings-service user-mood-service \
  user-series-movie-ratio-service item-completion-rate-service user-top-genre-service \
  user-watch-count-service user-top-device-language-service item-viewer-demographics-service
```

(`connect`/`connect-register` are shared with `catalog-input` — this just
adds four more connectors to the same worker. `kafka`, `schema-registry`,
`catalog-db` must already be up, and `watch-summary-service` (see
`../watch-summary/README.md`) needs to be running and producing to
`de.iu.Watch.Summary.V002` for `reporting-watch-sink-connector` to have
anything to mirror — it reads that topic now, not `Watch.Event.V002`
directly. None of the ten SQL jobs need `kafka`/`schema-registry` at
all — they only ever talk to `reporting-db`. Bring everything up
before/alongside Grafana — the Users/Items/Trending dashboards query
tables each job creates on first connect. `reporting-db`'s tables
(`items`/`users`/`ratings`/`watch_events`, with their FK constraints
already in place) exist from the moment `reporting-db` itself is
healthy — see "Retention" below — so there's no separate migration step
to wait on.)

Open http://localhost:3001 and sign in with your `GRAFANA_ADMIN_*`
credentials from `.env`. The **Reporting** folder has all four dashboards
(Summary, Items, Users, Trending). None of the ten jobs has an
Application UI — `docker compose logs -f <service-name>` is the
equivalent; each runs silently on a successful tick (no output).

## Retention

`client-input-db` holds no `ratings`/`watch_events` tables —
`generator.py` produces straight to Kafka, no MySQL table in front of it
(see `client-input/README.md`'s "How events reach Kafka").
The mechanism that cleans up a rating/watch event referencing a
since-deleted item/user lives entirely here, in `reporting-db`:

`reporting-output/postgres/init/01_schema.sql` (mounted into
`reporting-db`'s own `docker-entrypoint-initdb.d`, runs once on a fresh
volume — see `../docker-compose.yml`) declares `FOREIGN KEY ...
ON DELETE CASCADE` from `ratings`/`watch_events` back to `items`/`users`
as part of the initial `CREATE TABLE`s, not as a follow-up migration. A
rating/watch event that arrived for an item/user later deleted is cleaned
up the instant `reporting-item-sink-connector`/`reporting-user-sink-connector`
delete that row here — no Kafka involvement, no dependency on anything
upstream having cleaned up first (`de.iu.Rating.V002`/
`de.iu.Watch.Summary.V002` never carry a delete at all, see "What it
does" above, so there's nothing to receive even if this connector wanted
to react to it). Nothing reads `reporting-db`'s own WAL, so there's no
MySQL-style binlog-invisibility problem to route around here.

The FK constraints exist from `reporting-db`'s first healthy start —
no separate migration step to wait on, since `01_schema.sql` declares
them as part of the initial `CREATE TABLE`s rather than relying on
`auto.create`/`auto.evolve` to build the tables first (see "What it
does" above).

## Keeping it in sync with catalog-db

`reporting-db` (and the compacted topics themselves) can drift from
catalog-db's actual current rows: Postgres logical replication (`pgoutput`,
what Debezium uses) only emits row-level events for row-by-row `DELETE`s —
not for a `TRUNCATE` or a full `catalog-db-data` volume reset. If catalog-db
is ever reset that way, the old keys are stuck in the topics forever (no
tombstone was ever produced for them), and every downstream consumer
inherits the staleness — including the `CASCADE` above, which only ever
fires in reaction to `reporting-db.items`/`users` actually being deleted;
it can't clean up a delete that never happened in the first place.

`reconcile_stale_keys.py` fixes this at the source rather than patching
`reporting-db` in isolation: it diffs catalog-db's current `item_id`/
`user_id` sets against `reporting-db`'s (a live proxy for "every key
currently materialized from the topics") and publishes a tombstone straight
to Kafka for every orphaned key. That's the same mechanism catalog-db's own
row deletes already use downstream, so it cleans every consumer at once —
`reporting-db.items`/`users` via `delete.enabled`, which cascades onward to
`ratings`/`watch_events` via the `CASCADE` above; and `client-input-db`'s
`items`/`users` mirror via its own sink connectors (nothing to cascade onward
to there — `client-input-db` doesn't persist `ratings`/`watch_events`,
see "Retention" above). Not a running service — invoke it manually (see the
docstring at the top of the script for the full one-off `docker run` command)
whenever counts look off after a catalog-db reset.

## Validation

Connector- and cluster-level checks live here; per-job data checks live
in each job's own README (see the table above).

- `docker compose logs connect-register` — `reporting-item-sink-connector`,
  `reporting-user-sink-connector`, `reporting-rating-sink-connector`, and
  `reporting-watch-sink-connector` all register with `RUNNING` state.
- `docker compose exec reporting-db psql -U reporting -d reporting -c "select count(*) from items;"`
  / `"... from users;"` / `"... from ratings;"` / `"... from watch_events;"`
  — counts should track `catalog-db`'s `item`/`app_user` tables (once the
  connector catches up from earliest offset) and `client-input`'s
  rating/watch activity, respectively.
- Delete or edit an item via Catalog Admin (http://localhost:5000) and
  confirm the change (or row removal, for a delete) shows up in
  `reporting-db.items` within a few seconds.
- `docker compose exec reporting-db psql -U reporting -d reporting -c "\d ratings"`
  / `"\d watch_events"` — should list `fk_ratings_item`/`fk_ratings_user`
  / `fk_watch_item`/`fk_watch_user` from the moment `reporting-db` first
  becomes healthy, not just after some traffic has flowed (see
  "Retention" above).
- Delete an item via Catalog Admin that already has ratings/watch events,
  and confirm `reporting-db.ratings`/`watch_events` drop that item's rows
  within a few seconds too — this is the check that verifies the
  `ON DELETE CASCADE` in "Retention" above actually fires.
- All 4 Grafana dashboards reflect live counts without a manual refresh
  (30s auto-refresh).
- `docker compose logs -f <service-name>` for any of the ten jobs —
  each runs silently on a successful tick (no output). Each job's own
  `WATCH_EVENTS_TABLE_EXISTS_SQL`/`RATINGS_TABLE_EXISTS_SQL` check (a
  "table doesn't exist yet, skipping this tick" guard, printed on a
  repeating line if it ever fires) is defensive dead code now that
  `01_schema.sql` creates both tables at `reporting-db` init time — it's
  still there for the same reason `reporting-watch-sink-connector`'s
  tombstone-drop transform is: cheap insurance against an assumption this
  service doesn't fully control turning out to be wrong later.
- `docker compose exec reporting-db psql -U reporting -d reporting -c "select count(*) from item_rating_avg;"`
  — should track the count of items with at least one live rating, and
  update within `TRIGGER_INTERVAL_SECONDS` of a new rating landing.
