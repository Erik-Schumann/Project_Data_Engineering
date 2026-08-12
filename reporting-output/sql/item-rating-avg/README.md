# item-rating-avg

All-time average rating per item — the item-side mirror of
`../user-rating-avg/README.md`. A periodic SQL query against
`reporting-db`, same as that job — a trivial addition once `ratings`
existed as a real Postgres table.

## What it computes

For every item with at least one live rating: `avg_rating` (mean of
`rating` across every rating it's ever received) and `rating_count`
(count of *distinct users* who've rated it). Written to
`reporting-db.item_rating_avg`, keyed by `item_id`.

Same rules as `user-rating-avg`, just grouped by `item_id` instead of
`user_id`: latest rating per `(user_id, item_id)` wins (enforced by
`reporting-rating-sink-connector`'s upsert, not by this job), ratings for
a deleted item/user can't exist here in the first place (`ratings`'s
`FOREIGN KEY ... ON DELETE CASCADE` against `items`/`users` — see
`../user-rating-avg/README.md`), and it's a full sync every tick (an
`item_id` missing from this tick's aggregate has its row deleted, not
left stale).

## Job graph

```mermaid
flowchart TD
    subgraph Kafka
        RT["de.iu.Rating.V002<br/>event, 2-day retention"]
    end

    RT -->|reporting-rating-sink-connector<br/>upsert on user_id,item_id| RATINGS[("ratings")]

    subgraph IRA["item-rating-avg (this job)"]
        direction TB
        T["every TRIGGER_INTERVAL_SECONDS"] --> Q["SELECT item_id, avg(rating), count(*)<br/>FROM ratings<br/>GROUP BY item_id"]
        Q --> U["INSERT ... ON CONFLICT (item_id)<br/>DO UPDATE"]
        U --> D["DELETE stale item_ids<br/>(full sync)"]
    end

    RATINGS --> Q
    D --> IRAT[("item_rating_avg")]
    IRAT --> G["Grafana: Avg Rating / Item Detail<br/>(Items dashboard)"]
```

## Validation

```bash
docker compose exec reporting-db psql -U reporting -d reporting \
  -c "select count(*), min(avg_rating), max(avg_rating) from item_rating_avg;"
```
`avg_rating` should be within 1-5 (the rating scale) for every row; rows
should track the count of items with at least one live rating, not the
full catalog.
