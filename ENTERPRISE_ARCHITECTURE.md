# What This Would Take in an Enterprise Setting

`ARCHITECTURE.md` documents this project **as built** — a single-machine
Docker Compose stack sized for a portfolio assignment. This document is
the companion piece: what would actually have to change to run something
shaped like this in production, for a real streaming/analytics team, at
real scale — reliability, scalability, maintainability, and security,
extended with the operational concerns a single-dev/single-machine scope
never has to face.

## Kafka

**Here**: one broker, KRaft combined broker+controller, `replication-factor
1` everywhere (as there is just one broker), plaintext (TLS would have required a dedicated CA and is outscoped of the project)

**Enterprise**:
- **3+ broker cluster**, `replication-factor 3`, `min.insync.replicas 2` —
  survives a single broker loss without data loss or downtime. Dedicated
  controller quorum (3/5/7 nodes) separate from broker nodes past a certain
  cluster size, rather than KRaft's combined mode.
- **Rack/AZ awareness** (`broker.rack`) so replicas land in different
  availability zones, not just different machines.
- **mTLS + SASL/SCRAM**
  certificate authentification and encrypted transport for client-server communication/authentification. Per-service credentials issued through a secrets manager (see
  Security below), not shared `.env` values.
- **ACLs per principal** — today every consumer can read every topic
- **Kafka Connect in distributed mode**, a real worker pool instead of
  the single container this project runs. Dead-letter-queue handling
  itself is partially here already — `reporting-rating-sink-connector`/
  `reporting-watch-sink-connector` both set `errors.tolerance: "all"` plus
  a DLQ topic after a real incident where a single FK-violating record
  (a late event referencing an already-deleted item/user) crashed the
  whole task and silently halted all reporting-db ingestion until someone
  noticed (see `reporting-output/README.md`'s "Connector resilience"
  section). Not extended to the other two sink connectors or the Debezium
  source connector, and single-worker mode still means one Connect
  container going down takes every connector with it regardless of DLQ
  configuration — a real worker pool is what actually fixes that.
- **Tiered storage** (or a shorter retention policy backed by a
  data-lake sink) once topic volume makes broker-local disk the
  bottleneck — not a concern at this project's data volumes, but the
  first thing to hit at real scale for `Watch.Event`-shaped append-only
  topics.
- **Cluster observability**: Prometheus JMX exporters + Grafana (or
  Confluent Control Center / a licensed kPow deployment) watching
  under-replicated partitions, consumer lag per group, and ISR shrink
  events — kPow is already in this stack, but as an optional profile
  glancing at a healthy toy cluster, not wired into any alerting.

## Schema Registry & schema governance

**Here**: one instance, no compatibility enforcement beyond whatever the
registry's own defaults do.

**Enterprise**: HA registry (multiple instances behind a load balancer,
backed by a replicated Kafka topic they already use for this). A CI gate
that runs every schema change against the registry's compatibility API
*before* merge, not just at deploy time — this project relies on
manually remembering to bump `.V00N` on breaking changes (a convention,
not an enforced rule), which doesn't scale past one person who
remembers it.

## Postgres (`catalog-db`, `reporting-db`)

**Here**: single-instance containers, no replicas, no automated backups,
`wal_level=logical` for CDC and nothing else.

**Enterprise**:
- **Managed HA** — RDS Multi-AZ/Aurora, Cloud SQL HA, or a self-managed
  Patroni cluster. Automatic failover, not "restart the container and
  hope the volume survived."
- **Read replicas** for `reporting-db` specifically — Grafana and any
  future BI tool should read from a replica, not the same instance the
  JDBC sink connectors and reporting SQL jobs are writing into.
- **Connection pooling** (PgBouncer) — this project's reporting SQL jobs
  each open their own `psycopg2` connection per trigger; fine at 10 jobs
  on a 60s interval, a real connection-exhaustion risk at real job counts
  or shorter intervals.
- **Automated backups + PITR**, tested restores on a schedule, not just
  "the data's in a Docker volume."
- **`catalog-db`'s logical replication slot** needs monitoring in
  production — an unconsumed slot (e.g. Debezium down for an extended
  period) grows WAL without bound and can take a primary down. This
  project's single-consumer, always-on Debezium setup never surfaces
  that risk locally.

## Reporting SQL jobs

**Here**: ten plain scheduled jobs (`psycopg2`, `TRIGGER_INTERVAL_SECONDS`
timer), each doing a **full recompute every tick** against `reporting-db`
— a `GROUP BY` or a window-function ranking — rather than true
incremental aggregation. That's a real, working tradeoff at this
project's data volumes, and an explicitly unsustainable one past them —
cost grows with total table size, not just the window that actually
matters.

**Enterprise**:
- **Incremental aggregation** — materialized views with incremental
  refresh, or a proper stream-processing layer for the jobs whose
  "recompute everything" pattern stops being cheap at real data volumes
  (rolling-window rankings and all-time aggregates scale very
  differently).
- **Job monitoring & alerting** on job duration vs. trigger interval
  (this project already documented one real incident where a job's batch
  took 4+ minutes against a 60s trigger — see the `segment.bytes`
  writeup in `kafka/create-topics.sh`) and on consumer lag, wired into an
  actual paging system, not "someone notices the dashboard looks stale."
- **A real scheduler** (Airflow, Dagster, or equivalent) instead of each
  job being its own always-on container looping on a fixed interval —
  retries, backfills, and dependency ordering between jobs aren't
  representable today.

## Security

**Here**: session-based single-shared-credential auth on both frontends,
CSRF protection, domain validation on top of Avro schema validation, and
no TLS — proportionate for a single-developer local Docker Compose stack
whose actual audience is a portfolio review, not a shared network with
real adversaries on it. Nothing is encrypted in transit today; every
credential lives in a `.env` file.

### What closing the security gap actually looks like

An enterprise rollout would have to include both TLS (an internal CA
issuing and rotating certificates for Kafka, Postgres, and both
frontends — e.g. HashiCorp Vault's PKI engine or `cert-manager` on
Kubernetes) and licensed Kafka Streams observability (kPow Enterprise or
Confluent Control Center, for `watch-summary-service`'s topology/lag view
— see "Observability" below), alongside:

- **Secrets management** (Vault, AWS Secrets Manager, or equivalent) —
  every credential in this project lives in a `.env` file, committed to
  git because the repo is private (a documented, deliberate call for
  this scope). That call doesn't survive contact with a real team:
  secrets need rotation, per-environment values, and an audit trail of
  who accessed what, none of which a checked-in file provides.
- **RBAC + SSO (OIDC/SAML)** for both frontends, replacing the single
  shared admin credential — today there's no way to tell which admin
  made a given change; per-user accounts and an identity provider fix
  that.
- **mTLS for Kafka** —see above.
- **TLS for PostGres and Frontend** https and TLS for Postgress interaction.
- **Field-level encryption** for `User` PII (age, gender, zip_code) — an
  acknowledged, still-open gap.
- **Data-at-rest encryption** — Postgres storage and Kafka log segments,
  both currently unencrypted at rest; table-level (`pgcrypto`) or
  disk-level (LUKS, or the cloud provider's managed encryption) depending
  on the threat model.
- **Network segmentation** — this whole stack shares one Docker bridge
  network; a real deployment puts Kafka/Postgres in private subnets with
  security groups restricting exactly which services can reach which
  ports, not "anything on the network can reach anything."
- **Audit logging** — who changed what catalog row, who registered which
  connector, who restarted which reporting job. None of that is logged
  today beyond whatever each component's own stdout happens to capture.

## Observability

**Here**: Grafana dashboards over *business* data (ratings, watch
counts) — genuinely useful, but not the same thing as *operational*
observability. kPow gives some Kafka-internals visibility, and is part of
the default stack. Nothing watches JVM health, container resource
pressure, or error rates.

**Enterprise**:
- **Licensed kPow (or equivalent) for Kafka Streams observability** —
  this project runs `kpow-ce` (Community Edition), which shows plain
  consumer-group visibility but not the dedicated Kafka Streams view
  (topology visualization, per-instance state, lag) that
  `watch-summary-service` would benefit from — that view is
  Enterprise-only (kPow Enterprise, or Confluent Control Center).
- **Infrastructure metrics** — Prometheus scraping JMX (Kafka, Kafka
  Connect) and node/container exporters, a *separate* Grafana folder from
  the business dashboards this project already has.
- **Centralized logging** (Loki, ELK, or a managed equivalent) — right
  now, debugging anything means `docker compose logs <service>` by hand
  against whichever container is misbehaving; searchable, correlated logs
  across services would be meaningfully faster.
- **Distributed tracing** (OpenTelemetry) — for tracing one event's
  actual path from `client-input`'s producer through Kafka and into
  `reporting-db`, useful once "why is this row wrong" requires following
  a specific record rather than reasoning about the pipeline in
  aggregate.
- **Alerting on-call rotation** (PagerDuty/Opsgenie or equivalent) wired
  to the infra metrics above, with defined SLOs (e.g. "reporting-db
  reflects a catalog change within N seconds, 99% of the time") — this
  project has no SLOs at all; correctness is checked by hand, per
  `reporting-output/README.md`'s validation section.

## CI/CD

**Here**: automated tests exist per-service now (`client-input`: 103
unit + real-threads/real-SQLite integration tests; `catalog-input`: 158
tests — form validation/seed parsing unit tests plus route-level
integration tests driving every Flask CRUD family end to end against a
real Postgres; `reporting-output`: 14 integration tests against the live
stack, split between connector error-handling (regression-testing the DLQ
incident above) and the 10 scheduled jobs' own SQL aggregation logic —
window filters, tiebreak rules, threshold boundaries — run against a real
reporting-db; `watch-summary`: 5 `TopologyTestDriver` unit tests plus 2
integration tests running the real topology as an actual `KafkaStreams`
instance against a live broker + Schema Registry) — but none of it runs in
CI, only by hand (`pytest`/`gradle test`+`gradle integrationTest`, per
each service's own README). Every other change (Grafana panels, connector
configs, schema) is still validated purely by hand — `docker compose up`,
check the logs, query Postgres directly, look at a Grafana panel.

**Enterprise**:
- **Wire the existing test suites into an actual CI pipeline** — they
  run today, but only when someone remembers to.
- **End-to-end tests** running the full pipeline against ephemeral
  containers, not the already-running dev stack the current integration
  tests assume.
- **Staged environments** (dev → staging → prod) with environment-specific
  config, instead of the single `.env` this project runs everywhere.
- **Infrastructure as Code** (Terraform/Pulumi) provisioning the Kafka
  cluster and Postgres instances, instead of a hand-maintained
  `docker-compose.yml` — appropriate at this project's scope, not at
  production scale.
- **Schema-compatibility CI gate** — see Schema Registry above.
- **Canary or blue-green deploys** for the reporting jobs and Kafka
  Connect connectors, so a bad job version doesn't take down the whole
  reporting pipeline the way redeploying a container does here.

## Data governance

**Here**: no lineage tooling, though it would be a comparatively
low-effort addition — since the full stack is open-source (Postgres,
Kafka/Debezium), tools like OpenMetadata or DataHub could extract
lineage relatively easily, and Kafka Connect natively supports
OpenLineage. Feasible, just not built.

**Enterprise**, beyond lineage:
- **Data quality monitoring** (Great Expectations or equivalent) —
  this project catches data-quality issues by noticing a dashboard number
  looks wrong (e.g. the stale-key incident documented in
  `reporting-output/README.md`), not through automated checks.
- **PII classification & handling** — `User.age`/`gender`/`zip_code`
  aren't tagged or specially handled anywhere in this pipeline today.
  In KPow PII data should be masked using dat apolicies.
- **GDPR right-to-erasure support** —already supported.
- **Retention policy** — `Item.V001`/`User.V001` keep everything
  indefinitely (compacted by design — that's the whole point of a
  changelog topic). `Rating.V002`/`Watch.Event.V002` do have an explicit,
  defensible policy now (`cleanup.policy=delete`, `retention.ms` = 2 days
  — see `ARCHITECTURE.md`'s "Kafka topic catalog"), but 2 days is a
  portfolio-scale number picked mainly to outlast a connector outage, not
  one derived from a real durability/compliance requirement. A real
  deployment needs that derivation, plus a story for what happens if a
  consumer falls behind by more than the retention window (here: nothing
  catches it, the consumer just silently misses those offsets).

## Disaster recovery

**Here**: none — a lost Docker volume is a lost dataset, full stop, and
that's an accepted risk for local/portfolio use only.

**Enterprise**: documented RTO/RPO targets, cross-region Kafka
replication (MirrorMaker 2 or Confluent's Cluster Linking), Postgres
cross-region read replicas or logical replication to a DR site, and
*tested* restore drills on a schedule — a backup nobody has ever
restored from isn't a real backup.

## Cost & scale

Not really addressable at this project's scale, but worth naming: real
partition-count planning (this project fixed 4 partitions per topic
without ever needing to reconsider it), topic retention tuning to
control broker storage cost, autoscaling policies for Kafka Connect
workers tied to actual load rather than a fixed worker count, and
reserved-vs-on-demand infrastructure tradeoffs once the always-on
continuous jobs (10 reporting SQL jobs, `client-input`'s generator,
`watch-summary-service`) represent a real, ongoing cloud bill rather
than a laptop's spare CPU.

---

**How to read this document relative to the rest of the project**: none
of the above is a criticism of what got built. Every gap above was a
scope decision, not an oversight — this document exists to make the
*next* step past each of those decisions concrete, for anyone picking
this project up with production ambitions later.
