"""Core synthetic event generator — runs as a single background thread
(gunicorn is pinned to --workers 1 in the Dockerfile specifically so exactly
one generator thread ever exists; more workers would each spin up their own
generator and double-produce).

A "session" is an active user: at most one per user_id at a time. Each tick:
  1. Arrivals — open new sessions (users who aren't already active start
     watching a first item) from the cached item/user pool.
  2. Progress — advance every active session's current item and emit one
     event to de.iu.Watch.Event.V002 for it, every tick, at whatever
     position it's now at. There's no separate "finished" event any more:
     once position crosses the item's outcome threshold, that tick's event
     just happens to be the last one for this (user_id, item_id) — nothing
     on the wire marks it as such. Downstream (watch-summary-service, a
     Kafka Streams job — see ../watch-summary/README.md) infers "this
     watch is over" from a gap in these events, the same way it would
     infer a genuinely crashed client going silent. With some probability,
     a rating follows once an item concludes.
  3. Continuation — after an item finishes, the session either moves on to
     another item or closes, freeing the user up for a future session.

An item's outcome ('finish' / 'abandon') is decided once when the session
starts watching it, not re-rolled every tick, so behavior stays consistent
across that item's viewing. How many items a session watches before going
inactive is also decided once, at session open — a random cap between 1 and
20 (SESSION_MAX_ITEMS_RANGE), since realistically nobody binges an unbounded
number of items back to back; the session closes as soon as it hits that cap.

The dashboard (app.py) also drives this engine directly, outside the tick
loop: open_sessions(n) starts sessions on demand, end_session_now(user_id)
force-closes one early. Automatic arrival rate/batch size and simulated time
speed are runtime-configurable (state.py's control table, seeded from
ARRIVAL_PROBABILITY/MAX_ARRIVALS_PER_TICK/SIMULATION_SPEED env vars) rather
than fixed constants.

Ratings/watch events go straight to Kafka: a direct
confluent_kafka.SerializingProducer (hand-authored Avro, ../schemas/*.avsc),
produced the moment each ItemFinishedEvent/Rating happens. Deliberately no
persistence layer in between (client-input-db previously played that role,
2026-08-11 through 2026-08-11 — see ../README.md's "How events actually
reach Kafka now" for why that was tried and reverted): these are pure
telemetry, nothing else in this service reads them back, and a lost event
on a container crash is an acceptable, self-correcting gap for synthetic
traffic, not a durability problem worth a database in front of Kafka to
solve.

client-input-db still exists, but only for the item_id/user_id pool now: a
local SQL query against items/users, which
client-input-item-sink-connector/client-input-user-sink-connector keep
mirrored from de.iu.Item.V001/User.V001 — replaces the old in-process
catalog_consumer.py Kafka-Avro consumer entirely (see ../README.md).
"""
import contextlib
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone

import pymysql
import pymysql.cursors
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import state

log = logging.getLogger("client-input.generator")

# Wall-clock seconds between ticks — the real-time cadence of the generator
# loop itself. Distinct from SIMULATION_SPEED below: this is how often the
# clock ticks, that's how much simulated watch-time each tick advances.
# Fixed, not runtime-tunable like the dials below (dashboard/control-table)
# any more: since every tick now also emits a Watch.Event.V002 message per
# in-progress item (see the module docstring), watch-summary-service relies
# on that cadence staying safely under half of its own SESSION_GAP_SECONDS
# (10s) to tell "still watching" apart from "went quiet" — see
# ../watch-summary/README.md. Raising this at runtime would risk false
# "abandoned" summaries mid-watch, so it's a code constant, not a control.
TICK_SECONDS = 5
# Simulated seconds advanced per tick — how fast simulated time proceeds
# relative to the wall clock.
SIMULATION_SPEED = int(os.environ.get("SIMULATION_SPEED", "45"))

DEVICE_TYPES = ["smart_tv", "mobile", "desktop", "tablet", "game_console"]

# Series don't carry a per-episode runtime in the catalog schema — documented
# simplification, a fixed assumed episode length instead.
DEFAULT_EPISODE_SECONDS = 40 * 60

# Chance an item watched is abandoned partway rather than watched to
# completion — runtime-editable (state.py's control table), seeded from this
# env var. This generator itself never simulates a genuine crashed/vanished
# client (every simulated watch still ends via a deliberate finish/abandon
# roll) - but since every tick now emits an event, watch-summary-service
# would treat a real client going silent identically to a normal finish;
# that capability exists at the system level even though this generator
# doesn't exercise it.
ABANDON_PROBABILITY = float(os.environ.get("ABANDON_PROBABILITY", "0.25"))

# How many items a session watches before going inactive — rolled once, at
# session open, as random.randint(1, cap). A "sense-making" limit: nobody
# realistically binges an unbounded number of items back to back in one
# active stretch. The floor is always 1; the cap is runtime-editable (like
# SIMULATION_SPEED above), seeded from this env var.
SESSION_MAX_ITEMS = int(os.environ.get("SESSION_MAX_ITEMS", "20"))

# Per tick, probability of at least one new session opening, and the max
# batch size when one does. Env vars only seed the initial value in SQLite
# (state.py's control table) — after that, the dashboard's config form is
# the live source of truth, so a restart doesn't silently revert an
# operator's runtime tuning.
ARRIVAL_PROBABILITY = float(os.environ.get("ARRIVAL_PROBABILITY", "0.6"))
MAX_ARRIVALS_PER_TICK = int(os.environ.get("MAX_ARRIVALS_PER_TICK", "3"))

# Chance a finished item gets a rating — runtime-editable (state.py's
# control table), seeded from this env var. Abandoners rate at a fixed
# fraction of it rather than getting their own separate dial.
RATING_PROBABILITY = float(os.environ.get("RATING_PROBABILITY", "0.7"))
ABANDON_RATING_SKEW = 0.3

# How long a fetched items/users pool stays valid before the next _items/
# _users access re-queries client-input-db, instead of every single access
# doing its own full-table scan (open_sessions/open_custom_session/
# _continue_or_end_session - once per finished item - and the dashboard's
# free_user_ids()/item_ids() all go through this). The catalog changes on
# CDC-sink cadence (seconds at best, see client-input-item-sink-connector/
# client-input-user-sink-connector), not tick cadence, so a short cache is
# safe staleness, not a correctness risk - and it's the main relief valve
# for _db_lock contention between the tick thread and dashboard requests
# under load, since most calls now never touch client-input-db at all.
CATALOG_CACHE_TTL_SECONDS = int(os.environ.get("CATALOG_CACHE_TTL_SECONDS", "5"))

# state.batch() (see state.py) turns a tick's per-session commits into one
# commit for the whole tick - but SQLite still only allows one writer
# transaction open at a time, and a dashboard write blocked behind an open
# one fails outright (sqlite3.OperationalError: database is locked) past
# the connection's ~5s busy-timeout rather than just waiting longer. Chunking
# bounds how many sessions' worth of writes any single commit can be holding
# up, independent of how large active_sessions()/an arrivals batch gets -
# at realistic session counts this is still one commit per tick/batch either
# way, since len(rows) rarely exceeds this.
PROGRESS_BATCH_SIZE = int(os.environ.get("PROGRESS_BATCH_SIZE", "200"))


def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

DB_HOST = os.environ.get("CLIENT_INPUT_DB_HOST", "client-input-db")
DB_PORT = int(os.environ.get("CLIENT_INPUT_DB_PORT", "3306"))
DB_NAME = os.environ.get("CLIENT_INPUT_DB_NAME", "client_input")
DB_USER = os.environ.get("CLIENT_INPUT_DB_USER", "client_input")
DB_PASSWORD = os.environ.get("CLIENT_INPUT_DB_PASSWORD", "client_input")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMAS_DIR = os.environ.get("SCHEMAS_DIR", "/app/schemas")

RATING_TOPIC = "de.iu.Rating.V002"
WATCH_TOPIC = "de.iu.Watch.Event.V002"


def _connect_db():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor, autocommit=True,
    )


def _load_schema(filename):
    # utf-8-sig: a schema file re-saved on Windows can pick up a UTF-8 BOM,
    # which breaks json.loads/Avro parsing under a plain utf-8 read - same
    # gotcha catalog-input/schemas/*.avsc already hit (see
    # reporting-output/reconcile_stale_keys.py's _load_schema).
    with open(os.path.join(SCHEMAS_DIR, filename), encoding="utf-8-sig") as f:
        return f.read()


def _make_producer(topic, key_schema_file, value_schema_file):
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "key.serializer": AvroSerializer(registry, _load_schema(key_schema_file)),
        "value.serializer": AvroSerializer(registry, _load_schema(value_schema_file)),
    })


def _pick_outcome():
    return "abandon" if random.random() < state.get_abandon_probability() else "finish"


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# --------------------------------------------------------- domain validation --
# Enforced client-side right before every write. The generator is
# well-behaved so this should never actually trip — it's the explicit
# enforcement point the architecture calls for, cheap insurance against a
# generator bug silently producing bad data.

class ValidationError(ValueError):
    pass


def validate_item_finished(event):
    if event["watched_seconds"] <= 0:
        raise ValidationError("watched_seconds must be > 0")
    if not event["device_type"]:
        raise ValidationError("device_type is required")
    if event["session_ended_at"] > _now_ms():
        raise ValidationError("session_ended_at is in the future")


def validate_rating(rating):
    if not (1 <= rating["rating"] <= 5):
        raise ValidationError(f"rating out of range: {rating['rating']}")
    if rating["rated_at"] > _now_ms():
        raise ValidationError("rated_at is in the future")


# -------------------------------------------------------------------- engine --

class Generator:
    def __init__(self):
        self._db = _connect_db()
        self._rating_producer = _make_producer(
            RATING_TOPIC, "de.iu.Rating.V002-key.avsc", "de.iu.Rating.V002-value.avsc",
        )
        self._watch_producer = _make_producer(
            WATCH_TOPIC, "de.iu.Watch.Event.V002-key.avsc", "de.iu.Watch.Event.V002-value.avsc",
        )
        self._stop_flag = threading.Event()
        # Guards session-opening so a manual "add sessions" dashboard
        # request and the automatic per-tick arrival roll can't interleave
        # and race on picking the same free user twice.
        self._arrivals_lock = threading.Lock()
        # pymysql connections aren't safe for concurrent use from multiple
        # threads - the wire protocol is stateful per-connection, so
        # interleaved reads/writes from the tick thread and a Flask request
        # thread (dashboard() calling free_user_ids()/item_ids()) desync it
        # permanently, not just race a query result. Every _cursor() caller
        # goes through this lock instead of a connection pool since query
        # volume here is tiny (a few per tick).
        self._db_lock = threading.Lock()
        # In-process cache for _items/_users (see CATALOG_CACHE_TTL_SECONDS)
        # - separate lock from _db_lock since a cache hit shouldn't need to
        # touch client-input-db (or _db_lock) at all.
        self._catalog_cache_lock = threading.Lock()
        self._items_cache = None
        self._items_cache_at = 0.0
        self._users_cache = None
        self._users_cache_at = 0.0
        self.events_produced = 0

    @contextlib.contextmanager
    def _cursor(self):
        # ping(reconnect=True) transparently reopens a dropped connection
        # (e.g. client-input-db restarting) - cheap enough to check every
        # call at this tick cadence (a few seconds), simpler than a
        # reconnect-on-exception wrapper around every query site. Held
        # under _db_lock for the whole call, not just the ping/cursor()
        # setup, since the caller's query execution is what actually
        # touches the socket.
        with self._db_lock:
            self._db.ping(reconnect=True)
            with self._db.cursor() as cur:
                yield cur

    def _cached_query(self, cache_attr, cache_at_attr, sql):
        # CATALOG_CACHE_TTL_SECONDS <= 0 disables caching outright (every
        # call re-queries) rather than relying on a "now - cached_at < 0"
        # comparison to always be false - explicit rather than incidental,
        # and what tests use to keep the old always-fresh behavior.
        if CATALOG_CACHE_TTL_SECONDS > 0:
            now = time.monotonic()
            with self._catalog_cache_lock:
                cached_value = getattr(self, cache_attr)
                cached_at = getattr(self, cache_at_attr)
                if cached_value is not None and (now - cached_at) < CATALOG_CACHE_TTL_SECONDS:
                    return cached_value
        with self._cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        if CATALOG_CACHE_TTL_SECONDS > 0:
            with self._catalog_cache_lock:
                setattr(self, cache_attr, rows)
                setattr(self, cache_at_attr, time.monotonic())
        return rows

    @property
    def _items(self):
        # Live query against client-input-db's own items table (mirrored
        # from de.iu.Item.V001 by client-input-item-sink-connector), cached
        # for CATALOG_CACHE_TTL_SECONDS (see that constant's comment) rather
        # than re-queried on every single call. catalog_status='active'
        # matches the old catalog_consumer's active_items() filter.
        return self._cached_query(
            "_items_cache", "_items_cache_at",
            "SELECT item_id, type, runtime_minutes FROM items WHERE catalog_status = 'active'",
        )

    @property
    def _users(self):
        return self._cached_query(
            "_users_cache", "_users_cache_at",
            "SELECT user_id FROM users WHERE account_status = 'active'",
        )

    def _target_seconds(self, item):
        if item["type"] == "movie" and item["runtime_minutes"]:
            return int(item["runtime_minutes"]) * 60
        return DEFAULT_EPISODE_SECONDS

    @staticmethod
    def _on_delivery(err, msg):
        # Async delivery report, invoked from poll()/flush() on the calling
        # thread - errors are logged, not raised, since by the time this
        # fires the produce() call that triggered it has long since
        # returned. A produce failure here just means one event silently
        # never reached Kafka; see the module docstring for why that's an
        # acceptable gap for this service rather than something to retry.
        if err is not None:
            log.error("delivery failed for %s: %s", msg.topic() if msg else "?", err)

    def _emit_watch_progress(self, user_id, item_id, device_type, watched_seconds):
        # Called every tick for every in-progress item, not just once at
        # outcome - watched_seconds is just "position right now." Whichever
        # call ends up being the last one for this (user_id, item_id) is
        # the de facto "finished" fact, but nothing here marks it as such;
        # see the module docstring.
        event = {
            "user_id": user_id,
            "item_id": item_id,
            "watched_seconds": watched_seconds,
            "device_type": device_type,
            "session_ended_at": _now_ms(),
        }
        validate_item_finished(event)
        key = {"user_id": user_id, "item_id": item_id}
        self._watch_producer.produce(topic=WATCH_TOPIC, key=key, value=event, on_delivery=self._on_delivery)
        self._watch_producer.poll(0)
        self.events_produced += 1
        return event

    def _emit_rating(self, user_id, item_id, rating_value):
        rating = {
            "user_id": user_id,
            "item_id": item_id,
            "rating": rating_value,
            "rated_at": _now_ms(),
        }
        validate_rating(rating)
        # Produced every time, even for a re-rating of the same
        # (user_id, item_id) - no "latest wins" enforcement needed here any
        # more (that used to be an INSERT ... ON DUPLICATE KEY UPDATE in
        # client-input-db). reporting-rating-sink-connector's own
        # upsert-on-(user_id,item_id) still gives reporting-db "latest
        # wins" regardless of how many times Rating.V002 sees this key.
        key = {"user_id": user_id, "item_id": item_id}
        self._rating_producer.produce(topic=RATING_TOPIC, key=key, value=rating, on_delivery=self._on_delivery)
        self._rating_producer.poll(0)
        self.events_produced += 1

    def _maybe_rate(self, row, watched_seconds):
        # Finishers rate more often, and rate higher, than people who
        # abandoned. rating_probability is the runtime-editable dial for
        # finishers; abandoners rate at a fixed fraction of it (ABANDON_
        # RATING_SKEW) so the "finishers rate more" shape holds at any
        # setting rather than exposing two separate probabilities to tune.
        base_probability = state.get_rating_probability()
        if row["item_outcome"] == "finish":
            if random.random() < base_probability:
                rating_value = random.choices([3, 4, 5], weights=[1, 3, 4])[0]
            else:
                return False
        else:
            if random.random() < base_probability * ABANDON_RATING_SKEW:
                rating_value = random.choices([1, 2, 3], weights=[2, 3, 2])[0]
            else:
                return False
        self._emit_rating(row["user_id"], row["current_item_id"], rating_value)
        return True

    def _finish_item(self, row, watched_seconds):
        """Finish-specific bookkeeping (a maybe-rating + the dashboard log)
        for whatever item a session is currently on — the Kafka event
        itself is the caller's job (either _handle_progress's shared
        per-tick emit, or end_session_now's own). Caller decides what
        happens to the session next — continue to another item, or close
        it."""
        rated = self._maybe_rate(row, watched_seconds)
        state.log_finished_item(
            row["user_id"], row["current_item_id"], watched_seconds, row["device_type"],
            row["item_outcome"], rated, state.now_iso(),
        )

    def _decide_continuation(self, row):
        """Decides what should happen to row's session once its current
        item finishes - continue to a next item (which needs the catalog
        pool, so may touch client-input-db on a cache miss) or end the
        session - without writing anything to state.py. Split from
        _continue_or_end_session so _handle_progress's chunked batches (see
        PROGRESS_BATCH_SIZE) can run every I/O-bound decision before
        opening a state.batch(), instead of interleaving MySQL/Kafka calls
        with SQLite writes inside one open transaction."""
        items_done = row["items_finished"] + 1
        if items_done < row["max_items"]:
            items = self._items
            if items:
                item = random.choice(items)
                outcome = _pick_outcome()
                target_seconds = self._target_seconds(item)
                return ("continue", item["item_id"], target_seconds, outcome)
        return ("end",)

    @staticmethod
    def _apply_continuation(user_id, decision):
        """The pure-SQLite half of _decide_continuation's result - safe to
        call from inside a state.batch() block."""
        if decision[0] == "continue":
            _, item_id, target_seconds, outcome = decision
            state.advance_to_next_item(user_id, item_id, target_seconds, outcome)
        else:
            state.end_session(user_id)

    def _continue_or_end_session(self, row):
        self._apply_continuation(row["user_id"], self._decide_continuation(row))

    def open_sessions(self, n):
        """Opens exactly n new sessions right now (users who aren't already
        active start watching a first item), bypassing the per-tick arrival
        probability — used both by manual dashboard requests and, with n
        picked from the roll below, by the automatic tick. Skips users who
        already have an active session, since at most one is allowed per
        user; returns however many it actually managed to open."""
        items = self._items
        users = self._users
        if not items or not users:
            return 0
        created = 0
        with self._arrivals_lock:
            active_ids = state.active_user_ids()
            free_users = [u for u in users if u["user_id"] not in active_ids]
            random.shuffle(free_users)
            # Chunked the same way and for the same reason as
            # _handle_progress: one commit per chunk of at most
            # PROGRESS_BATCH_SIZE new sessions, not one per session, without
            # letting a single very large n hold a dashboard write off for
            # longer than that bound.
            for chunk in _chunked(free_users[:n], PROGRESS_BATCH_SIZE):
                with state.batch():
                    for user in chunk:
                        item = random.choice(items)
                        device_type = random.choice(DEVICE_TYPES)
                        outcome = _pick_outcome()
                        target_seconds = self._target_seconds(item)
                        max_items = random.randint(1, max(1, state.get_session_max_items()))
                        state.start_session(
                            user["user_id"], device_type, item["item_id"], target_seconds, outcome, max_items,
                        )
                        created += 1
        return created

    def free_user_ids(self):
        """Active users not currently in a session — the pool a custom
        session's user_id field can validly pick from."""
        active_ids = state.active_user_ids()
        return [u["user_id"] for u in self._users if u["user_id"] not in active_ids]

    def item_ids(self):
        return [i["item_id"] for i in self._items]

    def open_custom_session(self, user_id=None, item_id=None, device_type=None, outcome=None, max_items=None):
        """Operator-specified session, for testing a specific user/item/
        device/outcome combination rather than leaving every field to the
        random arrivals roll. Any field left unset (None) falls back to the
        exact same random selection open_sessions() uses. Returns
        (user_id, error) — error is None on success."""
        items = self._items
        users = self._users
        if not items or not users:
            return None, "Catalog pool isn't populated yet."

        if device_type and device_type not in DEVICE_TYPES:
            return None, f"Unknown device type '{device_type}'."
        if outcome and outcome not in ("finish", "abandon"):
            return None, "Outcome must be 'finish' or 'abandon'."
        if max_items is not None and max_items < 1:
            return None, "Max items must be a positive whole number."
        if item_id and not any(i["item_id"] == item_id for i in items):
            return None, f"'{item_id}' is not a known active item."

        with self._arrivals_lock:
            active_ids = state.active_user_ids()
            if user_id:
                if user_id in active_ids:
                    return None, f"{user_id} already has an active session."
                if not any(u["user_id"] == user_id for u in users):
                    return None, f"'{user_id}' is not a known active user."
            else:
                free = [u["user_id"] for u in users if u["user_id"] not in active_ids]
                if not free:
                    return None, "No free users available."
                user_id = random.choice(free)

            item = next((i for i in items if i["item_id"] == item_id), None) if item_id else random.choice(items)
            chosen_device = device_type or random.choice(DEVICE_TYPES)
            chosen_outcome = outcome or _pick_outcome()
            chosen_max_items = (
                max_items if max_items is not None
                else random.randint(1, max(1, state.get_session_max_items()))
            )
            target_seconds = self._target_seconds(item)
            state.start_session(
                user_id, chosen_device, item["item_id"], target_seconds, chosen_outcome, chosen_max_items,
            )
        return user_id, None

    def end_session_now(self, user_id):
        """Operator-triggered early close for a specific user's session:
        cuts the current item short (one last event, frozen at whatever
        the position currently is) and closes the session outright —
        doesn't roll for continuation."""
        row = state.get_session(user_id)
        if row is None:
            return False
        watched_seconds = max(row["position_seconds"], 1)
        self._emit_watch_progress(row["user_id"], row["current_item_id"], row["device_type"], watched_seconds)
        self._finish_item(row, watched_seconds)
        state.end_session(user_id)
        return True

    def _handle_arrivals(self):
        max_arrivals = state.get_max_arrivals_per_tick()
        if max_arrivals < 1:
            return
        if random.random() > state.get_arrival_probability():
            return
        n = random.randint(1, max_arrivals)
        self.open_sessions(n)

    def _handle_progress(self):
        simulation_speed = state.get_simulation_speed()
        # Chunked into blocks of at most PROGRESS_BATCH_SIZE sessions each.
        # Each chunk runs in two passes: first every Kafka produce and
        # catalog (client-input-db) lookup for that chunk, with no SQLite
        # transaction open at all; then a state.batch() that only ever does
        # fast, local, pure-SQLite writes - one commit per chunk instead of
        # one per session, without that commit's write-lock ever being held
        # across a produce()/MySQL round-trip. The two used to be
        # interleaved per-session inside one batch(), which meant a chunk
        # under real load (many sessions finishing the same tick, each
        # doing Avro-serialized Kafka produces) could hold the lock past
        # SQLite's ~5s busy-timeout and fail a concurrent dashboard write
        # outright - not just delay it. See PROGRESS_BATCH_SIZE's comment
        # and state.py's batch() docstring for the rest of the "lags under
        # load" story this and the catalog cache are both addressing.
        for chunk in _chunked(state.active_sessions(), PROGRESS_BATCH_SIZE):
            pending = []
            for row in chunk:
                position = row["position_seconds"] + simulation_speed
                threshold = (
                    row["target_seconds"] * random.uniform(0.85, 1.0)
                    if row["item_outcome"] == "finish"
                    else row["target_seconds"] * random.uniform(0.1, 0.6)
                )
                finished = position >= threshold
                if finished:
                    position = int(threshold)

                # One event per in-progress item, every tick, finished or not -
                # see the module docstring for why there's no separate
                # "finished" event any more.
                self._emit_watch_progress(row["user_id"], row["current_item_id"], row["device_type"], position)

                if finished:
                    rated = self._maybe_rate(row, position)
                    decision = self._decide_continuation(row)
                    pending.append(("finish", row, position, rated, decision))
                else:
                    pending.append(("progress", row["user_id"], position))

            with state.batch():
                for entry in pending:
                    if entry[0] == "progress":
                        _, user_id, position = entry
                        state.update_progress(user_id, position)
                    else:
                        _, row, position, rated, decision = entry
                        state.log_finished_item(
                            row["user_id"], row["current_item_id"], position, row["device_type"],
                            row["item_outcome"], rated, state.now_iso(),
                        )
                        self._apply_continuation(row["user_id"], decision)

    def tick(self):
        if state.is_paused():
            return
        self._handle_arrivals()
        self._handle_progress()

    def run_forever(self):
        state.init_db(
            ARRIVAL_PROBABILITY, MAX_ARRIVALS_PER_TICK, SIMULATION_SPEED,
            SESSION_MAX_ITEMS, RATING_PROBABILITY, ABANDON_PROBABILITY,
        )
        log.info(
            "generator starting: tick=%ss (fixed) speed=%s (catalog pool sourced live from client-input-db)",
            TICK_SECONDS, state.get_simulation_speed(),
        )
        while not self._stop_flag.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("tick failed, continuing")
            time.sleep(TICK_SECONDS)

    def stop(self):
        self._stop_flag.set()
        # Best-effort: flushes whatever's still sitting in each producer's
        # internal buffer so a graceful shutdown doesn't drop the last few
        # events. Bounded wait, not indefinite - a slow/unreachable broker
        # shouldn't hang shutdown.
        self._rating_producer.flush(10)
        self._watch_producer.flush(10)


_generator_instance = None
_generator_thread = None
_start_lock = threading.Lock()


def start_generator_thread():
    global _generator_instance, _generator_thread
    with _start_lock:
        if _generator_thread is not None:
            return _generator_instance
        _generator_instance = Generator()
        _generator_thread = threading.Thread(
            target=_generator_instance.run_forever, daemon=True, name="event-generator"
        )
        _generator_thread.start()
        return _generator_instance


def get_generator():
    return _generator_instance
