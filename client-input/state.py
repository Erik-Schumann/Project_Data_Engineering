"""SQLite store for the generator's in-flight state. Purely operational
bookkeeping for this service (same role as catalog-db's seed_log) — not
part of the Kafka domain model, not CDC'd. Backed by a named Docker volume
(client-input-state) so it survives container restarts. Chosen over an
in-memory dict specifically so state is queryable for debugging/integration
tests, not just something the dashboard can show.

A "session" here means an active user: at most one per user_id at a time.
While active, a session watches items one after another — each one ends
with a row in finished_items (and a matching Kafka publish upstream in
generator.py) before either the session moves on to another item or closes.
"""
import contextlib
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

DB_PATH = os.environ.get("STATE_DB_PATH", "state.db")

# One SQLite connection per thread; the generator thread and Flask request
# threads (gunicorn --workers 1, but still multiple request threads) both
# touch this, so serialize writes with a lock rather than fighting SQLite's
# single-writer model.
_lock = threading.Lock()
_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    user_id              TEXT PRIMARY KEY,   -- at most one active session per user
    device_type           TEXT NOT NULL,
    current_item_id       TEXT NOT NULL,
    item_outcome          TEXT NOT NULL,   -- 'finish' | 'abandon' — decided per item, not per session
    position_seconds      INTEGER NOT NULL DEFAULT 0,
    target_seconds        INTEGER NOT NULL,
    items_finished         INTEGER NOT NULL DEFAULT 0,
    max_items              INTEGER NOT NULL,  -- rolled once at session open (1-20): how many items this user watches before going inactive
    session_started_at    TEXT NOT NULL,
    item_started_at        TEXT NOT NULL,
    created_at             TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS finished_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id              TEXT NOT NULL,
    item_id              TEXT NOT NULL,
    watched_seconds      INTEGER NOT NULL,
    device_type          TEXT NOT NULL,
    item_outcome         TEXT NOT NULL,   -- 'finish' | 'abandon' — not published to Kafka, kept here for the dashboard only
    rated                INTEGER NOT NULL DEFAULT 0,
    session_ended_at     TEXT NOT NULL,
    created_at            TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS control (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""


def _conn():
    conn = getattr(_local, "conn", None)
    if conn is None:
        # timeout=2 (default is 5s) - deliberately short, not long: _write()
        # below owns retry/backoff at the application level now, since
        # SQLITE_LOCKED (unlike SQLITE_BUSY) isn't retried by this
        # connection-level timeout at all, so raising it doesn't help that
        # case and only makes SQLITE_BUSY waits worse. Confirmed empirically
        # (test_integration_sqlite_concurrency.py): an earlier attempt at
        # timeout=20 combined with _write()'s 5-attempt retry compounded to
        # a worst case of 5x20s per write, hanging the test. Two independent
        # short-leash layers beats one layer with a long one.
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=2)
        conn.row_factory = sqlite3.Row
        # WAL instead of the default rollback journal: a tick's batch() (see
        # below) can hold a write transaction open across many active
        # sessions' worth of updates, and WAL lets dashboard reads (and the
        # occasional dashboard-triggered write, once the batch releases)
        # proceed without blocking on it as hard as the default journal
        # mode would. synchronous=NORMAL is the documented safe pairing for
        # WAL - the durability it gives up (losing the most recent commits
        # on an OS crash, not an app crash) matches this table's own
        # "purely operational, not CDC'd, not the source of truth" status
        # from the module docstring.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA wal_autocheckpoint = 10000")
        _local.conn = conn
    return conn


def _commit():
    # Skipped while a batch() block from this same thread is open - the
    # batch's own exit commits once for the whole block instead of once per
    # call. Threads other than the one that opened the batch are unaffected
    # (batch_depth lives on the same thread-local as the connection cache),
    # so e.g. a dashboard request's own writes still commit immediately.
    if getattr(_local, "batch_depth", 0) == 0:
        _conn().commit()


def _write(sql, params=()):
    """execute() + _commit() with a small bounded retry for transient
    SQLite locking, called *without* the caller already holding _lock -
    this acquires it fresh per attempt instead. WAL mode plus the
    connection's own timeout= (see _conn()) absorb most contention, but
    confirmed empirically (test_integration_sqlite_concurrency.py) that a
    single dashboard write landing during a heavily-loaded tick's batch can
    still occasionally hit "database is locked".

    The retry's backoff sleep deliberately happens *outside* _lock: an
    earlier version had every caller do `with _lock: _write(...)`, so a
    thread's whole multi-attempt retry-and-backoff cycle held _lock the
    entire time - which blocked every *other* thread's completely
    unrelated state.py calls (including the tick thread's own
    state.batch() commit) for that whole duration, turning a bounded retry
    into an effective deadlock between the two threads this is supposed to
    keep independent. Confirmed by reproducing the hang, then confirming
    this fix (lock held only per-attempt, not across the backoff) resolves
    it - not just reasoned about.
    """
    last_exc = None
    for attempt in range(5):
        try:
            with _lock:
                _conn().execute(sql, params)
                _commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last_exc = exc
            time.sleep(0.05 * (attempt + 1))
    raise last_exc


@contextlib.contextmanager
def batch():
    """Defers every state-mutating call made on this thread, inside this
    block, to a single commit at the end - turns a tick's N per-session
    writes (update_progress/advance_to_next_item/log_finished_item/
    end_session, once each per active session) into one commit instead of
    N. Reentrant: nested batch() blocks only commit once, at the outermost
    exit."""
    _local.batch_depth = getattr(_local, "batch_depth", 0) + 1
    try:
        yield
    finally:
        _local.batch_depth -= 1
        if _local.batch_depth == 0:
            with _lock:
                _commit()


def init_db(arrival_probability_default=0.6, max_arrivals_per_tick_default=3, simulation_speed_default=45,
            session_max_items_default=20, rating_probability_default=0.7,
            abandon_probability_default=0.25):
    with _lock:
        _conn().executescript(SCHEMA)
        _commit()
        # INSERT OR IGNORE: control values persist across restarts (same
        # volume as the sessions table) — the env-supplied defaults only
        # seed a fresh state.db, they don't clobber a value already changed
        # from the dashboard.
        _conn().executemany(
            "INSERT OR IGNORE INTO control (key, value) VALUES (?, ?)",
            [
                ("paused", "0"),
                ("arrival_probability", str(arrival_probability_default)),
                ("max_arrivals_per_tick", str(max_arrivals_per_tick_default)),
                ("simulation_speed", str(simulation_speed_default)),
                ("session_max_items", str(session_max_items_default)),
                ("rating_probability", str(rating_probability_default)),
                ("abandon_probability", str(abandon_probability_default)),
            ],
        )
        _commit()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def start_session(user_id, device_type, item_id, target_seconds, outcome, max_items):
    ts = now_iso()
    _write(
        """INSERT INTO sessions
           (user_id, device_type, current_item_id, item_outcome,
            position_seconds, target_seconds, items_finished, max_items, session_started_at, item_started_at)
           VALUES (?, ?, ?, ?, 0, ?, 0, ?, ?, ?)""",
        (user_id, device_type, item_id, outcome, target_seconds, max_items, ts, ts),
    )


def advance_to_next_item(user_id, item_id, target_seconds, outcome):
    ts = now_iso()
    _write(
        """UPDATE sessions SET current_item_id = ?, item_outcome = ?, target_seconds = ?,
           position_seconds = 0, items_finished = items_finished + 1, item_started_at = ?
           WHERE user_id = ?""",
        (item_id, outcome, target_seconds, ts, user_id),
    )


def update_progress(user_id, position_seconds):
    _write(
        "UPDATE sessions SET position_seconds = ? WHERE user_id = ?",
        (position_seconds, user_id),
    )


def end_session(user_id):
    _write("DELETE FROM sessions WHERE user_id = ?", (user_id,))


def log_finished_item(user_id, item_id, watched_seconds, device_type, item_outcome, rated, session_ended_at):
    _write(
        """INSERT INTO finished_items
           (user_id, item_id, watched_seconds, device_type, item_outcome, rated, session_ended_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (user_id, item_id, watched_seconds, device_type, item_outcome, int(rated), session_ended_at),
    )


def get_session(user_id):
    with _lock:
        return _conn().execute(
            "SELECT * FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()


def active_sessions():
    with _lock:
        return _conn().execute(
            "SELECT * FROM sessions ORDER BY session_started_at DESC"
        ).fetchall()


def active_user_ids():
    with _lock:
        rows = _conn().execute("SELECT user_id FROM sessions").fetchall()
    return {row["user_id"] for row in rows}


def recent_finished_items(limit=50):
    with _lock:
        return _conn().execute(
            "SELECT * FROM finished_items ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


def device_breakdown():
    with _lock:
        rows = _conn().execute(
            "SELECT device_type, COUNT(*) AS n FROM finished_items GROUP BY device_type ORDER BY n DESC"
        ).fetchall()
    return [{"device_type": r["device_type"], "count": r["n"]} for r in rows]


def counts():
    with _lock:
        active = _conn().execute("SELECT COUNT(*) AS n FROM sessions").fetchone()["n"]
        finished = _conn().execute("SELECT COUNT(*) AS n FROM finished_items").fetchone()["n"]
    return {"active_sessions": active, "finished_items": finished}


def is_paused():
    with _lock:
        row = _conn().execute("SELECT value FROM control WHERE key = 'paused'").fetchone()
    return row is not None and row["value"] == "1"


def set_paused(paused: bool):
    _write("UPDATE control SET value = ? WHERE key = 'paused'", ("1" if paused else "0",))


def get_arrival_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'arrival_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.6


def set_arrival_probability(value: float):
    _write("UPDATE control SET value = ? WHERE key = 'arrival_probability'", (str(value),))


def get_max_arrivals_per_tick():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'max_arrivals_per_tick'"
        ).fetchone()
    return int(row["value"]) if row else 3


def set_max_arrivals_per_tick(value: int):
    _write("UPDATE control SET value = ? WHERE key = 'max_arrivals_per_tick'", (str(value),))


def get_abandon_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'abandon_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.25


def set_abandon_probability(value: float):
    _write("UPDATE control SET value = ? WHERE key = 'abandon_probability'", (str(value),))


def get_simulation_speed():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'simulation_speed'"
        ).fetchone()
    return int(row["value"]) if row else 45


def set_simulation_speed(value: int):
    _write("UPDATE control SET value = ? WHERE key = 'simulation_speed'", (str(value),))


def get_session_max_items():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'session_max_items'"
        ).fetchone()
    return int(row["value"]) if row else 20


def set_session_max_items(value: int):
    _write("UPDATE control SET value = ? WHERE key = 'session_max_items'", (str(value),))


def get_rating_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'rating_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.7


def set_rating_probability(value: float):
    _write("UPDATE control SET value = ? WHERE key = 'rating_probability'", (str(value),))
