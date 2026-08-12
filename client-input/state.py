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
import os
import sqlite3
import threading
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
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return conn


def init_db(arrival_probability_default=0.6, max_arrivals_per_tick_default=3, simulation_speed_default=45,
            session_max_items_default=20, rating_probability_default=0.7,
            abandon_probability_default=0.25):
    with _lock:
        _conn().executescript(SCHEMA)
        _conn().commit()
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
        _conn().commit()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def start_session(user_id, device_type, item_id, target_seconds, outcome, max_items):
    ts = now_iso()
    with _lock:
        _conn().execute(
            """INSERT INTO sessions
               (user_id, device_type, current_item_id, item_outcome,
                position_seconds, target_seconds, items_finished, max_items, session_started_at, item_started_at)
               VALUES (?, ?, ?, ?, 0, ?, 0, ?, ?, ?)""",
            (user_id, device_type, item_id, outcome, target_seconds, max_items, ts, ts),
        )
        _conn().commit()


def advance_to_next_item(user_id, item_id, target_seconds, outcome):
    ts = now_iso()
    with _lock:
        _conn().execute(
            """UPDATE sessions SET current_item_id = ?, item_outcome = ?, target_seconds = ?,
               position_seconds = 0, items_finished = items_finished + 1, item_started_at = ?
               WHERE user_id = ?""",
            (item_id, outcome, target_seconds, ts, user_id),
        )
        _conn().commit()


def update_progress(user_id, position_seconds):
    with _lock:
        _conn().execute(
            "UPDATE sessions SET position_seconds = ? WHERE user_id = ?",
            (position_seconds, user_id),
        )
        _conn().commit()


def end_session(user_id):
    with _lock:
        _conn().execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        _conn().commit()


def log_finished_item(user_id, item_id, watched_seconds, device_type, item_outcome, rated, session_ended_at):
    with _lock:
        _conn().execute(
            """INSERT INTO finished_items
               (user_id, item_id, watched_seconds, device_type, item_outcome, rated, session_ended_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, item_id, watched_seconds, device_type, item_outcome, int(rated), session_ended_at),
        )
        _conn().commit()


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
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'paused'", ("1" if paused else "0",)
        )
        _conn().commit()


def get_arrival_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'arrival_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.6


def set_arrival_probability(value: float):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'arrival_probability'", (str(value),)
        )
        _conn().commit()


def get_max_arrivals_per_tick():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'max_arrivals_per_tick'"
        ).fetchone()
    return int(row["value"]) if row else 3


def set_max_arrivals_per_tick(value: int):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'max_arrivals_per_tick'", (str(value),)
        )
        _conn().commit()


def get_abandon_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'abandon_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.25


def set_abandon_probability(value: float):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'abandon_probability'", (str(value),)
        )
        _conn().commit()


def get_simulation_speed():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'simulation_speed'"
        ).fetchone()
    return int(row["value"]) if row else 45


def set_simulation_speed(value: int):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'simulation_speed'", (str(value),)
        )
        _conn().commit()


def get_session_max_items():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'session_max_items'"
        ).fetchone()
    return int(row["value"]) if row else 20


def set_session_max_items(value: int):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'session_max_items'", (str(value),)
        )
        _conn().commit()


def get_rating_probability():
    with _lock:
        row = _conn().execute(
            "SELECT value FROM control WHERE key = 'rating_probability'"
        ).fetchone()
    return float(row["value"]) if row else 0.7


def set_rating_probability(value: float):
    with _lock:
        _conn().execute(
            "UPDATE control SET value = ? WHERE key = 'rating_probability'", (str(value),)
        )
        _conn().commit()
