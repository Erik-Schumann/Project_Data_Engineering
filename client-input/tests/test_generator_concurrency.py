"""Regression test for the real client-input incident (2026-08-15): the
dashboard route (free_user_ids()/item_ids()) and the background tick
thread shared one pymysql connection with no locking. Concurrent queries
from the two threads interleaved reads on the same socket and permanently
desynced the MySQL wire protocol (a struct.error on the next read), leaving
the tick thread stuck in a blocking read - "the service doesn't react."

The fix (generator.py's _cursor()) holds _db_lock for the whole query, not
just connection setup. This test proves that lock actually serializes
concurrent callers - not just that a Lock object exists somewhere - by
making the fake DB's query execution slow enough that two threads calling
in at the same moment would visibly overlap if the lock didn't hold.
"""
import threading
import time

import generator as generator_module
from conftest import FakeProducer


class _SlowTrackingCursor:
    def __init__(self, db):
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        db = self._db
        with db.tracker_lock:
            db.concurrent += 1
            db.max_concurrent = max(db.max_concurrent, db.concurrent)
        # Long enough that two unsynchronized threads reliably overlap;
        # short enough the test still runs fast.
        time.sleep(0.03)
        with db.tracker_lock:
            db.concurrent -= 1

    def fetchall(self):
        return []


class SlowTrackingDB:
    """Stands in for the pymysql connection - every query "executes" slowly
    while under a shared counter, so any overlap between two threads' calls
    shows up as concurrent > 1."""

    def __init__(self):
        self.tracker_lock = threading.Lock()
        self.concurrent = 0
        self.max_concurrent = 0

    def ping(self, reconnect=True):
        pass

    def cursor(self):
        return _SlowTrackingCursor(self)


def test_cursor_serializes_concurrent_callers_across_threads(monkeypatch, isolated_state_db):
    slow_db = SlowTrackingDB()
    monkeypatch.setattr(generator_module, "_connect_db", lambda: slow_db)
    monkeypatch.setattr(generator_module, "_make_producer", lambda *a, **k: FakeProducer())
    gen = generator_module.Generator()

    # A realistic mix of what actually raced in production: the dashboard
    # route calling free_user_ids()/item_ids() while ticks are in flight.
    calls = [gen.free_user_ids, gen.item_ids] * 4
    threads = [threading.Thread(target=call) for call in calls]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert slow_db.max_concurrent == 1, (
        f"expected every _cursor() call to be serialized, but saw "
        f"{slow_db.max_concurrent} execute() calls overlap - the DB lock "
        f"isn't actually preventing concurrent access"
    )


def test_cursor_without_the_lock_would_actually_overlap(monkeypatch, isolated_state_db):
    # Negative control for the test above: proves _SlowTrackingCursor's
    # instrumentation is capable of detecting an overlap at all (i.e. the
    # previous test isn't passing merely because 0.03s is too short to ever
    # overlap, or because threading.Thread happens to run serially here).
    # Bypasses generator.py's _cursor() entirely and hits the fake DB
    # directly and unsynchronized, the way the pre-fix code effectively did.
    slow_db = SlowTrackingDB()

    def unsynchronized_query():
        with slow_db.cursor() as cur:
            cur.execute("SELECT 1")

    threads = [threading.Thread(target=unsynchronized_query) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert slow_db.max_concurrent > 1
