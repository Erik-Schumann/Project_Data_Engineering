import threading

import pytest

import generator
import state


@pytest.fixture(autouse=True)
def isolated_state_db(tmp_path, monkeypatch):
    """state.py keys its SQLite connection off the module-level DB_PATH and
    caches one connection per thread in a module-level threading.local(). Point
    both at a fresh, empty file per test so tests can't see each other's rows
    (including via the thread-local cache surviving a DB_PATH monkeypatch)."""
    db_path = tmp_path / "state.db"
    monkeypatch.setattr(state, "DB_PATH", str(db_path))
    monkeypatch.setattr(state, "_local", threading.local())
    state.init_db()
    return state


class _RoutingCursor:
    """Stands in for a pymysql DictCursor. Only understands the two queries
    Generator actually issues (against items/users), matched by substring -
    that's the entire real surface Generator touches on client-input-db."""

    def __init__(self, db):
        self._db = db
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        if "FROM items" in sql:
            self._result = self._db.items
        elif "FROM users" in sql:
            self._result = self._db.users
        else:
            raise AssertionError(f"unexpected query in test: {sql!r}")

    def fetchall(self):
        return self._result


class FakeDB:
    def __init__(self, items=None, users=None):
        self.items = items if items is not None else []
        self.users = users if users is not None else []
        self.query_count = 0  # counts real round-trips - what the catalog cache should cut down on

    def ping(self, reconnect=True):
        pass

    def cursor(self):
        self.query_count += 1
        return _RoutingCursor(self)


class _CommitCountingConnection:
    """Wraps state's real per-thread sqlite3.Connection, counting only
    actual conn.commit() calls - not calls to state._commit(), which is
    invoked by every mutating function regardless of whether batch() causes
    it to skip the real commit. Everything else passes through untouched.
    sqlite3.Connection is an immutable C type, so this wraps the instance
    rather than patching .commit on the class."""

    def __init__(self, real_conn, counter):
        object.__setattr__(self, "_real_conn", real_conn)
        object.__setattr__(self, "_counter", counter)

    def commit(self):
        self._counter.append(1)
        return self._real_conn.commit()

    def __getattr__(self, name):
        return getattr(self._real_conn, name)


@pytest.fixture
def commit_counter(isolated_state_db):
    counter = []
    state._local.conn = _CommitCountingConnection(state._conn(), counter)
    return counter


class FakeProducer:
    def __init__(self):
        self.produced = []
        # batch_depth at the moment of each produce() call - regression
        # tracking for the real 2026-08-15 incident where _handle_progress
        # produced to Kafka from inside an open state.batch(), so a slow
        # produce() (Avro/schema-registry serialization, not free even
        # against a fake broker's network path) could hold state.db's write
        # lock past SQLite's busy-timeout and fail a concurrent dashboard
        # write outright. Should always be 0: Kafka I/O must never happen
        # while a SQLite batch transaction is open.
        self.batch_depth_at_produce = []

    def produce(self, topic, key, value, on_delivery=None):
        self.batch_depth_at_produce.append(getattr(state._local, "batch_depth", 0))
        self.produced.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout=0):
        pass

    def flush(self, timeout=None):
        pass


@pytest.fixture
def make_generator(monkeypatch, isolated_state_db):
    """Builds a real generator.Generator with the MySQL connection and both
    Kafka producers swapped for fakes, so its actual business logic
    (arrivals/progress/continuation/rating) runs for real against state.py
    (isolated SQLite from the fixture above) without touching client-input-db
    or Kafka. Returns (generator_instance, fake_db) so tests can adjust the
    item/user pool after construction too.

    Disables the _items/_users cache (CATALOG_CACHE_TTL_SECONDS=0) by
    default, so every test written before that cache existed keeps seeing
    fake_db mutations immediately - test_generator_caching.py overrides this
    per-test to exercise the cache itself."""

    def _make(items=None, users=None, catalog_cache_ttl_seconds=0):
        fake_db = FakeDB(items=items, users=users)
        monkeypatch.setattr(generator, "_connect_db", lambda: fake_db)
        monkeypatch.setattr(generator, "_make_producer", lambda *a, **k: FakeProducer())
        monkeypatch.setattr(generator, "CATALOG_CACHE_TTL_SECONDS", catalog_cache_ttl_seconds)
        gen = generator.Generator()
        return gen, fake_db

    return _make
