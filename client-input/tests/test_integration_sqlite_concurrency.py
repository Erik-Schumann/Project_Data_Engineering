"""Integration test: state.db under sustained concurrent load from a real
background tick loop plus a real dashboard-style write, both running as
actual threads with actual disk I/O and actual timing - not the isolated,
single-call-at-a-time shape the rest of the unit suite uses. client-input-db
(MySQL) and Kafka are still faked (FakeDB/FakeProducer, via the
make_generator fixture) - this test is specifically about state.db's real
SQLite behavior under concurrency, not the full docker stack, so it needs
no Docker and stays fast enough to run alongside the unit suite.

Exactly one dashboard-writer thread, not several: client-input's Dockerfile
pins gunicorn to `--workers 1` (a sync worker), so production never has
more than one HTTP request in flight at a time - the real maximum
concurrency here is the one background generator thread plus whichever
single request the one worker happens to be handling.

This test's own history is worth keeping: it went through two real fixes
before it passed reliably, both discovered by actually running it, not by
reasoning about the code.

1. An earlier version used three concurrent dashboard-writer threads "for
   extra pressure". That produced real `database is locked` errors and a
   16-second tick, but it was testing an unrepresentative failure mode -
   multiple simultaneous writers SQLite's single-writer model was never
   going to tolerate well, a scenario this service's single-worker
   deployment can't actually produce. Cut to one writer thread.

2. Even with one writer thread at a realistic interval, `database is
   locked` still occurred - reproducibly, not a rare flake. Raising the
   connection's busy-timeout (state.py's `_conn()`, 5s -> 20s) had *no*
   effect at all, which ruled out a simple "waited and gave up" explanation
   (SQLITE_BUSY) and pointed at a different SQLite error class
   (SQLITE_LOCKED) that isn't retried by the connection's own busy-timeout
   the same way. Fixed with an explicit, bounded application-level retry
   in state.py's `_write()` instead of relying on SQLite's internal
   handling - confirmed this actually closes the gap by rerunning the same
   reproduction in isolation before trusting pytest's single run of it.
"""
import threading
import time

import pytest

import state


ITEMS = [{"item_id": f"i{i}", "type": "series", "runtime_minutes": None} for i in range(5)]
USERS = [{"user_id": f"u{i}"} for i in range(500)]


@pytest.mark.integration
def test_sustained_ticks_never_lock_out_a_concurrent_dashboard_write(make_generator):
    gen, _ = make_generator(items=ITEMS, users=USERS)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(200)  # matches PROGRESS_BATCH_SIZE's default - realistic "heavy" load, not an extreme
    state.set_simulation_speed(10_000)  # most active sessions finish nearly every tick
    state.set_rating_probability(1.0)  # exercises the rating-produce path inside _handle_progress too

    stop = threading.Event()
    errors = []
    errors_lock = threading.Lock()
    dashboard_writes_completed = 0
    counter_lock = threading.Lock()

    def record_error(exc):
        with errors_lock:
            errors.append(exc)

    def tick_loop():
        while not stop.is_set():
            try:
                gen.tick()
            except Exception as exc:  # noqa: BLE001 - deliberately broad, this is the failure mode under test
                record_error(exc)
            # Production always sleeps TICK_SECONDS (5s) between ticks,
            # regardless of how long tick() itself took (see run_forever) -
            # a zero-gap tight loop here would queue up far more concurrent
            # batch-open windows than the real incident ever involved and
            # test a different, more pathological failure mode than the one
            # this is meant to guard against. A short-but-nonzero gap keeps
            # this fast while still giving each batch room to actually close.
            time.sleep(0.05)

    def dashboard_writer():
        nonlocal dashboard_writes_completed
        while not stop.is_set():
            try:
                # Same shape as app.py's /config route: a single control-table
                # write from a thread that isn't the generator's own.
                state.set_arrival_probability(1.0)
            except Exception as exc:  # noqa: BLE001
                record_error(exc)
            else:
                with counter_lock:
                    dashboard_writes_completed += 1
            time.sleep(0.01)

    threads = [threading.Thread(target=tick_loop), threading.Thread(target=dashboard_writer)]
    for t in threads:
        t.start()

    time.sleep(1.5)  # sustained concurrent load, not a single lucky interleaving

    stop.set()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive(), "a thread didn't stop - something is still wedged"

    assert errors == [], f"{len(errors)} error(s) during sustained concurrent load, first: {errors[0] if errors else None}"
    # Not just "no errors" - the dashboard write must have actually made
    # forward progress, not merely survived by getting starved to zero for
    # the whole window. Not a high bar: under this deliberately heavy,
    # sustained tick load, each write genuinely does take a while (measured
    # ~2 completions in the 1.5s window, consistently, across repeated
    # runs) - that's real, honest contention overhead from _write()'s
    # retry/backoff, not a bug to hide by asserting a number this scenario
    # can't actually reach. The bugs this test exists to catch are the ones
    # above: errors, and threads that don't stop.
    assert dashboard_writes_completed >= 1
