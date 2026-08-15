"""_handle_progress/open_sessions chunk their state.batch() blocks to at
most PROGRESS_BATCH_SIZE sessions each, instead of one batch for the whole
tick - bounds how many sessions' worth of writes a single commit can be
holding a dashboard write behind (SQLite only allows one writer transaction
open at a time; a very large unbounded batch could make a concurrent
dashboard write wait past its busy-timeout and fail outright). Uses the
commit_counter fixture (conftest.py) to count real commits against a real
per-thread sqlite3.Connection - not the FakeDB, which only stands in for
client-input-db (MySQL), not state.db."""
import state
import generator as generator_module
from generator import DEFAULT_EPISODE_SECONDS


SERIES = {"item_id": "i1", "type": "series", "runtime_minutes": None}


def test_handle_progress_never_produces_to_kafka_while_a_sqlite_batch_is_open(make_generator):
    # Regression test for the actual incident: _handle_progress used to
    # produce watch/rating events to Kafka from inside its state.batch()
    # block. Every finishing session in this test produces at least a
    # watch-progress event (and, with rating forced on, a rating too) -
    # none of those produce() calls should observe batch_depth > 0.
    users = [{"user_id": f"u{i}"} for i in range(5)]
    gen, _ = make_generator(items=[SERIES], users=users)
    state.set_rating_probability(1.0)  # also exercise the rating produce() path
    state.set_abandon_probability(0.0)  # force outcome="finish" for every session -
    # _maybe_rate's abandon branch only rates at rating_probability * 0.3
    # even at the dial's max, so a mix of outcomes wouldn't guarantee all 5
    # rate deterministically the way this test needs.
    for _ in users:
        gen.open_sessions(1)
    state.set_simulation_speed(10_000)  # guarantee every session finishes this tick

    gen._handle_progress()

    assert len(gen._watch_producer.batch_depth_at_produce) == 5
    assert all(depth == 0 for depth in gen._watch_producer.batch_depth_at_produce)
    assert len(gen._rating_producer.batch_depth_at_produce) == 5
    assert all(depth == 0 for depth in gen._rating_producer.batch_depth_at_produce)


def test_handle_progress_under_the_chunk_size_commits_once(make_generator, commit_counter, monkeypatch):
    monkeypatch.setattr(generator_module, "PROGRESS_BATCH_SIZE", 200)
    users = [{"user_id": f"u{i}"} for i in range(5)]
    gen, _ = make_generator(items=[SERIES], users=users)
    for u in users:
        gen.open_sessions(1)  # opens exactly one at a time so batching here isn't what's under test
    commit_counter.clear()

    gen._handle_progress()

    assert len(commit_counter) == 1


def test_handle_progress_over_the_chunk_size_splits_into_multiple_commits(make_generator, commit_counter, monkeypatch):
    monkeypatch.setattr(generator_module, "PROGRESS_BATCH_SIZE", 3)  # force chunking with a small, fast-to-test pool
    users = [{"user_id": f"u{i}"} for i in range(10)]
    gen, _ = make_generator(items=[SERIES], users=users)
    for _ in users:
        gen.open_sessions(1)
    commit_counter.clear()

    gen._handle_progress()

    # 10 sessions / chunk size 3 -> chunks of 3,3,3,1 -> 4 commits, not 1
    # (would defeat the point) and not 10 (the pre-batching behavior).
    assert len(commit_counter) == 4


def test_handle_progress_chunking_does_not_lose_or_duplicate_any_session(make_generator, commit_counter, monkeypatch):
    monkeypatch.setattr(generator_module, "PROGRESS_BATCH_SIZE", 3)
    users = [{"user_id": f"u{i}"} for i in range(10)]
    gen, _ = make_generator(items=[SERIES], users=users)
    for _ in users:
        gen.open_sessions(1)
    import state
    state.set_simulation_speed(1)  # tiny advance, so nobody finishes this tick - isolates "did every row get touched"

    gen._handle_progress()

    sessions = state.active_sessions()
    assert len(sessions) == 10  # nobody dropped
    assert {s["position_seconds"] for s in sessions} == {1}  # every one advanced exactly once, none skipped/double-applied


def test_open_sessions_under_the_chunk_size_commits_once(make_generator, commit_counter, monkeypatch):
    monkeypatch.setattr(generator_module, "PROGRESS_BATCH_SIZE", 200)
    users = [{"user_id": f"u{i}"} for i in range(5)]
    gen, _ = make_generator(items=[SERIES], users=users)
    commit_counter.clear()

    created = gen.open_sessions(5)

    assert created == 5
    assert len(commit_counter) == 1


def test_open_sessions_over_the_chunk_size_splits_into_multiple_commits(make_generator, commit_counter, monkeypatch):
    monkeypatch.setattr(generator_module, "PROGRESS_BATCH_SIZE", 3)
    users = [{"user_id": f"u{i}"} for i in range(10)]
    gen, _ = make_generator(items=[SERIES], users=users)
    commit_counter.clear()

    created = gen.open_sessions(10)

    assert created == 10
    assert len(commit_counter) == 4  # 3,3,3,1
    import state
    assert len(state.active_user_ids()) == 10  # all 10 landed, chunking didn't drop any
