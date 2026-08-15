"""state.batch(): defers every state-mutating call's commit to a single
commit at the end of the block, instead of one fsync-equivalent commit per
call - the fix for _handle_progress doing up to ~2 commits per active
session, every tick. Uses the commit_counter fixture (conftest.py), which
wraps the real per-thread sqlite3.Connection to count actual conn.commit()
calls - sqlite3.Connection is an immutable C type, so it can't be patched
at the class level, and counting calls to state._commit() itself would be
wrong (every mutating function calls it regardless of whether batch()
causes it to skip the real commit)."""
import threading

import pytest

import state


def test_without_batch_each_call_commits_individually(commit_counter):
    state.start_session("u1", "mobile", "i1", 100, "finish", 3)
    state.start_session("u2", "mobile", "i1", 100, "finish", 3)
    assert len(commit_counter) == 2


def test_batch_defers_multiple_writes_to_a_single_commit(commit_counter):
    state.start_session("u1", "mobile", "i1", 100, "finish", 3)
    state.start_session("u2", "mobile", "i1", 100, "finish", 3)
    commit_counter.clear()

    with state.batch():
        state.update_progress("u1", 10)
        state.update_progress("u2", 20)
        state.log_finished_item("u3", "i9", 50, "mobile", "finish", False, state.now_iso())

    assert len(commit_counter) == 1
    # And the batched writes actually landed, not just "fewer commits":
    assert state.get_session("u1")["position_seconds"] == 10
    assert state.get_session("u2")["position_seconds"] == 20
    assert len(state.recent_finished_items()) == 1


def test_batch_with_no_writes_still_commits_exactly_once(commit_counter):
    # An empty batch (e.g. a tick with zero active sessions) shouldn't skip
    # the commit entirely or double up - it should behave like a no-op
    # transaction that still closes cleanly.
    with state.batch():
        pass
    assert len(commit_counter) == 1


def test_nested_batches_commit_only_once_at_the_outermost_exit(commit_counter):
    # _handle_progress's batch() could in principle be entered while some
    # future caller already has one open - reentrancy must not commit once
    # per nesting level.
    with state.batch():
        with state.batch():
            state.start_session("u1", "mobile", "i1", 100, "finish", 3)
            assert len(commit_counter) == 0  # nothing committed yet at the inner exit
        assert len(commit_counter) == 0  # still nothing - outer batch not done yet
    assert len(commit_counter) == 1
    assert state.get_session("u1") is not None  # the write did land


def test_a_write_raising_inside_batch_still_closes_the_transaction(commit_counter):
    # The batch's finally block must run even on an exception - a crashed
    # tick shouldn't leave batch_depth stuck above 0, or every subsequent
    # write on this thread would silently stop committing forever.
    with pytest.raises(RuntimeError):
        with state.batch():
            state.start_session("u1", "mobile", "i1", 100, "finish", 3)
            raise RuntimeError("simulated failure mid-tick")

    assert len(commit_counter) == 1  # the finally block's commit still ran
    assert state.get_session("u1") is not None  # and the write before the raise landed

    commit_counter.clear()
    state.start_session("u2", "mobile", "i1", 100, "finish", 3)
    assert len(commit_counter) == 1  # batching didn't leak into later, unbatched calls


def test_batch_depth_is_a_separate_counter_per_thread():
    # batch_depth lives on the same threading.local() as the connection
    # cache - a dashboard request thread's own write (e.g. the pause
    # button) must not get silently deferred just because the generator
    # thread happens to have a batch open right now. Checked directly
    # rather than by forcing real SQLite lock contention: the two threads'
    # write transactions genuinely can't overlap at the SQLite level
    # regardless of batch_depth (confirmed separately - a write from a
    # second thread while another connection holds an open transaction
    # blocks on SQLite's own busy-timeout, not something state.py controls
    # or this suite should assert exact timing on), so what's actually
    # being verified here is the counter itself, not lock behavior.
    depths_seen = {}

    def record(name):
        depths_seen[name] = getattr(state._local, "batch_depth", 0)

    with state.batch():
        record("main_thread_inside_open_batch")
        t = threading.Thread(target=record, args=("other_thread",))
        t.start()
        t.join()

    assert depths_seen["main_thread_inside_open_batch"] == 1
    assert depths_seen["other_thread"] == 0


def test_wal_mode_is_active_after_init_db():
    with state._lock:
        mode = state._conn().execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
