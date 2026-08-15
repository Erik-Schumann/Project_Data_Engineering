"""state.py's SQLite-backed session/control store. Each test runs against a
fresh, isolated DB file (conftest.py's autouse isolated_state_db fixture) so
tests can't see each other's rows. Assertions target the documented
invariants (at most one session per user, control values surviving a
restart, per-key isolation across the six near-identical get/set pairs)
rather than just "insert then read back the same value"."""
import sqlite3

import pytest

import state


CONTROL_ROUND_TRIP_CASES = [
    ("arrival_probability", 0.42),
    ("max_arrivals_per_tick", 7),
    ("abandon_probability", 0.13),
    ("simulation_speed", 999),
    ("session_max_items", 12),
    ("rating_probability", 0.88),
]


# ------------------------------------------------------------------ sessions --

def test_start_session_raises_on_duplicate_user_id():
    # Schema enforces "at most one active session per user" via user_id
    # PRIMARY KEY - this is that invariant's regression test.
    state.start_session("u1", "mobile", "i1", 5400, "finish", 3)
    with pytest.raises(sqlite3.IntegrityError):
        state.start_session("u1", "desktop", "i2", 2400, "abandon", 5)


def test_get_session_returns_none_for_unknown_user():
    assert state.get_session("ghost") is None


def test_update_progress_only_touches_position_seconds():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    state.update_progress("u1", 1234)
    row = state.get_session("u1")
    assert row["position_seconds"] == 1234
    assert row["current_item_id"] == "i1"
    assert row["items_finished"] == 0


def test_advance_to_next_item_resets_position_and_swaps_item_fields():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    state.update_progress("u1", 3000)

    state.advance_to_next_item("u1", "i2", 2400, "abandon")

    row = state.get_session("u1")
    assert row["current_item_id"] == "i2"
    assert row["item_outcome"] == "abandon"
    assert row["target_seconds"] == 2400
    assert row["position_seconds"] == 0
    assert row["items_finished"] == 1


def test_advance_to_next_item_increments_the_existing_count():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    state.advance_to_next_item("u1", "i2", 2400, "abandon")  # 0 -> 1
    state.advance_to_next_item("u1", "i3", 1200, "finish")  # 1 -> 2
    assert state.get_session("u1")["items_finished"] == 2


def test_end_session_removes_the_row_and_frees_the_user():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    assert state.active_user_ids() == {"u1"}

    state.end_session("u1")

    assert state.get_session("u1") is None
    assert state.active_user_ids() == set()


def test_active_user_ids_reflects_only_currently_active_sessions():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    state.start_session("u2", "mobile", "i1", 5400, "finish", 5)
    state.end_session("u1")
    assert state.active_user_ids() == {"u2"}


# ------------------------------------------------------------- finished_items --

def test_log_finished_item_stores_the_given_fields():
    ts = state.now_iso()
    state.log_finished_item("u1", "i1", 321, "tablet", "abandon", True, ts)

    rows = state.recent_finished_items()

    assert len(rows) == 1
    row = rows[0]
    assert row["user_id"] == "u1"
    assert row["item_id"] == "i1"
    assert row["watched_seconds"] == 321
    assert row["device_type"] == "tablet"
    assert row["item_outcome"] == "abandon"
    assert row["rated"] == 1  # bool True stored as SQLite int 1
    assert row["session_ended_at"] == ts


def test_log_finished_item_stores_false_rated_as_zero():
    state.log_finished_item("u1", "i1", 100, "mobile", "finish", False, state.now_iso())
    assert state.recent_finished_items()[0]["rated"] == 0


def test_recent_finished_items_respects_the_limit_argument():
    for i in range(5):
        state.log_finished_item(f"u{i}", "i1", 100, "mobile", "finish", False, state.now_iso())
    assert len(state.recent_finished_items(limit=3)) == 3
    assert len(state.recent_finished_items()) == 5


def test_device_breakdown_groups_and_orders_by_count_descending():
    for _ in range(3):
        state.log_finished_item("u1", "i1", 100, "mobile", "finish", False, state.now_iso())
    state.log_finished_item("u1", "i1", 100, "desktop", "finish", False, state.now_iso())
    for _ in range(2):
        state.log_finished_item("u1", "i1", 100, "tablet", "finish", False, state.now_iso())

    assert state.device_breakdown() == [
        {"device_type": "mobile", "count": 3},
        {"device_type": "tablet", "count": 2},
        {"device_type": "desktop", "count": 1},
    ]


def test_counts_reports_active_sessions_and_finished_items_separately():
    state.start_session("u1", "mobile", "i1", 5400, "finish", 5)
    state.start_session("u2", "mobile", "i1", 5400, "finish", 5)
    state.log_finished_item("u3", "i1", 100, "mobile", "finish", False, state.now_iso())

    assert state.counts() == {"active_sessions": 2, "finished_items": 1}


# ------------------------------------------------------------------- pause --

def test_is_paused_defaults_to_false_after_init_db():
    assert state.is_paused() is False


def test_set_paused_round_trips_both_ways():
    state.set_paused(True)
    assert state.is_paused() is True
    state.set_paused(False)
    assert state.is_paused() is False


# ---------------------------------------------------------------- init_db --

def test_init_db_does_not_overwrite_a_value_already_changed():
    # Documented invariant (state.py's control-table comment): control
    # values persist across a restart. init_db() must use INSERT OR IGNORE,
    # not something that would silently revert an operator's runtime change
    # whenever the container restarts with the env-supplied default.
    state.set_simulation_speed(123)

    state.init_db(simulation_speed_default=999)  # simulates a restart w/ a different env default

    assert state.get_simulation_speed() == 123


def test_control_getters_fall_back_to_hardcoded_defaults_when_unseeded():
    # init_db (already run by the autouse fixture) seeds every key via
    # INSERT OR IGNORE - remove one to exercise the getter's own fallback,
    # the actual safety net for a control table that predates a newly added
    # key (no public API deletes a control row, hence going through the
    # connection directly here).
    with state._lock:
        state._conn().execute("DELETE FROM control WHERE key = 'arrival_probability'")
        state._conn().commit()
    assert state.get_arrival_probability() == 0.6


# ------------------------------------------------- control get/set pairs --

@pytest.mark.parametrize("name,value", CONTROL_ROUND_TRIP_CASES)
def test_control_setter_changes_only_its_own_key(name, value):
    # Six near-identical get/set pairs, each keyed by a distinct SQL string -
    # exactly the shape of code where copy-pasting one pair into the next
    # and forgetting to update the key string is an easy, real mistake.
    # Snapshot every other key first so a wrong key shows up as an
    # unexpected change somewhere else, not just a right value in one place.
    others_before = {
        other: getattr(state, f"get_{other}")()
        for other, _ in CONTROL_ROUND_TRIP_CASES
        if other != name
    }

    getattr(state, f"set_{name}")(value)

    assert getattr(state, f"get_{name}")() == value
    for other, expected in others_before.items():
        assert getattr(state, f"get_{other}")() == expected
