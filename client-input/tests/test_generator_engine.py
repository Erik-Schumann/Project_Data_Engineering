"""Generator's business logic (arrivals, sessions, ratings, continuation),
exercised through the make_generator harness (conftest.py): a real
generator.Generator wired to real state.py (isolated SQLite) with only the
MySQL connection and Kafka producers faked out. Assertions check actual
observable outcomes (DB rows, produced messages) against the documented
rules in generator.py, not just "no exception was raised"."""
import random

import state
import generator as generator_module
from generator import DEVICE_TYPES, DEFAULT_EPISODE_SECONDS, WATCH_TOPIC


MOVIE = {"item_id": "i1", "type": "movie", "runtime_minutes": 90}
SERIES = {"item_id": "i2", "type": "series", "runtime_minutes": None}


# ------------------------------------------------------------ _target_seconds --

def test_target_seconds_movie_with_runtime_uses_runtime_in_seconds(make_generator):
    gen, _ = make_generator()
    assert gen._target_seconds({"type": "movie", "runtime_minutes": 90}) == 90 * 60


def test_target_seconds_movie_without_runtime_falls_back_to_default(make_generator):
    gen, _ = make_generator()
    assert gen._target_seconds({"type": "movie", "runtime_minutes": None}) == DEFAULT_EPISODE_SECONDS


def test_target_seconds_movie_with_zero_runtime_falls_back_to_default(make_generator):
    # 0 is falsy, so `and item["runtime_minutes"]` treats it the same as
    # missing - a genuine edge case in the source's truthiness check.
    gen, _ = make_generator()
    assert gen._target_seconds({"type": "movie", "runtime_minutes": 0}) == DEFAULT_EPISODE_SECONDS


def test_target_seconds_series_ignores_runtime_minutes(make_generator):
    gen, _ = make_generator()
    assert gen._target_seconds({"type": "series", "runtime_minutes": 90}) == DEFAULT_EPISODE_SECONDS


# -------------------------------------------------------------- open_sessions --

def test_open_sessions_returns_zero_when_no_items(make_generator):
    gen, _ = make_generator(items=[], users=[{"user_id": "u1"}])
    assert gen.open_sessions(5) == 0
    assert state.active_user_ids() == set()


def test_open_sessions_returns_zero_when_no_users(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[])
    assert gen.open_sessions(5) == 0


def test_open_sessions_skips_users_already_in_an_active_session(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}, {"user_id": "u2"}])
    state.start_session("u1", "mobile", MOVIE["item_id"], 5400, "finish", 3)

    created = gen.open_sessions(5)

    assert created == 1
    assert state.active_user_ids() == {"u1", "u2"}


def test_open_sessions_never_creates_more_than_n(make_generator):
    users = [{"user_id": f"u{i}"} for i in range(5)]
    gen, _ = make_generator(items=[MOVIE], users=users)

    created = gen.open_sessions(2)

    assert created == 2
    assert len(state.active_user_ids()) == 2


def test_open_sessions_with_n_zero_creates_nothing(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    assert gen.open_sessions(0) == 0
    assert state.active_user_ids() == set()


def test_open_sessions_sets_current_item_and_computed_target_seconds(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    gen.open_sessions(1)
    row = state.get_session("u1")
    assert row["current_item_id"] == MOVIE["item_id"]
    assert row["target_seconds"] == MOVIE["runtime_minutes"] * 60
    assert row["items_finished"] == 0
    assert row["position_seconds"] == 0


# --------------------------------------------------------------- id helpers --

def test_free_user_ids_excludes_active_users(make_generator):
    users = [{"user_id": "u1"}, {"user_id": "u2"}, {"user_id": "u3"}]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.start_session("u2", "mobile", MOVIE["item_id"], 5400, "finish", 3)

    assert gen.free_user_ids() == ["u1", "u3"]


def test_item_ids_lists_every_item_in_the_pool(make_generator):
    gen, _ = make_generator(items=[MOVIE, SERIES], users=[])
    assert gen.item_ids() == ["i1", "i2"]


# ---------------------------------------------------------- open_custom_session --

def test_open_custom_session_rejects_empty_catalog_pool(make_generator):
    gen, _ = make_generator(items=[], users=[])
    user_id, error = gen.open_custom_session(user_id="u1")
    assert user_id is None
    assert error == "Catalog pool isn't populated yet."


def test_open_custom_session_rejects_unknown_device_type(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="u1", device_type="fax_machine")
    assert user_id is None
    assert "Unknown device type" in error
    assert state.active_user_ids() == set()


def test_open_custom_session_rejects_unknown_outcome(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="u1", outcome="rage_quit")
    assert user_id is None
    assert error == "Outcome must be 'finish' or 'abandon'."


def test_open_custom_session_rejects_max_items_below_one(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="u1", max_items=0)
    assert user_id is None
    assert error == "Max items must be a positive whole number."


def test_open_custom_session_accepts_max_items_of_exactly_one(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="u1", max_items=1)
    assert error is None
    assert state.get_session("u1")["max_items"] == 1


def test_open_custom_session_rejects_unknown_item_id(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="u1", item_id="does-not-exist")
    assert user_id is None
    assert "is not a known active item" in error


def test_open_custom_session_rejects_a_user_already_in_a_session(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", MOVIE["item_id"], 5400, "finish", 3)

    user_id, error = gen.open_custom_session(user_id="u1")

    assert user_id is None
    assert "already has an active session" in error


def test_open_custom_session_rejects_unknown_user_id(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    user_id, error = gen.open_custom_session(user_id="not-a-real-user")
    assert user_id is None
    assert "is not a known active user" in error


def test_open_custom_session_rejects_when_no_free_users_and_none_specified(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", MOVIE["item_id"], 5400, "finish", 3)

    user_id, error = gen.open_custom_session()  # user_id omitted

    assert user_id is None
    assert error == "No free users available."


def test_open_custom_session_success_uses_every_specified_field(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])

    user_id, error = gen.open_custom_session(
        user_id="u1", item_id="i1", device_type="tablet", outcome="abandon", max_items=7,
    )

    assert error is None
    assert user_id == "u1"
    row = state.get_session("u1")
    assert row["device_type"] == "tablet"
    assert row["item_outcome"] == "abandon"
    assert row["max_items"] == 7
    assert row["current_item_id"] == "i1"
    assert row["target_seconds"] == MOVIE["runtime_minutes"] * 60


def test_open_custom_session_success_fills_in_unspecified_fields(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])

    user_id, error = gen.open_custom_session()  # everything auto-picked

    assert error is None
    assert user_id == "u1"  # only free user
    row = state.get_session("u1")
    assert row["current_item_id"] == "i1"  # only item
    assert row["device_type"] in DEVICE_TYPES
    assert row["item_outcome"] in ("finish", "abandon")
    assert row["max_items"] >= 1


# ------------------------------------------------------------ end_session_now --

def test_end_session_now_returns_false_for_unknown_user(make_generator):
    gen, _ = make_generator()
    assert gen.end_session_now("ghost") is False


def test_end_session_now_floors_watched_seconds_to_one_second(make_generator):
    # A session ended the instant it opens has position_seconds == 0.
    # validate_item_finished (see test_generator_validation.py) requires
    # watched_seconds > 0, so end_session_now must floor to 1 rather than
    # pass 0 straight through - this is exactly that floor's regression
    # test: without it, this call would raise ValidationError.
    gen, fake_db = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.set_rating_probability(0.0)  # keep the produced-message count deterministic
    state.start_session("u1", "mobile", "i1", 5400, "finish", 3)
    assert state.get_session("u1")["position_seconds"] == 0

    result = gen.end_session_now("u1")

    assert result is True
    assert state.get_session("u1") is None
    watch_producer = gen._watch_producer
    assert len(watch_producer.produced) == 1
    assert watch_producer.produced[0]["value"]["watched_seconds"] == 1
    assert watch_producer.produced[0]["topic"] == WATCH_TOPIC


def test_end_session_now_records_a_finished_item_and_removes_the_session(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.set_rating_probability(0.0)
    state.start_session("u1", "smart_tv", "i1", 5400, "finish", 3)
    state.update_progress("u1", 200)

    gen.end_session_now("u1")

    assert state.get_session("u1") is None
    finished = state.recent_finished_items()
    assert len(finished) == 1
    assert finished[0]["watched_seconds"] == 200
    assert finished[0]["device_type"] == "smart_tv"
    assert finished[0]["rated"] == 0


# ------------------------------------------------------------------ _maybe_rate --

def test_maybe_rate_never_fires_when_probability_is_zero(make_generator):
    gen, _ = make_generator()
    state.set_rating_probability(0.0)
    row = {"item_outcome": "finish", "user_id": "u1", "current_item_id": "i1"}

    assert gen._maybe_rate(row, 100) is False
    assert gen._rating_producer.produced == []


def test_maybe_rate_finishers_always_rate_3_to_5_when_probability_is_one(make_generator):
    gen, _ = make_generator()
    state.set_rating_probability(1.0)
    row = {"item_outcome": "finish", "user_id": "u1", "current_item_id": "i1"}

    results = [gen._maybe_rate(row, 100) for _ in range(15)]

    assert all(results)  # the gate was open on every single call
    values = [m["value"]["rating"] for m in gen._rating_producer.produced]
    assert len(values) == 15
    assert all(v in (3, 4, 5) for v in values)


def test_maybe_rate_abandoners_rate_1_to_3_when_gate_forced_open(make_generator, monkeypatch):
    import generator as generator_module

    gen, _ = make_generator()
    state.set_rating_probability(1.0)
    # Abandoners rate at probability * ABANDON_RATING_SKEW (0.3), so
    # probability=1.0 alone doesn't guarantee the gate opens - force
    # random.random() to the minimum so it does, deterministically.
    monkeypatch.setattr(generator_module.random, "random", lambda: 0.0)
    row = {"item_outcome": "abandon", "user_id": "u1", "current_item_id": "i1"}

    results = [gen._maybe_rate(row, 50) for _ in range(15)]

    assert all(results)
    values = [m["value"]["rating"] for m in gen._rating_producer.produced]
    assert len(values) == 15
    assert all(v in (1, 2, 3) for v in values)


def test_maybe_rate_abandoners_never_fire_when_probability_is_zero(make_generator):
    gen, _ = make_generator()
    state.set_rating_probability(0.0)
    row = {"item_outcome": "abandon", "user_id": "u1", "current_item_id": "i1"}

    assert gen._maybe_rate(row, 50) is False
    assert gen._rating_producer.produced == []


# ----------------------------------------------------- _continue_or_end_session --

def test_continue_or_end_session_advances_to_a_new_item_when_under_the_cap(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", "i1", 5400, "finish", max_items=5)
    row = state.get_session("u1")  # items_finished=0, max_items=5

    gen._continue_or_end_session(row)

    updated = state.get_session("u1")
    assert updated is not None
    assert updated["items_finished"] == 1
    assert updated["position_seconds"] == 0
    assert updated["current_item_id"] == "i1"  # only item in the pool


def test_continue_or_end_session_ends_when_item_cap_reached(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", "i1", 5400, "finish", max_items=1)
    row = state.get_session("u1")  # items_finished=0, max_items=1 -> items_done=1, not < 1

    gen._continue_or_end_session(row)

    assert state.get_session("u1") is None


def test_continue_or_end_session_ends_when_no_items_left_in_pool_even_under_cap(make_generator):
    gen, fake_db = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", "i1", 5400, "finish", max_items=5)
    row = state.get_session("u1")  # items_done=1 < 5, but the catalog pool is about to run dry
    fake_db.items = []  # simulate the pool emptying out between the session opening and now

    gen._continue_or_end_session(row)

    assert state.get_session("u1") is None


# ------------------------------------------------------------------------ tick --

def test_tick_does_nothing_while_paused(make_generator):
    gen, _ = make_generator(items=[MOVIE], users=[{"user_id": "u1"}])
    state.start_session("u1", "mobile", "i1", 5400, "finish", 3)
    state.set_paused(True)

    gen.tick()

    assert state.get_session("u1")["position_seconds"] == 0
    assert gen._watch_producer.produced == []


def test_tick_advances_and_closes_a_session_that_finishes_this_tick(make_generator):
    gen, _ = make_generator(items=[SERIES], users=[{"user_id": "u1"}])
    state.set_max_arrivals_per_tick(0)  # keep this tick deterministic: no new arrivals
    state.set_rating_probability(0.0)  # ...and no extra Kafka message from a rating
    state.set_simulation_speed(10_000)  # comfortably exceeds any finish threshold below
    # DEFAULT_EPISODE_SECONDS (2400) * up to 1.0 is the highest possible
    # finish threshold for a "finish"-outcome item - 10_000 clears it regardless
    # of the random uniform(0.85, 1.0) multiplier baked into _handle_progress.
    state.start_session("u1", "mobile", "i2", DEFAULT_EPISODE_SECONDS, "finish", max_items=1)

    gen.tick()

    assert state.get_session("u1") is None  # max_items=1 -> session closes after this item
    assert len(gen._watch_producer.produced) == 1
    assert gen._watch_producer.produced[0]["value"]["user_id"] == "u1"


# ------------------------------------------------ arrivals batch-size behavior --
# max_arrivals_per_tick is a CEILING per tick, not a guaranteed count:
# _handle_arrivals rolls n = random.randint(1, max_arrivals_per_tick) every
# tick it fires. These pin down exactly what that means in practice - the
# real-world question being "why isn't it opening 300 sessions when I set
# arrival_probability=1 and have 300 new users available."

def test_handle_arrivals_gate_never_skips_when_probability_is_one(make_generator):
    # random.random() is in [0, 1), so `random.random() > 1.0` is always
    # False - the skip-this-tick branch can never fire at probability=1.0.
    users = [{"user_id": f"u{i}"} for i in range(300)]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(300)

    gen._handle_arrivals()

    assert len(state.active_user_ids()) >= 1


def test_handle_arrivals_can_open_as_few_as_one_session_even_at_max_settings(make_generator, monkeypatch):
    # The actual "why isn't it opening 300 sessions" mechanism: even with
    # arrival_probability=1.0 (gate always open) and max_arrivals_per_tick=300
    # with all 300 users free, a single tick can still open just 1 session,
    # because n = random.randint(1, 300) is uniform across that whole range,
    # not concentrated near the ceiling. Forcing randint to its own lower
    # bound makes this deterministic instead of a rare, hard-to-catch case.
    users = [{"user_id": f"u{i}"} for i in range(300)]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(300)
    monkeypatch.setattr(generator_module.random, "randint", lambda lo, hi: lo)

    gen._handle_arrivals()

    assert len(state.active_user_ids()) == 1


def test_handle_arrivals_reaches_the_full_ceiling_when_the_roll_maxes_out(make_generator, monkeypatch):
    # The other boundary: when the roll does land on the ceiling, all 300
    # free users get a session in that one tick - confirms the ceiling
    # itself is wired correctly (open_sessions(n) isn't silently capped
    # below n some other way).
    users = [{"user_id": f"u{i}"} for i in range(300)]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(300)
    monkeypatch.setattr(generator_module.random, "randint", lambda lo, hi: hi)

    gen._handle_arrivals()

    assert len(state.active_user_ids()) == 300


def test_handle_arrivals_at_the_project_default_batch_size_needs_many_ticks_for_300_users(make_generator):
    # Unmocked, realistic randomness at MAX_ARRIVALS_PER_TICK's actual
    # project default (3, see generator.py) - 10 ticks * at most 3 users
    # each caps out well below 300, which is exactly the "it's trickling in
    # a few at a time, not all at once" behavior an operator would observe
    # unless they also raise max_arrivals_per_tick, not just the probability.
    users = [{"user_id": f"u{i}"} for i in range(300)]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(3)  # the actual project default

    for _ in range(10):
        gen._handle_arrivals()

    assert 1 <= len(state.active_user_ids()) <= 30


def test_handle_arrivals_does_nothing_when_max_arrivals_per_tick_is_zero(make_generator):
    users = [{"user_id": f"u{i}"} for i in range(300)]
    gen, _ = make_generator(items=[MOVIE], users=users)
    state.set_arrival_probability(1.0)
    state.set_max_arrivals_per_tick(0)

    gen._handle_arrivals()

    assert state.active_user_ids() == set()
