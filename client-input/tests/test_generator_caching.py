"""The _items/_users in-process cache (CATALOG_CACHE_TTL_SECONDS): before
this, every open_sessions/open_custom_session/_continue_or_end_session/
free_user_ids/item_ids call did its own full-table client-input-db query,
serialized behind _db_lock - the main source of "lags under load". These
tests use a controllable fake clock (generator.time.monotonic patched to a
FakeClock) rather than real sleeps, and FakeDB.query_count (conftest.py) to
verify actual round-trip counts, not just returned values."""
import generator as generator_module


class FakeClock:
    def __init__(self, start=0.0):
        self._now = start

    def __call__(self):
        return self._now

    def advance(self, seconds):
        self._now += seconds


def test_cache_disabled_by_default_in_tests_every_call_requeries(make_generator):
    # make_generator defaults to catalog_cache_ttl_seconds=0 specifically so
    # every other test in this suite keeps its pre-caching "always fresh"
    # semantics - confirmed explicitly here.
    gen, fake_db = make_generator(items=[{"item_id": "i1", "type": "movie", "runtime_minutes": 90}])
    gen._items
    gen._items
    assert fake_db.query_count == 2


def test_cache_hit_within_ttl_skips_the_query(make_generator, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    gen, fake_db = make_generator(
        items=[{"item_id": "i1", "type": "movie", "runtime_minutes": 90}], catalog_cache_ttl_seconds=10,
    )

    first = gen._items
    fake_db.query_count = 0  # ignore the constructor-time state, isolate what happens next
    clock.advance(5)  # well inside the 10s TTL
    second = gen._items

    assert fake_db.query_count == 0
    assert second == first


def test_cache_returns_stale_data_within_ttl_even_after_the_pool_changes(make_generator, monkeypatch):
    # The direct consequence of caching: a change to client-input-db's
    # items table isn't visible until the cache expires. Documented as the
    # deliberate tradeoff it is (see CATALOG_CACHE_TTL_SECONDS's comment),
    # not an accident.
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    original_item = {"item_id": "i1", "type": "movie", "runtime_minutes": 90}
    gen, fake_db = make_generator(items=[original_item], catalog_cache_ttl_seconds=10)

    gen._items  # populates the cache
    fake_db.items = [{"item_id": "i2", "type": "series", "runtime_minutes": None}]
    clock.advance(5)  # still inside the TTL

    assert gen._items == [original_item]  # stale on purpose


def test_cache_expires_and_requeries_after_the_ttl_elapses(make_generator, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    original_item = {"item_id": "i1", "type": "movie", "runtime_minutes": 90}
    gen, fake_db = make_generator(items=[original_item], catalog_cache_ttl_seconds=10)

    gen._items
    updated_item = {"item_id": "i2", "type": "series", "runtime_minutes": None}
    fake_db.items = [updated_item]
    clock.advance(10.001)  # just past the TTL boundary

    assert gen._items == [updated_item]


def test_cache_boundary_at_exactly_the_ttl_still_counts_as_expired(make_generator, monkeypatch):
    # (now - cached_at) < TTL is a strict inequality - landing exactly on
    # the boundary must re-query, not treat "just expired" as "still fresh".
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    gen, fake_db = make_generator(items=[{"item_id": "i1", "type": "movie", "runtime_minutes": 90}],
                                   catalog_cache_ttl_seconds=10)

    gen._items
    fake_db.query_count = 0
    clock.advance(10)  # exactly the TTL, not "just under" it

    gen._items
    assert fake_db.query_count == 1


def test_items_and_users_caches_are_independent(make_generator, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    gen, fake_db = make_generator(
        items=[{"item_id": "i1", "type": "movie", "runtime_minutes": 90}],
        users=[{"user_id": "u1"}],
        catalog_cache_ttl_seconds=10,
    )

    gen._items  # caches items only
    fake_db.query_count = 0
    gen._users  # must still be a real query - a shared cache slot would wrongly skip this

    assert fake_db.query_count == 1


def test_cache_collapses_many_calls_across_a_realistic_arrivals_burst(make_generator, monkeypatch):
    # The actual motivating scenario: open_custom_session/_continue_or_end_
    # session each read self._items once per call - a burst of many such
    # calls within one TTL window (e.g. many items finishing in the same
    # tick) should cost one query total, not one per call.
    clock = FakeClock()
    monkeypatch.setattr(generator_module.time, "monotonic", clock)
    gen, fake_db = make_generator(items=[{"item_id": "i1", "type": "movie", "runtime_minutes": 90}],
                                   catalog_cache_ttl_seconds=15)

    for _ in range(50):
        gen._items
        clock.advance(0.1)  # 50 calls span 5s of simulated time, well inside the 15s TTL

    assert fake_db.query_count == 1
