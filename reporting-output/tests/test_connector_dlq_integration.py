"""Integration test for reporting-rating-sink-connector's error handling.
Regression test for the 2026-08-15 incident: a Rating.V002 event referencing
an item_id/user_id that doesn't exist in reporting-db violates the FK
constraint (ratings.item_id/user_id REFERENCE items/users, ON DELETE
CASCADE - see ../postgres/init/01_schema.sql). Before errors.tolerance was
configured, Kafka Connect's default (`errors.tolerance=none`) meant that
single failure crashed the whole connector task and halted it for every
user, not just the offending one - reporting-db silently stopped receiving
*any* new ratings/watch_events until someone noticed and manually
intervened. errors.tolerance=all plus a dead-letter-queue (see
../connect/reporting-rating-sink-connector.json) fixes that: a bad record
gets logged and skipped instead. This test proves both halves - the bad
record doesn't crash the connector, and a good record still lands
correctly (so a regression that DLQs *everything* wouldn't silently pass).

Needs the real stack: `docker compose up -d` from the repo root first.
"""
import time
import uuid

import pytest

from conftest import CONNECT_URL, connector_status, topic_total_offset, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("require_live_stack")]

CONNECTOR_NAME = "reporting-rating-sink-connector"
DLQ_TOPIC = "reporting-rating-sink-dlq"


def _produce_rating(rating_producer, user_id, item_id, rating_value=4):
    key = {"user_id": user_id, "item_id": item_id}
    value = {
        "user_id": user_id,
        "item_id": item_id,
        "rating": rating_value,
        "rated_at": int(time.time() * 1000),
    }
    rating_producer.produce(topic="de.iu.Rating.V002", key=key, value=value)
    rating_producer.flush(10)


@pytest.fixture
def existing_user_and_item(reporting_db):
    """A real (user_id, item_id) pair that exists right now in reporting-db
    - needed for the positive-control test, since an FK-satisfying record
    has to reference something that's actually there."""
    with reporting_db.cursor() as cur:
        cur.execute("SELECT user_id FROM users LIMIT 1")
        user_row = cur.fetchone()
        cur.execute("SELECT item_id FROM items LIMIT 1")
        item_row = cur.fetchone()
    if not user_row or not item_row:
        pytest.skip("reporting-db has no users/items yet - seed the catalog first")
    return user_row["user_id"], item_row["item_id"]


def test_connector_is_running_before_the_test_even_starts():
    # Sanity precondition - if this fails, every other test's "stayed
    # RUNNING" assertion is meaningless (it might already be down for an
    # unrelated reason).
    status = connector_status(CONNECTOR_NAME)
    assert status["connector"]["state"] == "RUNNING"
    assert all(t["state"] == "RUNNING" for t in status["tasks"])


def test_fk_violating_rating_does_not_crash_the_connector(rating_producer, reporting_db, raw_consumer):
    ghost_user = f"integration-test-ghost-user-{uuid.uuid4().hex[:8]}"
    ghost_item = f"integration-test-ghost-item-{uuid.uuid4().hex[:8]}"
    dlq_offset_before = topic_total_offset(raw_consumer, DLQ_TOPIC)

    _produce_rating(rating_producer, ghost_user, ghost_item)

    # The DLQ should grow - this specific bad record getting routed there,
    # not silently swallowed or (worse) causing the connector to fall over.
    grew = wait_until(lambda: topic_total_offset(raw_consumer, DLQ_TOPIC) > dlq_offset_before, timeout_s=30)
    assert grew, f"expected {DLQ_TOPIC}'s offset to grow past {dlq_offset_before} within 30s"

    # And it must never have been inserted - the FK violation is real, not
    # just routed to the DLQ *in addition to* landing in the table.
    with reporting_db.cursor() as cur:
        cur.execute("SELECT 1 FROM ratings WHERE user_id = %s AND item_id = %s", (ghost_user, ghost_item))
        assert cur.fetchone() is None

    # The actual regression this guards against: the task used to die here.
    status = connector_status(CONNECTOR_NAME)
    assert status["connector"]["state"] == "RUNNING"
    assert all(t["state"] == "RUNNING" for t in status["tasks"]), status


def test_valid_rating_still_lands_in_reporting_db(rating_producer, reporting_db, existing_user_and_item):
    # Positive control: a fix that routes *everything* to the DLQ (e.g.
    # errors.tolerance=all with a config mistake elsewhere) would pass the
    # test above and still be badly broken. This is what actually catches
    # that: a legitimately valid record must still land in the real table.
    user_id, item_id = existing_user_and_item
    rating_value = 5

    _produce_rating(rating_producer, user_id, item_id, rating_value=rating_value)

    def landed():
        with reporting_db.cursor() as cur:
            cur.execute(
                "SELECT rating FROM ratings WHERE user_id = %s AND item_id = %s", (user_id, item_id),
            )
            row = cur.fetchone()
            return row is not None and row["rating"] == rating_value

    assert wait_until(landed, timeout_s=30), (
        f"expected a rating for ({user_id}, {item_id}) to land in reporting-db within 30s"
    )

    status = connector_status(CONNECTOR_NAME)
    assert status["connector"]["state"] == "RUNNING"
