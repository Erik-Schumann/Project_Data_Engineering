"""generator.py's domain-validation functions - the explicit enforcement
point right before every Kafka write (see the module docstring's "domain
validation" section). Tests target the actual documented rules and their
boundaries, not just "call it and see it doesn't blow up"."""
import time

import pytest

from generator import ValidationError, validate_item_finished, validate_rating


def _now_ms():
    return int(time.time() * 1000)


def _event(**overrides):
    event = {"watched_seconds": 120, "device_type": "mobile", "session_ended_at": _now_ms()}
    event.update(overrides)
    return event


def _rating(**overrides):
    rating = {"rating": 4, "rated_at": _now_ms()}
    rating.update(overrides)
    return rating


# --------------------------------------------------------- item-finished --

def test_validate_item_finished_accepts_a_well_formed_event():
    validate_item_finished(_event())  # must not raise


@pytest.mark.parametrize("watched_seconds", [0, -1, -3600])
def test_validate_item_finished_rejects_non_positive_watched_seconds(watched_seconds):
    with pytest.raises(ValidationError, match="watched_seconds must be > 0"):
        validate_item_finished(_event(watched_seconds=watched_seconds))


def test_validate_item_finished_accepts_one_second_as_the_floor():
    validate_item_finished(_event(watched_seconds=1))  # boundary: must not raise


@pytest.mark.parametrize("device_type", ["", None])
def test_validate_item_finished_rejects_missing_device_type(device_type):
    with pytest.raises(ValidationError, match="device_type is required"):
        validate_item_finished(_event(device_type=device_type))


def test_validate_item_finished_rejects_session_ended_at_in_the_future():
    with pytest.raises(ValidationError, match="session_ended_at is in the future"):
        validate_item_finished(_event(session_ended_at=_now_ms() + 60_000))


def test_validate_item_finished_accepts_session_ended_at_exactly_now_or_earlier():
    # _now_ms() is called again inside validate_item_finished, strictly later
    # than this event's timestamp - so "now" here is always <= the function's
    # own "now", exercising the non-future boundary without a flaky race.
    validate_item_finished(_event(session_ended_at=_now_ms()))


def test_validate_item_finished_checks_watched_seconds_before_device_type():
    # Bad watched_seconds AND bad device_type at once - confirms the actual
    # check order rather than assuming it, since a reordering would change
    # which message callers see first.
    with pytest.raises(ValidationError, match="watched_seconds must be > 0"):
        validate_item_finished(_event(watched_seconds=0, device_type=""))


# --------------------------------------------------------------- rating --

def test_validate_rating_accepts_a_well_formed_rating():
    validate_rating(_rating())  # must not raise


@pytest.mark.parametrize("value", [1, 5])
def test_validate_rating_accepts_boundary_values(value):
    validate_rating(_rating(rating=value))  # must not raise


@pytest.mark.parametrize("value", [0, -1, 6, 100])
def test_validate_rating_rejects_out_of_range_values(value):
    with pytest.raises(ValidationError, match="rating out of range"):
        validate_rating(_rating(rating=value))


def test_validate_rating_rejects_rated_at_in_the_future():
    with pytest.raises(ValidationError, match="rated_at is in the future"):
        validate_rating(_rating(rated_at=_now_ms() + 60_000))
