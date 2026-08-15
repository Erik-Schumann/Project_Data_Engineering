"""Catalog Admin's form-parsing/validation helpers (app.py). Deliberately
adversarial: numeric-looking-but-not-quite strings, boundary values, and the
exact validation-order/whitelist-fallback contracts the routes rely on -
not just a happy-path submission per function."""
import pytest

import app as app_module
from app import (
    ACCOUNT_STATUSES,
    CATALOG_STATUSES,
    ITEMS_SORT_COLUMNS,
    ITEM_TYPES,
    SUBSCRIPTION_PLANS,
    parse_float,
    parse_int,
    read_item_form,
    read_user_form,
    sort_params,
)


# -------------------------------------------------------------- parse_int --

@pytest.mark.parametrize("raw,expected", [("42", 42), ("-3", -3), ("  7  ", 7), ("+5", 5), ("007", 7)])
def test_parse_int_accepts_well_formed_integers(raw, expected):
    assert parse_int(raw) == (expected, True)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_int_blank_is_none_but_not_an_error(raw):
    # Distinguishes "field left empty" (ok=True, value=None -> NULL in the
    # DB) from "field filled in with garbage" (ok=False -> form re-shown
    # with an error) - callers depend on this exact (None, True) vs.
    # (None, False) split.
    assert parse_int(raw) == (None, True)


@pytest.mark.parametrize("raw", [
    "abc", "12.5", "1,000", "0x1A", "42.0", "1e3", "inf", "nan", "NaN",
])
def test_parse_int_rejects_anything_not_a_plain_integer_literal(raw):
    # "42.0"/"1e3" look numeric but int() doesn't parse decimals/exponents
    # directly - a real trap for anyone assuming parse_int is as permissive
    # as float-then-int coercion (see to_int's docstring-equivalent test in
    # test_seed_catalog.py, which explicitly is that permissive).
    value, ok = parse_int(raw)
    assert ok is False
    assert value is None


def test_parse_int_accepts_non_ascii_unicode_decimal_digits():
    # int() recognizes Unicode decimal digits beyond ASCII 0-9 (e.g.
    # Arabic-Indic) - genuinely surprising if you assume this function only
    # accepts what a <input type="number"> would send, worth pinning
    # explicitly rather than leaving as an unverified assumption.
    assert parse_int("٤٢") == (42, True)


def test_parse_int_accepts_underscore_digit_separators():
    # int("1_000") == 1000 is real Python behavior (PEP 515 grouping
    # underscores apply to str->int parsing, not just source-code literals)
    # - a release_year of "2_020" would silently parse as 2020 rather than
    # being rejected as garbage. Confirmed against the interpreter before
    # writing this, not assumed.
    assert parse_int("1_000") == (1000, True)


# ------------------------------------------------------------ parse_float --

@pytest.mark.parametrize("raw,expected", [("4.5", 4.5), ("-2", -2.0), (" 3.14 ", 3.14), ("1e3", 1000.0)])
def test_parse_float_accepts_well_formed_numbers(raw, expected):
    assert parse_float(raw) == (expected, True)


@pytest.mark.parametrize("raw", [None, "", "  "])
def test_parse_float_blank_is_none_but_not_an_error(raw):
    assert parse_float(raw) == (None, True)


def test_parse_float_rejects_actual_garbage():
    value, ok = parse_float("not-a-number")
    assert ok is False
    assert value is None


@pytest.mark.parametrize("raw", ["nan", "NaN", "inf", "-inf", "Infinity"])
def test_parse_float_currently_accepts_nan_and_infinity_as_valid(raw):
    # KNOWN GAP, not a deliberate feature: Python's float() parses these as
    # legitimate special float values, so e.g. an imdb_score or
    # monthly_spend_hours field of "nan"/"inf" sails through as (value, True)
    # with no validation error, and would be written to Postgres as-is
    # (float8 supports 'NaN'::float8, so it wouldn't even fail at the DB
    # layer). Pinned here so this is a visible, intentional test rather than
    # a silent surprise - flagged to the user as worth a real range/finite
    # check if it matters for data quality.
    value, ok = parse_float(raw)
    assert ok is True
    assert value != value or value in (float("inf"), float("-inf"))  # nan != nan is True


# ------------------------------------------------------------- sort_params --

def test_sort_params_valid_column_and_direction():
    with app_module.app.test_request_context("/?sort=title&dir=desc"):
        key, sql_expr, direction = sort_params(ITEMS_SORT_COLUMNS, default="item_id")
    assert (key, sql_expr, direction) == ("title", ITEMS_SORT_COLUMNS["title"], "desc")


def test_sort_params_unknown_column_falls_back_to_default_not_the_first_column():
    with app_module.app.test_request_context("/?sort='; DROP TABLE item; --"):
        key, _, direction = sort_params(ITEMS_SORT_COLUMNS, default="item_id")
    # The whole point of the whitelist: nothing resembling injected SQL ever
    # reaches the sort_columns[...] lookup as a live key.
    assert key == "item_id"
    assert direction == "asc"


def test_sort_params_invalid_direction_falls_back_to_asc_without_touching_the_column():
    with app_module.app.test_request_context("/?sort=title&dir=DESC;DROP"):
        key, _, direction = sort_params(ITEMS_SORT_COLUMNS, default="item_id")
    assert key == "title"  # a bad `dir` shouldn't also reset a valid `sort`
    assert direction == "asc"


def test_sort_params_no_query_args_uses_defaults():
    with app_module.app.test_request_context("/"):
        key, _, direction = sort_params(ITEMS_SORT_COLUMNS, default="item_id")
    assert (key, direction) == ("item_id", "asc")


def test_sort_params_raises_if_default_itself_is_not_a_whitelisted_column():
    # Fragility worth pinning even though today's two call sites (app.py:323,
    # 830) both pass a valid default: sort_columns[sort_key] is a bare dict
    # lookup with no .get()/fallback, so a bad `default` argument - not a bad
    # query string - would 500 the page instead of degrading gracefully.
    with app_module.app.test_request_context("/"):
        with pytest.raises(KeyError):
            sort_params(ITEMS_SORT_COLUMNS, default="not_a_real_column")


# ----------------------------------------------------------- read_item_form --

VALID_GENRES = {"drama", "comedy"}
VALID_COUNTRIES = {"US", "DE"}
VALID_LANGUAGES = {"en", "de"}


def _item_form(**overrides):
    form = {
        "type": "movie",
        "title": "Test Movie",
        "description": "",
        "release_year": "2020",
        "runtime_minutes": "120",
        "season_count": "",
        "episode_count": "",
        "date_added": "",
        "content_rating": "",
        "country": "us",
        "original_language": "EN",
        "imdb_score": "7.5",
        "genre_primary": "drama",
        "genre_secondary": "",
        "catalog_status": "active",
    }
    form.update(overrides)
    return form


def test_read_item_form_valid_input_normalizes_case_and_types():
    data, errors = read_item_form(_item_form(), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["release_year"] == 2020  # str -> int
    assert data["country"] == "US"  # uppercased
    assert data["original_language"] == "en"  # lowercased
    assert data["imdb_score"] == 7.5  # str -> float


def test_read_item_form_missing_title_is_rejected_even_when_whitespace_only():
    _, errors = read_item_form(_item_form(title="   "), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Title is required" in e for e in errors)


@pytest.mark.parametrize("bad_type", ["", "documentary", "Movie", "movieseries"])
def test_read_item_form_rejects_anything_not_exactly_movie_or_series(bad_type):
    _, errors = read_item_form(_item_form(type=bad_type), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Type must be movie or series" in e for e in errors)


def test_read_item_form_strips_surrounding_whitespace_from_type_before_checking():
    # data["type"] is built from form.get("type", "").strip() - " movie"
    # must be accepted (stripped to "movie"), not rejected as unknown.
    data, errors = read_item_form(_item_form(type=" movie "), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["type"] == "movie"


def test_read_item_form_accumulates_every_error_not_just_the_first():
    # Multiple independent violations at once - confirms errors is a list
    # that keeps collecting, not a fail-fast that hides later problems from
    # the admin filling out the form.
    _, errors = read_item_form(
        _item_form(type="bad", title="", release_year="not-a-year", country="zz", genre_primary="sci-fi"),
        VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES,
    )
    assert len(errors) >= 5


def test_read_item_form_non_numeric_release_year_names_the_field_in_the_error():
    _, errors = read_item_form(_item_form(release_year="not-a-year"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Release Year must be a whole number" in e for e in errors)


def test_read_item_form_decimal_looking_runtime_is_rejected_not_truncated():
    # A tempting-but-wrong fix would silently truncate "120.5" to 120 -
    # parse_int must reject it outright instead, same as any other garbage.
    data, errors = read_item_form(_item_form(runtime_minutes="120.5"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Runtime Minutes must be a whole number" in e for e in errors)
    assert data["runtime_minutes"] is None


def test_read_item_form_unknown_country_is_rejected_case_insensitively_checked_uppercase():
    # valid_countries holds uppercase codes; the function uppercases the
    # input before checking - "us" (lowercase in the form) must still pass
    # since VALID_COUNTRIES contains "US", not fail as if it were case-sensitive.
    data, errors = read_item_form(_item_form(country="us"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["country"] == "US"


def test_read_item_form_unknown_country_after_normalization_is_an_error():
    _, errors = read_item_form(_item_form(country="zz"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid country" in e for e in errors)


def test_read_item_form_unknown_primary_genre_is_an_error():
    _, errors = read_item_form(_item_form(genre_primary="sci-fi"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid primary genre" in e for e in errors)


def test_read_item_form_unknown_secondary_genre_is_an_error():
    _, errors = read_item_form(_item_form(genre_secondary="sci-fi"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid secondary genre" in e for e in errors)


def test_read_item_form_does_not_require_primary_and_secondary_genres_to_differ():
    # Documents current behavior, not a recommendation: genre_primary ==
    # genre_secondary passes with zero errors. Might be intentional
    # (secondary is optional/free), might be an overlooked check - flagged,
    # not silently assumed correct.
    data, errors = read_item_form(
        _item_form(genre_primary="drama", genre_secondary="drama"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES,
    )
    assert errors == []
    assert data["genre_primary"] == data["genre_secondary"] == "drama"


@pytest.mark.parametrize("bad_status", ["", "deleted", "Active"])
def test_read_item_form_rejects_unknown_catalog_status(bad_status):
    _, errors = read_item_form(_item_form(catalog_status=bad_status), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid catalog status" in e for e in errors)


def test_read_item_form_catalog_status_defaults_to_active_when_field_absent():
    form = _item_form()
    del form["catalog_status"]
    data, errors = read_item_form(form, VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["catalog_status"] == "active"


def test_read_item_form_checkbox_only_true_for_the_literal_string_on():
    # HTML checkboxes send "on" when checked and are simply absent when
    # unchecked - "true"/"1"/"yes" must NOT be treated as checked, since a
    # differently-behaved client sending those would otherwise silently
    # flip the flag the wrong way.
    for not_on in ("true", "1", "yes", "On", "ON"):
        data, _ = read_item_form(
            _item_form(is_netflix_original=not_on), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES,
        )
        assert data["is_netflix_original"] is False, f"{not_on!r} should not count as checked"

    data, _ = read_item_form(_item_form(is_netflix_original="on"), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert data["is_netflix_original"] is True


def test_read_item_form_blank_optional_fields_become_none_not_empty_string():
    data, errors = read_item_form(_item_form(description="  "), VALID_GENRES, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["description"] is None  # not ""


# ----------------------------------------------------------- read_user_form --

def _user_form(**overrides):
    form = {
        "age": "30",
        "gender": "female",
        "occupation": "programmer",
        "country": "de",
        "preferred_language": "DE",
        "signup_date": "2024-01-01",
        "subscription_plan": "premium",
        "email": "a@b.com",
        "first_name": "A",
        "last_name": "B",
        "state_province": "",
        "city": "",
        "primary_device": "Mobile",
        "monthly_spend_hours": "12.5",
        "account_status": "active",
    }
    form.update(overrides)
    return form


def test_read_user_form_valid_input_normalizes_case_and_types():
    data, errors = read_user_form(_user_form(), VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["age"] == 30
    assert data["country"] == "DE"
    assert data["preferred_language"] == "de"


@pytest.mark.parametrize("bad_age", ["old", "30.5", "twenty"])
def test_read_user_form_rejects_non_integer_age(bad_age):
    _, errors = read_user_form(_user_form(age=bad_age), VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Age must be a whole number" in e for e in errors)


@pytest.mark.parametrize("weird_age", ["-5", "0", "999"])
def test_read_user_form_does_not_range_check_age(weird_age):
    # Documents a real gap: parse_int only checks "is this an integer?", not
    # "is this a plausible age?" - a negative or absurdly large age is
    # accepted with zero errors. Flagged as a missing-validation candidate,
    # not fixed here since the sane range is a product decision, not implied
    # by the existing code.
    data, errors = read_user_form(_user_form(age=weird_age), VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["age"] == int(weird_age)


@pytest.mark.parametrize("bad_plan", ["gold", "", "Premium"])
def test_read_user_form_rejects_unknown_subscription_plan(bad_plan):
    _, errors = read_user_form(_user_form(subscription_plan=bad_plan), VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid subscription plan" in e for e in errors)


def test_read_user_form_subscription_plan_defaults_to_basic_when_absent():
    form = _user_form()
    del form["subscription_plan"]
    data, errors = read_user_form(form, VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["subscription_plan"] == "basic"


@pytest.mark.parametrize("bad_status", ["deleted", "", "Active"])
def test_read_user_form_rejects_unknown_account_status(bad_status):
    _, errors = read_user_form(_user_form(account_status=bad_status), VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid account status" in e for e in errors)


def test_read_user_form_unknown_preferred_language_is_an_error():
    _, errors = read_user_form(_user_form(preferred_language="zz"), VALID_COUNTRIES, VALID_LANGUAGES)
    assert any("Invalid preferred language" in e for e in errors)


def test_read_user_form_blank_optional_fields_become_none():
    data, errors = read_user_form(_user_form(state_province="  ", city=""), VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["state_province"] is None
    assert data["city"] is None


def test_read_user_form_email_accepts_anything_nonblank_no_format_check():
    # No email format validation exists at all - "not-an-email" passes.
    # Documented, not asserted as correct: flagged as a possible gap for an
    # admin-facing form where a typo'd email would otherwise go unnoticed.
    data, errors = read_user_form(_user_form(email="not-an-email"), VALID_COUNTRIES, VALID_LANGUAGES)
    assert errors == []
    assert data["email"] == "not-an-email"
