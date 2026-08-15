"""seed_catalog.py's pure parsing/classification helpers. Deliberately
adversarial on the CSV-cell-shaped inputs these actually see in practice
(soeiro/MovieLens/Netflix-pack files) rather than just well-formed cases."""
import pytest

import seed_catalog
from seed_catalog import categorize_seed, parse_list_field, to_bool, to_float, to_int


# ----------------------------------------------------------------- to_int --

@pytest.mark.parametrize("raw,expected", [("42", 42), ("  7  ", 7), ("-3", -3)])
def test_to_int_accepts_plain_integers(raw, expected):
    assert to_int(raw) == expected


@pytest.mark.parametrize("raw,expected", [("42.0", 42), ("42.9", 42), ("-3.9", -3)])
def test_to_int_truncates_decimal_strings_toward_zero(raw, expected):
    # Deliberately more permissive than app.py's parse_int (see
    # test_app_forms.py): goes through float() first, so "42.9" is accepted
    # and *truncated*, not rejected. Real behavioral split between the two
    # "same purpose, different file" functions - pinned so it doesn't
    # silently drift, not because either behavior is obviously "more right".
    assert to_int(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_to_int_blank_is_none(raw):
    assert to_int(raw) is None


def test_to_int_garbage_returns_none():
    assert to_int("not-a-number") is None


@pytest.mark.parametrize("raw", ["inf", "-inf", "Infinity", "-Infinity"])
def test_to_int_infinity_returns_none_instead_of_crashing(raw):
    # Regression test for a real bug: float(raw) parses "inf"/"-inf" fine,
    # but int(float(raw)) then raises OverflowError, not ValueError - a bare
    # `except ValueError` let it crash the seeding run on a malformed
    # numeric CSV cell. Fixed in seed_catalog.py's to_int to also catch
    # OverflowError.
    assert to_int(raw) is None


def test_to_int_nan_returns_none():
    # int(float('nan')) raises ValueError (unlike the inf case above, which
    # raises OverflowError) - already covered by the existing except clause,
    # confirmed here so the two float-edge-cases aren't conflated.
    assert to_int("nan") is None


# --------------------------------------------------------------- to_float --

@pytest.mark.parametrize("raw,expected", [("4.5", 4.5), ("-2", -2.0), (" 3.14 ", 3.14)])
def test_to_float_accepts_well_formed_numbers(raw, expected):
    assert to_float(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "  "])
def test_to_float_blank_is_none(raw):
    assert to_float(raw) is None


def test_to_float_garbage_returns_none():
    assert to_float("nope") is None


def test_to_float_currently_accepts_infinity_as_valid():
    # Same known gap as parse_float in app.py (see test_app_forms.py):
    # float() parses these as legitimate special values, so a corrupted
    # numeric CSV cell reading "inf" silently becomes a "valid" imdb_score/
    # runtime rather than getting skipped as bad data. Flagged, not fixed
    # here - deciding whether to reject non-finite values is a data-quality
    # call, not an unambiguous bug the way to_int's OverflowError crash was.
    assert to_float("inf") == float("inf")
    assert to_float("-inf") == float("-inf")


def test_to_float_currently_accepts_nan_as_valid():
    value = to_float("nan")
    assert value != value  # the only self-consistent way to check for NaN


# ---------------------------------------------------------------- to_bool --

@pytest.mark.parametrize("raw", ["true", "True", "TRUE", " true ", "tRuE", "true "])
def test_to_bool_true_values_are_case_and_whitespace_insensitive(raw):
    assert to_bool(raw) is True


@pytest.mark.parametrize("raw", ["false", "False", "", "0", "1", "yes", "no", None, "   "])
def test_to_bool_everything_else_is_false(raw):
    # "1"/"yes" are common "truthy" spellings in other CSV dialects but this
    # function recognizes exactly one literal spelling - worth confirming
    # nothing else sneaks through as True.
    assert to_bool(raw) is False


# --------------------------------------------------------- parse_list_field --

def test_parse_list_field_parses_a_python_literal_list_of_strings():
    assert parse_list_field("['drama', 'crime']") == ["drama", "crime"]


def test_parse_list_field_parses_a_python_literal_tuple_too():
    assert parse_list_field("('drama', 'crime')") == ["drama", "crime"]


@pytest.mark.parametrize("raw", ["", None])
def test_parse_list_field_empty_or_none_is_an_empty_list(raw):
    assert parse_list_field(raw) == []


def test_parse_list_field_whitespace_only_is_also_an_empty_list():
    # "  " is truthy (`if not raw` doesn't catch it), so this exercises the
    # ast.literal_eval-fails -> comma-split fallback path, not the early
    # return - different code path, same correct empty-list result.
    assert parse_list_field("   ") == []


def test_parse_list_field_falls_back_to_comma_split_on_bare_words():
    # "drama" alone isn't a valid Python literal (it parses as a Name node,
    # which ast.literal_eval explicitly rejects) - must still fall back
    # cleanly rather than raise.
    assert parse_list_field("drama, crime") == ["drama", "crime"]


def test_parse_list_field_fallback_strips_stray_quotes_and_brackets():
    # Malformed/half-bracketed input (unquoted words inside brackets isn't
    # valid Python either) - the fallback's strip(" '\"[]") must still
    # produce clean tokens, not "[drama]" verbatim.
    assert parse_list_field("[drama], [crime]") == ["drama", "crime"]


def test_parse_list_field_single_quoted_string_literal_is_not_treated_as_a_list():
    # ast.literal_eval("'hello'") succeeds (it's a valid literal) but
    # produces a plain str, not a list/tuple - the isinstance guard must
    # reject it and fall through to the comma-split fallback rather than
    # e.g. iterating over the string's characters.
    assert parse_list_field("'hello'") == ["hello"]


def test_parse_list_field_literal_list_of_numbers_gets_stringified():
    # A non-string-list literal (e.g. "[1, 2, 3]") still hits the
    # isinstance(list) branch - each element gets str()'d, not rejected.
    assert parse_list_field("[1, 2, 3]") == ["1", "2", "3"]


def test_parse_list_field_drops_blank_entries_from_a_literal_list():
    assert parse_list_field("['drama', '', '  ', 'crime']") == ["drama", "crime"]


# ------------------------------------------------------------ categorize_seed --

def test_categorize_seed_general_when_both_items_and_users_files_present(tmp_path, monkeypatch):
    seed_dir = tmp_path / "mypack"
    seed_dir.mkdir()
    (seed_dir / "titles.csv").write_text("")
    (seed_dir / "users.dat").write_text("")
    monkeypatch.setattr(seed_catalog, "SEEDS_DIR", str(tmp_path))
    assert categorize_seed("mypack") == "general"


@pytest.mark.parametrize("items_file,users_file", [
    ("titles.csv", "users.dat"), ("titles.csv", "users.csv"),
    ("movies.csv", "users.dat"), ("movies.csv", "users.csv"),
])
def test_categorize_seed_recognizes_both_items_and_both_users_filenames(tmp_path, monkeypatch, items_file, users_file):
    seed_dir = tmp_path / "pack"
    seed_dir.mkdir()
    (seed_dir / items_file).write_text("")
    (seed_dir / users_file).write_text("")
    monkeypatch.setattr(seed_catalog, "SEEDS_DIR", str(tmp_path))
    assert categorize_seed("pack") == "general"


def test_categorize_seed_items_only(tmp_path, monkeypatch):
    seed_dir = tmp_path / "itemsonly"
    seed_dir.mkdir()
    (seed_dir / "movies.csv").write_text("")
    monkeypatch.setattr(seed_catalog, "SEEDS_DIR", str(tmp_path))
    assert categorize_seed("itemsonly") == "items"


def test_categorize_seed_users_only(tmp_path, monkeypatch):
    seed_dir = tmp_path / "usersonly"
    seed_dir.mkdir()
    (seed_dir / "users.csv").write_text("")
    monkeypatch.setattr(seed_catalog, "SEEDS_DIR", str(tmp_path))
    assert categorize_seed("usersonly") == "users"


def test_categorize_seed_directory_with_neither_file_is_misclassified_as_users(tmp_path, monkeypatch):
    # KNOWN QUIRK, flagged not fixed: has_items and has_users are both
    # False for an empty/unrecognized directory, and
    # `"items" if has_items else "users"` silently falls to "users" rather
    # than something like "empty"/"unknown". A seed pack folder with only
    # e.g. a README or in-progress files would show up mislabeled in the
    # Seed Data page's "Users" section instead of being excluded.
    seed_dir = tmp_path / "empty_or_wip_pack"
    seed_dir.mkdir()
    (seed_dir / "README.md").write_text("not a seed file")
    monkeypatch.setattr(seed_catalog, "SEEDS_DIR", str(tmp_path))
    assert categorize_seed("empty_or_wip_pack") == "users"


def test_categorize_seed_real_netflix_full_pack_on_disk():
    # No monkeypatching - exercises the actual checked-in seeds/netflix_full/
    # (confirmed on disk to contain both movies.csv and users.csv), catching
    # drift if that pack's files are ever renamed without updating this.
    assert categorize_seed("netflix_full") == "general"


def test_categorize_seed_nonexistent_pack_name_is_also_users_not_an_error():
    # Same quirk as the empty-directory case above, reached via a directory
    # that doesn't exist at all - os.path.isfile on a missing path is just
    # False, not an error, so this silently returns "users" too.
    assert categorize_seed("this-pack-does-not-exist") == "users"
