"""Shared fixtures for catalog-input's route-level integration tests
(tests/test_app_routes.py). These need the real docker-compose stack
running: catalog-db, reachable on its host-mapped localhost port (see
../../.env for CATALOG_DB_PORT). Run `docker compose up -d catalog-db`
from the repo root first.

Unlike test_app_forms.py/test_seed_catalog.py (pure functions, no DB),
these drive app.py's actual Flask routes end to end via app.test_client()
against a real Postgres - the same "integration means real infra"
convention reporting-output's tests use (see
../../reporting-output/tests/conftest.py's docstring), not client-input's
"real threads/timing, but still-faked infra" one.

The env vars below are set *before* `import app` (module level, at
collection time): app.py reads FRONTEND_ADMIN_USERNAME/PASSWORD/
FRONTEND_SECRET_KEY into module globals once at import time, so a
monkeypatch fixture (which only takes effect once a test actually runs)
would be too late to affect them. setdefault() so a developer's own shell
env can still override any of these; PGHOST/PGPORT default to localhost
here specifically because db.get_conn()'s own built-in default
("catalog-db") is the in-Docker service hostname, unreachable from a
host-run pytest. FRONTEND_ADMIN_USERNAME/PASSWORD are pinned to a
test-only value rather than left at app.py's own "admin"/"changeme"
default so a login test never silently depends on whatever real
credentials happen to be in the repo's own .env.
"""
import os
import uuid

os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", os.environ.get("CATALOG_DB_PORT", "5432"))
os.environ.setdefault("PGDATABASE", os.environ.get("CATALOG_DB_NAME", "catalog"))
os.environ.setdefault("PGUSER", os.environ.get("CATALOG_DB_USER", "catalog"))
os.environ.setdefault("PGPASSWORD", os.environ.get("CATALOG_DB_PASSWORD", "catalog"))
os.environ.setdefault("FRONTEND_ADMIN_USERNAME", "test-admin")
os.environ.setdefault("FRONTEND_ADMIN_PASSWORD", "test-password")
os.environ.setdefault("FRONTEND_SECRET_KEY", "test-only-secret-key")

import psycopg2
import psycopg2.extras
import pytest

import app as app_module


@pytest.fixture(scope="session", autouse=True)
def require_live_stack():
    """Fail fast with one clear message instead of every test timing out
    separately on a connection nothing's listening on."""
    try:
        conn = psycopg2.connect(
            host=os.environ["PGHOST"], port=os.environ["PGPORT"],
            dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"], connect_timeout=5,
        )
        conn.close()
    except Exception as exc:
        pytest.exit(
            f"catalog-db not reachable at {os.environ['PGHOST']}:{os.environ['PGPORT']} - "
            f"these are integration tests and need the real stack. Run "
            f"`docker compose up -d catalog-db` from the repo root first. ({exc})",
            returncode=1,
        )


@pytest.fixture
def client():
    """A Flask test client, CSRF disabled - route wiring/persistence is
    what's under test here, not the CSRF token round-trip itself."""
    app_module.app.config["TESTING"] = True
    app_module.app.config["WTF_CSRF_ENABLED"] = False
    return app_module.app.test_client()


@pytest.fixture
def logged_in_client(client):
    """Bypasses the /login form (already covered by its own test) by
    setting the session cookie login_required actually checks."""
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    return client


@pytest.fixture
def db_conn():
    """A connection independent of whatever the route handler itself opens
    per-request via db.get_conn() - for tests to seed rows before a
    request and assert on the DB's contents after one."""
    conn = psycopg2.connect(
        host=os.environ["PGHOST"], port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"], cursor_factory=psycopg2.extras.RealDictCursor,
    )
    conn.autocommit = True
    yield conn
    conn.close()


def unique_marker():
    """This is a real, shared catalog-db (the same one a developer might
    be manually poking at) - every test that writes must clean up exactly
    what it created, and needs an unambiguous way to find it again. A
    fresh marker per test is that handle, folded into whatever free-text
    field (title, city, ...) the row being created has - never into an
    item_id/user_id (app-generated, ^i[0-9]+$/^u[0-9]+$-constrained - see
    postgres/init/01_schema.sql) or a genre/country/language code (too
    short/constrained to embed a marker in - those tests use a fixed,
    obviously-fake value like "ZZ" instead, cleaned up by that exact value)."""
    return f"zz-test-{uuid.uuid4().hex[:10]}"
