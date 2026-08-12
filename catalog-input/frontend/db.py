import os

import psycopg2
import psycopg2.extras


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "catalog-db"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "catalog"),
        user=os.environ.get("PGUSER", "catalog"),
        password=os.environ.get("PGPASSWORD", "catalog"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
