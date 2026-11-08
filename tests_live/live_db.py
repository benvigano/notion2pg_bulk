"""PostgreSQL helpers for the live tests: fresh databases and queries on snapshot results."""

from __future__ import annotations

import uuid
from typing import Any, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from notion2pg_bulk.orchestrate import create_target_engine


def create_database(admin_url: str) -> str:
    name = f"n2pg_live_{uuid.uuid4().hex[:10]}"
    admin = create_engine(make_url(admin_url).set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    admin.dispose()
    return make_url(admin_url).set(database=name).render_as_string(hide_password=False)


def drop_database(admin_url: str, url: str) -> None:
    admin = create_engine(make_url(admin_url).set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{make_url(url).database}" WITH (FORCE)'))
    admin.dispose()


def pg(url: str, sql: str, **params: Any) -> List[tuple]:
    """Run a query on a snapshot database; UUIDs come back as strings."""
    engine = create_target_engine(url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
    finally:
        engine.dispose()
    return [tuple(_plain(value) for value in row) for row in rows]


def _plain(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def dump(url: str) -> dict:
    """Every row of every table in the ``notion`` and ``rows`` schemas, sorted."""
    tables = pg(url, "SELECT table_schema, table_name FROM information_schema.tables "
                     "WHERE table_schema IN ('notion', 'rows') ORDER BY 1, 2")
    out = {}
    for schema, table in tables:
        quoted = '"' + table.replace('"', '""') + '"'
        out[f"{schema}.{table}"] = sorted(repr(row) for row in pg(url, f"SELECT * FROM {schema}.{quoted}"))
    return out
