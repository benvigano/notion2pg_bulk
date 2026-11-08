"""Target database precondition: the snapshot only writes into an empty database."""

from __future__ import annotations

from typing import List

from sqlalchemy import Connection, text

from .errors import TargetNotEmptyError

_EXTRA_SCHEMAS = r"""
    SELECT 'schema ' || nspname
    FROM pg_namespace
    WHERE nspname NOT IN ('public', 'pg_catalog', 'information_schema')
      AND nspname NOT LIKE 'pg\_toast%'
      AND nspname NOT LIKE 'pg\_temp%'
"""

_PUBLIC_RELATIONS = """
    SELECT 'relation public.' || c.relname
    FROM pg_class c
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
"""

_PUBLIC_FUNCTIONS = """
    SELECT 'function public.' || p.proname
    FROM pg_proc p
    WHERE p.pronamespace = 'public'::regnamespace
"""

_PUBLIC_TYPES = """
    SELECT 'type public.' || t.typname
    FROM pg_type t
    LEFT JOIN pg_class c ON c.oid = t.typrelid
    WHERE t.typnamespace = 'public'::regnamespace
      AND (t.typrelid = 0 OR c.relkind = 'c')
      AND t.typcategory <> 'A'
"""

_FOREIGN_OBJECTS = text(
    "SELECT description FROM ("
    + " UNION ALL ".join(f"({query})" for query in (_EXTRA_SCHEMAS, _PUBLIC_RELATIONS, _PUBLIC_FUNCTIONS, _PUBLIC_TYPES))
    + ") AS found (description) ORDER BY description"
)


def assert_target_empty(conn: Connection) -> None:
    """Check that the target holds no user objects.

    Ignored: the ``public`` schema itself, system schemas, and default
    extensions (``plpgsql`` lives in ``pg_catalog``).

    :param conn: Connection inside the snapshot transaction.
    :raises TargetNotEmptyError: A schema other than ``public`` exists, or
        ``public`` holds a relation, function or user-defined type.
    """
    found: List[str] = list(conn.execute(_FOREIGN_OBJECTS).scalars())
    if found:
        raise TargetNotEmptyError(found)
