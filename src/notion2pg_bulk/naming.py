"""Verbatim naming of row tables and columns, and the workspace checks it requires."""

from __future__ import annotations

from collections import Counter
from typing import Iterable, List

from .errors import SnapshotIntegrityError
from .records import DataSourceRecord
from .schema import ROW_ID_COLUMN, property_columns

MAX_IDENTIFIER_BYTES = 63
RESERVED_COLUMN_NAMES = frozenset({"tableoid", "xmin", "xmax", "cmin", "cmax", "ctid", ROW_ID_COLUMN})


def _identifier_problem(name: str) -> str | None:
    if not name:
        return "is empty"
    if "\x00" in name:
        return "contains a NUL character"
    size = len(name.encode("utf-8"))
    if size > MAX_IDENTIFIER_BYTES:
        return f"is {size} bytes of UTF-8 (max {MAX_IDENTIFIER_BYTES})"
    return None


def column_problems(data_source: DataSourceRecord) -> List[str]:
    """:returns: Violations of the column naming rules in one data source."""
    problems = []
    for prop, _ in property_columns(data_source):
        issue = _identifier_problem(prop.name)
        if issue is not None:
            problems.append(f"property {prop.name!r} ({prop.property_id}) of data source {data_source.data_source_id} {issue}")
        if prop.name in RESERVED_COLUMN_NAMES:
            problems.append(
                f"property {prop.name!r} ({prop.property_id}) of data source {data_source.data_source_id} "
                "uses a reserved column name"
            )
    return problems


def check_workspace_names(data_sources: Iterable[DataSourceRecord]) -> None:
    """Check that every data source and property can be named verbatim.

    :param data_sources: All data sources of the snapshot.
    :raises SnapshotIntegrityError: A title is empty, duplicated or too long,
        or a column-bearing property name is too long or reserved.
    """
    sources = list(data_sources)
    problems: List[str] = []
    for source in sources:
        issue = _identifier_problem(source.table_name)
        if issue is not None:
            problems.append(f"title {source.table_name!r} of data source {source.data_source_id} {issue}")
        problems.extend(column_problems(source))
    counts = Counter(source.table_name for source in sources)
    for title, count in sorted(counts.items()):
        if count > 1 and title:
            ids = ", ".join(s.data_source_id for s in sources if s.table_name == title)
            problems.append(f"title {title!r} is shared by {count} data sources ({ids})")
    if problems:
        raise SnapshotIntegrityError("Workspace naming requirements not met: " + "; ".join(problems))
