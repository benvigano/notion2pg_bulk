"""Verbatim naming: workspace checks run before any write."""

from __future__ import annotations

import pytest

from fake_notion import data_source, new_id
from notion2pg_bulk.errors import SnapshotIntegrityError
from notion2pg_bulk.naming import check_workspace_names
from notion2pg_bulk.records import parse_data_source
from notion2pg_bulk.schema import row_table

TITLE = {"id": "title", "name": "Name", "description": None, "type": "title", "title": {}}


def source(title, **props):
    properties = {"Name": TITLE, **{name: {"id": new_id()[:4], "name": name, "description": None, **cfg}
                                    for name, cfg in props.items()}}
    return parse_data_source(data_source(new_id(), new_id(), title, properties))


def test_valid_names_pass_and_are_verbatim():
    ds = source("Tasks & Bugs (2026)", **{"Due date": {"type": "date", "date": {}},
                                          "Due-date": {"type": "date", "date": {}},
                                          "due date": {"type": "date", "date": {}}})
    check_workspace_names([ds, source("tasks & bugs (2026)")])
    table = row_table(ds)
    assert table.name == "Tasks & Bugs (2026)"
    assert [c.name for c in table.columns] == ["_page_id", "Name", "Due date", "Due-date", "due date"]


@pytest.mark.parametrize("sources", [
    lambda: [source("")],
    lambda: [source("Same"), source("Same")],
    lambda: [source("x" * 64)],
    lambda: [source("é" * 32)],
    lambda: [source("T", **{"y" * 64: {"type": "number", "number": {}}})],
    lambda: [source("T", ctid={"type": "number", "number": {}})],
    lambda: [source("T", _page_id={"type": "number", "number": {}})],
    lambda: [source("T", xmin={"type": "checkbox", "checkbox": {}})],
])
def test_violations_raise(sources):
    with pytest.raises(SnapshotIntegrityError):
        check_workspace_names(sources())


def test_limits_are_bytes_not_characters():
    check_workspace_names([source("x" * 63), source("é" * 31 + "x")])


def test_properties_without_column_are_not_named():
    check_workspace_names([source("T", ctid={"type": "formula", "formula": {"expression": "1"}})])
