"""Determinism: responses carry a per-response request_id, which must not reach the snapshot."""

from __future__ import annotations

from fake_notion import new_id, paragraph
from helpers import dump_database, run_snapshot
from notion2pg_bulk.records import parse_view
from workspace import build_workspace


def test_view_content_excludes_the_response_request_id():
    view = {"object": "view", "id": new_id(), "parent": {"type": "database_id", "database_id": new_id()},
            "data_source_id": None, "name": "v", "type": "table", "filter": None, "request_id": new_id()}
    assert "request_id" not in parse_view(view).content


def test_two_snapshots_of_an_unchanged_workspace_are_identical(database_url, second_database_url, clock):
    ws = build_workspace()
    ws.fake.children[ws.standalone_page].append(paragraph("more"))
    run_snapshot(ws.fake, database_url, clock)
    run_snapshot(ws.fake, second_database_url, clock)
    first, second = dump_database(database_url), dump_database(second_database_url)
    assert first == second and sum(len(rows) for rows in first.values()) > 20
