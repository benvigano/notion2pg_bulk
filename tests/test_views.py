"""View listing and attribution."""

from __future__ import annotations

from fake_notion import data_source, database, new_id, view
from helpers import run_snapshot
from test_rows import TITLE
from test_snapshot import query
from workspace import build_workspace


def test_views_are_attributed_to_their_own_data_source(database_url, clock):
    ws = build_workspace()
    second_ds = new_id()
    ws.fake.databases[ws.projects_db]["data_sources"].append({"id": second_ds, "name": "Archive"})
    ws.fake.add_data_source(data_source(second_ds, ws.projects_db, "Archive", {"Name": TITLE}))
    archive_view = new_id()
    ws.fake.add_view(view(archive_view, ws.projects_db, second_ds, "board"))
    run_snapshot(ws.fake, database_url, clock)
    rows = {r[0]: (r[1], r[2]) for r in query(database_url, "SELECT view_id::text, database_id::text, data_source_id::text FROM notion.views")}
    assert rows == {ws.table_view: (ws.projects_db, ws.projects_ds), archive_view: (ws.projects_db, second_ds)}
    assert ws.fake.count("GET", f"views/{ws.table_view}") == 1
