"""Trashed objects returned by the API (checked against the live API: GET returns them with in_trash)."""

from __future__ import annotations

import httpx
import pytest

from fake_notion import block, data_source, database, new_id, page, page_mention, title_value
from helpers import run_snapshot
from notion2pg_bulk.errors import SnapshotIntegrityError
from test_discovery import WORKSPACE, captured_pages, tables
from test_rows import TITLE
from test_snapshot import query
from workspace import build_workspace


def trashed_source(ws, title="Trashed"):
    db_id, ds_id = new_id(), new_id()
    ws.fake.add_database(database(db_id, f"{title} DB", [ds_id], parent=WORKSPACE, in_trash=True))
    ws.fake.add_data_source(data_source(ds_id, db_id, title, {"Name": TITLE}, in_trash=True), searchable=False)
    return db_id, ds_id


def stopped(events, reason):
    return [e for e in events.events if e["event"] == "discovery_stopped" and e["reason"] == reason]


def test_relation_to_a_trashed_data_source_ends_the_path(database_url, clock, telemetry_events):
    ws = build_workspace()
    _, gone_ds = trashed_source(ws)
    ws.fake.data_sources[ws.tasks_ds]["properties"]["Gone"] = {
        "id": "gone", "name": "Gone", "description": None, "type": "relation",
        "relation": {"data_source_id": gone_ds, "database_id": new_id(), "type": "single_property", "single_property": {}}}
    ws.fake.faults.append(lambda r: httpx.Response(404, json={"object": "error", "status": 404, "code": "object_not_found",
                                                              "message": "gone", "request_id": "r"})
                          if r.url.path == f"/v1/data_sources/{gone_ds}/query" else None)
    run_snapshot(ws.fake, database_url, clock)
    assert "Trashed" not in tables(database_url)
    assert ws.fake.count("POST", f"data_sources/{gone_ds}/query") == 0
    [event] = stopped(telemetry_events, "in_trash")
    assert event["locus"]["endpoint"] == "/v1/data_sources/{data_source_id}" and event["anomaly"]["http_status"] == 200


def test_trashed_data_source_listed_by_a_live_database_is_skipped(database_url, clock):
    ws = build_workspace()
    gone_ds = new_id()
    ws.fake.databases[ws.tasks_db]["data_sources"].append({"id": gone_ds, "name": "Gone"})
    ws.fake.add_data_source(data_source(gone_ds, ws.tasks_db, "Gone", {"Name": TITLE}, in_trash=True), searchable=False)
    run_snapshot(ws.fake, database_url, clock)
    assert "Gone" not in tables(database_url)


def test_mentioned_trashed_page_and_database_are_not_recorded(database_url, clock, telemetry_events):
    ws = build_workspace()
    gone_page = new_id()
    ws.fake.add_page(page(gone_page, WORKSPACE, {"title": title_value("Gone")}, in_trash=True), searchable=False)
    gone_db, _ = trashed_source(ws)
    del ws.fake.databases[gone_db]
    ws.fake.children[ws.standalone_page].append(
        block("paragraph", {"rich_text": [page_mention(gone_page), page_mention(gone_db, kind="database")], "color": "default"}))
    ws.fake.children[ws.standalone_page].append(block("link_to_page", {"type": "database_id", "database_id": gone_db}))
    run_snapshot(ws.fake, database_url, clock)
    assert gone_page not in captured_pages(database_url)
    assert query(database_url, "SELECT count(*) FROM notion.databases WHERE database_id = :d", d=gone_db)[0][0] == 0
    assert [e["locus"]["endpoint"] for e in stopped(telemetry_events, "in_trash")] == ["/v1/pages/{page_id}"]
    assert [e["locus"]["endpoint"] for e in stopped(telemetry_events, "not_found")] == ["/v1/views"]


def test_data_source_trashed_during_the_run_raises(database_url, clock):
    ws = build_workspace()
    late = page(new_id(), {"type": "data_source_id", "data_source_id": ws.tasks_ds, "database_id": ws.tasks_db},
                {"Name": title_value("late"), "Extra": {"id": "xtra", "type": "number", "number": 1}},
                created_time="2026-05-01T00:00:00.000Z")
    late["properties"]["Project"] = {"id": "back", "type": "relation", "relation": [], "has_more": False}
    ws.fake.add_row(ws.tasks_ds, late)
    reads = {"n": 0}

    def trash_on_second_read(request):
        if request.method == "GET" and request.url.path == f"/v1/data_sources/{ws.tasks_ds}":
            reads["n"] += 1
            if reads["n"] == 2:
                ws.fake.data_sources[ws.tasks_ds]["in_trash"] = True
        return None

    ws.fake.faults.append(trash_on_second_read)
    with pytest.raises(SnapshotIntegrityError, match="trashed during the snapshot"):
        run_snapshot(ws.fake, database_url, clock)
