"""Discovery beyond search: up, down and sideways, repeated until nothing new is found."""

from __future__ import annotations

import pytest

from fake_notion import (
    block, data_source, database, error_body, new_id, page, page_mention, paragraph, rich_text, title_value, view,
)
from helpers import run_snapshot
from notion2pg_bulk.errors import NotionAPIError
from test_rows import TITLE
from test_snapshot import query
from workspace import build_workspace

WORKSPACE = {"type": "workspace", "workspace": True}


def hidden_page(ws, parent, title="Hidden", body_text="hidden body"):
    page_id = new_id()
    ws.fake.add_page(page(page_id, parent, {"title": title_value(title)}), searchable=False)
    ws.fake.set_children(page_id, [paragraph(body_text)])
    return page_id


def hidden_source(ws, title, row_count=1):
    db_id, ds_id = new_id(), new_id()
    ws.fake.add_database(database(db_id, f"{title} DB", [ds_id], parent=WORKSPACE))
    ws.fake.add_data_source(data_source(ds_id, db_id, title, {"Name": TITLE}), searchable=False)
    for index in range(row_count):
        ws.fake.add_row(ds_id, page(new_id(), {"type": "data_source_id", "data_source_id": ds_id, "database_id": db_id},
                                    {"Name": title_value(f"{title} {index}")}))
    return db_id, ds_id


def captured_pages(database_url):
    return {r[0] for r in query(database_url, "SELECT page_id::text FROM notion.pages")}


def body_count(database_url, page_id):
    return query(database_url, "SELECT count(*) FROM notion.blocks WHERE page_id = :p", p=page_id)[0][0]


def tables(database_url):
    return {r[0] for r in query(database_url, "SELECT table_name FROM notion.catalog_tables")}


# ---------------------------------------------------------------- up

def test_parents_of_pages_are_fetched_up_to_the_workspace(database_url, clock):
    ws = build_workspace()
    grandparent = hidden_page(ws, WORKSPACE, "Grandparent")
    parent = hidden_page(ws, {"type": "page_id", "page_id": grandparent}, "Parent")
    ws.fake.pages[ws.standalone_page]["parent"] = {"type": "page_id", "page_id": parent}
    run_snapshot(ws.fake, database_url, clock)
    assert {grandparent, parent} <= captured_pages(database_url)
    assert body_count(database_url, parent) == 1 and body_count(database_url, grandparent) == 1


def test_block_parents_lead_to_the_containing_page(database_url, clock):
    ws = build_workspace()
    holder = hidden_page(ws, WORKSPACE, "Holder")
    toggle = block("toggle", {"rich_text": [], "color": "default"}, has_children=True,
                   parent={"type": "page_id", "page_id": holder})
    ws.fake.set_children(holder, [toggle])
    ws.fake.pages[ws.standalone_page]["parent"] = {"type": "block_id", "block_id": toggle["id"]}
    ws.fake.set_children(toggle["id"], [block("child_page", {"title": "Handbook"}, block_id=ws.standalone_page)])
    run_snapshot(ws.fake, database_url, clock)
    assert holder in captured_pages(database_url)
    assert ws.fake.count("GET", f"blocks/{toggle['id']}") >= 1


def test_parent_of_a_database_is_fetched(database_url, clock):
    ws = build_workspace()
    home = hidden_page(ws, WORKSPACE, "Home")
    ws.fake.databases[ws.tasks_db]["parent"] = {"type": "page_id", "page_id": home}
    run_snapshot(ws.fake, database_url, clock)
    assert home in captured_pages(database_url) and body_count(database_url, home) == 1


# ---------------------------------------------------------------- sideways

def test_relation_targets_are_followed(database_url, clock):
    ws = build_workspace()
    target_db, target_ds = hidden_source(ws, "Clients", row_count=2)
    ws.fake.data_sources[ws.tasks_ds]["properties"]["Client"] = {
        "id": "clnt", "name": "Client", "description": None, "type": "relation",
        "relation": {"data_source_id": target_ds, "database_id": target_db, "type": "single_property", "single_property": {}}}
    target_view = new_id()
    ws.fake.add_view(view(target_view, target_db, target_ds))
    run_snapshot(ws.fake, database_url, clock)
    assert "Clients" in tables(database_url)
    assert query(database_url, 'SELECT count(*) FROM rows."Clients"')[0][0] == 2
    assert query(database_url, "SELECT count(*) FROM notion.views WHERE view_id = :v", v=target_view)[0][0] == 1
    assert dict(query(database_url, "SELECT database_id::text, is_linked FROM notion.databases"))[target_db] is False


def test_views_and_widgets_lead_to_data_sources_and_views(database_url, clock):
    ws = build_workspace()
    _, metrics_ds = hidden_source(ws, "Metrics")
    dashboard, widget = new_id(), new_id()
    ws.fake.add_view(view(dashboard, ws.projects_db, None, "dashboard", configuration={
        "type": "dashboard", "rows": [{"widgets": [{"id": "w", "view_id": widget, "width": 12, "row_index": 0}]}]}))
    ws.fake.add_view(view(widget, new_id(), metrics_ds, "chart", dashboard_view_id=dashboard))
    run_snapshot(ws.fake, database_url, clock)
    assert ws.fake.count("GET", f"views/{widget}") == 1
    assert "Metrics" in tables(database_url)
    assert {r[0] for r in query(database_url, "SELECT view_id::text FROM notion.views")} >= {dashboard, widget}


def test_link_to_page_and_mentions_are_followed(database_url, clock):
    ws = build_workspace()
    linked_page = hidden_page(ws, WORKSPACE, "Linked")
    mentioned_page = hidden_page(ws, WORKSPACE, "Mentioned in block")
    row_mentioned = hidden_page(ws, WORKSPACE, "Mentioned in a row")
    mentioned_db, mentioned_ds = hidden_source(ws, "Mentioned")
    ws.fake.add_view(view(new_id(), mentioned_db, mentioned_ds))
    linked_db, linked_ds = hidden_source(ws, "Pointed")
    ws.fake.add_view(view(new_id(), linked_db, linked_ds))
    ws.fake.children[ws.standalone_page] += [
        block("link_to_page", {"type": "page_id", "page_id": linked_page}),
        block("link_to_page", {"type": "database_id", "database_id": linked_db}),
        block("paragraph", {"rich_text": [rich_text("see "), page_mention(mentioned_page),
                                          page_mention(mentioned_db, kind="database")], "color": "default"}),
    ]
    ws.fake.pages[ws.task_rows[0]]["properties"]["Name"]["title"].append(page_mention(row_mentioned))
    run_snapshot(ws.fake, database_url, clock, row_bodies=False)
    assert {linked_page, mentioned_page, row_mentioned} <= captured_pages(database_url)
    assert all(body_count(database_url, p) == 1 for p in (linked_page, mentioned_page, row_mentioned))
    assert {"Mentioned", "Pointed"} <= tables(database_url)


def test_mentioned_row_leads_to_its_data_source(database_url, clock):
    ws = build_workspace()
    _, ds_id = hidden_source(ws, "Elsewhere")
    row_id = ws.fake.rows[ds_id][0]
    ws.fake.children[ws.standalone_page].append(
        block("paragraph", {"rich_text": [page_mention(row_id)], "color": "default"}))
    run_snapshot(ws.fake, database_url, clock)
    assert query(database_url, 'SELECT _page_id::text FROM rows."Elsewhere"')[0][0] == row_id
    [parent] = query(database_url, "SELECT parent_type FROM notion.pages WHERE page_id = :p", p=row_id)
    assert parent[0] == "data_source_id"


def test_wiki_child_data_sources_are_followed(database_url, clock):
    ws = build_workspace()
    child_db, child_ds = hidden_source(ws, "Wiki child")
    ws.fake.query_extras[ws.tasks_ds] = [ws.fake.data_sources[child_ds]]
    run_snapshot(ws.fake, database_url, clock)
    assert "Wiki child" in tables(database_url)


# ---------------------------------------------------------------- fixpoint and 404 handling

def test_discovery_repeats_until_nothing_new(database_url, clock):
    ws = build_workspace()
    third = hidden_page(ws, WORKSPACE, "Third")
    db_id, ds_id = hidden_source(ws, "Chain")
    chain_row = ws.fake.rows[ds_id][0]
    ws.fake.set_children(chain_row, [block("link_to_page", {"type": "page_id", "page_id": third})])
    second = hidden_page(ws, WORKSPACE, "Second")
    ws.fake.set_children(second, [block("paragraph", {"rich_text": [page_mention(chain_row)], "color": "default"})])
    ws.fake.children[ws.standalone_page].append(block("child_page", {"title": "Second"}, block_id=second))
    run_snapshot(ws.fake, database_url, clock)
    assert {second, third} <= captured_pages(database_url)
    assert body_count(database_url, chain_row) == 1 and body_count(database_url, third) == 1


def test_404_on_discovery_lookups_ends_the_path(database_url, clock, telemetry_events):
    ws = build_workspace()
    ghost_page, ghost_ds, ghost_db = new_id(), new_id(), new_id()
    ws.fake.children[ws.standalone_page] += [
        block("link_to_page", {"type": "page_id", "page_id": ghost_page}),
        block("link_to_page", {"type": "database_id", "database_id": ghost_db}),
    ]
    ws.fake.data_sources[ws.tasks_ds]["properties"]["Ghost"] = {
        "id": "ghst", "name": "Ghost", "description": None, "type": "relation",
        "relation": {"data_source_id": ghost_ds, "database_id": new_id(), "type": "single_property", "single_property": {}}}
    ws.fake.faults.append(lambda r: error_body(404, "object_not_found")
                          if r.url.path == "/v1/views" and r.url.params.get("database_id") == ghost_db else None)
    run_snapshot(ws.fake, database_url, clock)
    stopped = {e["locus"]["endpoint"] for e in telemetry_events.events if e["event"] == "discovery_stopped"}
    assert stopped == {"/v1/pages/{page_id}", "/v1/data_sources/{data_source_id}", "/v1/views"}
    assert not [e for e in telemetry_events.events if e["event"] == "request_failed"]


def test_404_outside_discovery_still_raises(database_url, clock):
    ws = build_workspace()
    ws.fake.faults.append(lambda r: error_body(404, "object_not_found")
                          if r.url.path == f"/v1/blocks/{ws.standalone_page}/children" else None)
    with pytest.raises(NotionAPIError):
        run_snapshot(ws.fake, database_url, clock)


def test_synced_copy_leads_to_the_page_of_its_original(database_url, clock):
    ws = build_workspace()
    holder = hidden_page(ws, WORKSPACE, "Holder")
    column_list = block("column_list", {}, has_children=True, parent={"type": "page_id", "page_id": holder})
    original = block("synced_block", {"synced_from": None}, has_children=True,
                     parent={"type": "block_id", "block_id": column_list["id"]})
    ws.fake.set_children(holder, [column_list])
    ws.fake.set_children(column_list["id"], [original])
    ws.fake.set_children(original["id"], [paragraph("synced content")])
    copy = block("synced_block", {"synced_from": {"type": "block_id", "block_id": original["id"]}}, has_children=True)
    ws.fake.children[ws.standalone_page].append(copy)
    run_snapshot(ws.fake, database_url, clock)
    assert holder in captured_pages(database_url)
    assert body_count(database_url, holder) == 3
    assert ws.fake.count("GET", f"blocks/{original['id']}") >= 1 and ws.fake.count("GET", f"blocks/{column_list['id']}") >= 1
    [stored] = query(database_url, "SELECT content FROM notion.blocks WHERE block_id = :b", b=copy["id"])
    assert stored[0] == {"synced_from": {"type": "block_id", "block_id": original["id"]}}


def test_synced_copy_of_a_known_page_costs_no_new_page(database_url, clock):
    ws = build_workspace()
    original = block("synced_block", {"synced_from": None}, has_children=True,
                     parent={"type": "page_id", "page_id": ws.standalone_page})
    ws.fake.children[ws.standalone_page].append(original)
    ws.fake.set_children(original["id"], [paragraph("synced content")])
    ws.fake.set_children(ws.project_row, [block("synced_block", {"synced_from": {"type": "block_id",
                                                                                 "block_id": original["id"]}})])
    run_snapshot(ws.fake, database_url, clock)
    assert ws.fake.count("GET", f"pages/{ws.standalone_page}") == 0
    assert body_count(database_url, ws.standalone_page) == 5
