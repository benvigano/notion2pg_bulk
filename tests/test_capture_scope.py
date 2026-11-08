"""Capture scope: discovery beyond search, dashboards, linked databases, icons."""

from __future__ import annotations

from fake_notion import block, data_source, new_id, page, paragraph, title_value, view
from helpers import run_snapshot
from test_rows import TITLE
from test_snapshot import query
from workspace import build_workspace

HOSTED_ICON = {"type": "file", "file": {"url": "https://prod-files-secure.s3/x.png", "expiry_time": "2026-11-08T12:00:00.000Z"}}
CUSTOM_EMOJI = {"type": "custom_emoji", "custom_emoji": {"id": "e1", "name": "party", "url": "https://e.x/p.png"}}


def test_data_source_missed_by_search_is_found_through_its_database(database_url, clock):
    ws = build_workspace()
    hidden = new_id()
    ws.fake.databases[ws.tasks_db]["data_sources"].append({"id": hidden, "name": "Hidden"})
    ws.fake.add_data_source(data_source(hidden, ws.tasks_db, "Hidden", {"Name": TITLE}), searchable=False)
    ws.fake.add_row(hidden, page(new_id(), {"type": "data_source_id", "data_source_id": hidden, "database_id": ws.tasks_db},
                                 {"Name": title_value("found")}))
    run_snapshot(ws.fake, database_url, clock)
    assert query(database_url, 'SELECT count(*) FROM rows."Hidden"')[0][0] == 1


def test_synced_data_source_parented_by_a_data_source(database_url, clock):
    ws = build_workspace()
    synced = new_id()
    obj = data_source(synced, ws.tasks_db, "Synced", {"Name": TITLE},
                      sync_metadata={"external_database_sync": {"sync_id": new_id(), "provider": "github"}})
    obj["parent"] = {"type": "data_source_id", "data_source_id": ws.tasks_ds, "database_id": ws.tasks_db}
    ws.fake.add_data_source(obj)
    run_snapshot(ws.fake, database_url, clock)
    [found] = query(database_url, "SELECT database_id::text, parent_type, parent_id::text, sync_metadata "
                                  "FROM notion.data_sources WHERE data_source_id = :d", d=synced)
    assert found[:3] == (ws.tasks_db, "data_source_id", ws.tasks_ds)
    assert found[3]["external_database_sync"]["provider"] == "github"


def test_dashboards_and_widgets_are_listed_by_database(database_url, clock):
    ws = build_workspace()
    dashboard, widget = new_id(), new_id()
    ws.fake.add_view(view(dashboard, ws.projects_db, None, "dashboard",
                          rows=[{"widgets": [{"id": "w1", "view_id": widget, "width": 12, "row_index": 0}]}]))
    ws.fake.add_view(view(widget, ws.projects_db, ws.tasks_ds, "chart", dashboard_view_id=dashboard))
    run_snapshot(ws.fake, database_url, clock)
    found = {r[0]: r[1:] for r in query(database_url, "SELECT view_id::text, type, data_source_id::text, "
                                                      "dashboard_view_id::text, content FROM notion.views")}
    assert found[dashboard][:3] == ("dashboard", None, None)
    assert found[dashboard][3]["rows"][0]["widgets"][0]["view_id"] == widget
    assert found[widget][:3] == ("chart", ws.tasks_ds, dashboard)
    assert ws.fake.count("GET", f"views/{widget}") == 1


def test_linked_databases_from_views_and_blocks(database_url, clock):
    ws = build_workspace()
    via_view_only, via_block = new_id(), new_id()
    ws.fake.add_view(view(new_id(), via_view_only, ws.tasks_ds))
    ws.fake.add_view(view(new_id(), via_block, ws.projects_ds, "board"))
    toggle = block("toggle", {"rich_text": [], "color": "default"}, has_children=True)
    ws.fake.children[ws.standalone_page].append(toggle)
    ws.fake.set_children(toggle["id"], [block("child_database", {"title": "My linked tasks"}, block_id=via_block)])
    ws.fake.children[ws.standalone_page].append(block("child_database", {"title": "Projects DB"}, block_id=ws.projects_db))
    run_snapshot(ws.fake, database_url, clock)
    found = {r[0]: r[1:] for r in query(database_url, "SELECT database_id::text, is_linked, block_title, parent_type, "
                                                      "parent_id::text, title FROM notion.databases")}
    assert found[via_view_only] == (True, None, None, None, None)
    assert found[via_block] == (True, "My linked tasks", "block_id", toggle["id"], None)
    assert found[ws.projects_db][0] is False and found[ws.projects_db][4][0]["plain_text"] == "Projects DB"
    assert ws.fake.count("GET", f"databases/{via_block}") == 0
    assert ws.fake.count("GET", f"blocks/{via_block}/children") == 0


def test_child_pages_found_while_walking_are_captured(database_url, clock):
    ws = build_workspace()
    hidden, trashed, nested = new_id(), new_id(), new_id()
    ws.fake.add_page(page(hidden, {"type": "page_id", "page_id": ws.standalone_page}, {"title": title_value("Hidden")}),
                     searchable=False)
    ws.fake.add_page(page(trashed, {"type": "page_id", "page_id": ws.standalone_page}, {"title": title_value("Bin")},
                          in_trash=True), searchable=False)
    ws.fake.add_page(page(nested, {"type": "page_id", "page_id": hidden}, {"title": title_value("Nested")}),
                     searchable=False)
    ws.fake.children[ws.standalone_page] += [block("child_page", {"title": "Hidden"}, has_children=True, block_id=hidden),
                                             block("child_page", {"title": "Bin"}, block_id=trashed)]
    ws.fake.set_children(hidden, [paragraph("hidden body"), block("child_page", {"title": "Nested"}, block_id=nested)])
    ws.fake.set_children(nested, [paragraph("nested body")])
    run_snapshot(ws.fake, database_url, clock)
    pages = {r[0]: r[1] for r in query(database_url, "SELECT page_id::text, parent_id::text FROM notion.pages")}
    assert pages[hidden] == ws.standalone_page and pages[nested] == hidden and trashed not in pages
    bodies = {r[0]: r[1] for r in query(database_url, "SELECT page_id::text, count(*) FROM notion.blocks GROUP BY 1")}
    assert bodies[hidden] == 2 and bodies[nested] == 1 and trashed not in bodies
    assert bodies[ws.standalone_page] == 5


def test_hosted_icons_are_dropped_and_others_kept(database_url, clock):
    ws = build_workspace()
    ws.fake.pages[ws.standalone_page]["icon"] = HOSTED_ICON
    ws.fake.pages[ws.project_row]["icon"] = CUSTOM_EMOJI
    ws.fake.databases[ws.projects_db]["icon"] = HOSTED_ICON
    ws.fake.children[ws.standalone_page].append(
        block("callout", {"rich_text": [], "icon": HOSTED_ICON, "color": "gray_background"}))
    ws.fake.children[ws.standalone_page].append(
        block("callout", {"rich_text": [], "icon": {"type": "icon", "icon": {"name": "pizza", "color": "blue"}},
                          "color": "default"}))
    run_snapshot(ws.fake, database_url, clock)
    icons = dict(query(database_url, "SELECT page_id::text, icon FROM notion.pages"))
    assert icons[ws.standalone_page] is None and icons[ws.project_row] == CUSTOM_EMOJI
    assert query(database_url, "SELECT icon FROM notion.databases WHERE database_id = :d", d=ws.projects_db)[0][0] is None
    callouts = [r[0] for r in query(database_url, "SELECT content->'icon' FROM notion.blocks WHERE type = 'callout' "
                                                  "ORDER BY position")]
    assert callouts == [None, {"type": "icon", "icon": {"name": "pizza", "color": "blue"}}]


def test_trashed_and_archived_search_results_are_not_captured(database_url, clock):
    ws = build_workspace()
    gone = [new_id(), new_id()]
    ws.fake.add_page(page(gone[0], {"type": "workspace", "workspace": True}, {"title": title_value("T")}, in_trash=True))
    ws.fake.add_page(page(gone[1], {"type": "workspace", "workspace": True}, {"title": title_value("A")}, is_archived=True))
    run_snapshot(ws.fake, database_url, clock)
    pages = {r[0] for r in query(database_url, "SELECT page_id::text FROM notion.pages")}
    assert not pages & set(gone)


def test_page_and_database_fields(database_url, clock):
    ws = build_workspace()
    ws.fake.pages[ws.standalone_page].update(is_locked=True, public_url="https://x.notion.site/h")
    ws.fake.databases[ws.projects_db].update(is_locked=True, show_data_source_title=True, is_inline=True,
                                             parent={"type": "page_id", "page_id": ws.standalone_page})
    run_snapshot(ws.fake, database_url, clock)
    [p] = query(database_url, "SELECT is_locked, public_url, title FROM notion.pages WHERE page_id = :p", p=ws.standalone_page)
    assert p[:2] == (True, "https://x.notion.site/h") and p[2][0]["plain_text"] == "Handbook"
    [d] = query(database_url, "SELECT is_locked, show_data_source_title, is_inline, parent_type, parent_id::text, "
                              "description FROM notion.databases WHERE database_id = :d", d=ws.projects_db)
    assert d[:5] == (True, True, True, "page_id", ws.standalone_page)
    assert d[5][0]["plain_text"] == "A description"
