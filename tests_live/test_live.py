"""Live checks of assumptions the snapshot depends on that the Notion docs don't state exactly.

Each test names the doc statement it checks. A failure means the assumption no longer
holds: the answer is a loud failure in the code or a "Coverage limits" entry, never a
workaround built on observed behaviour.
"""

from __future__ import annotations

import pytest

from live_db import dump, pg
from live_workspace import DATES

LINKED_NAMES = ("api_linked_db", "hidden_linked_db")


def mentions(rich):
    return sum(1 for item in rich or [] if item.get("type") == "mention")


def stops(runs, label, reason):
    return [event for event in runs.events[label] if event["event"] == "discovery_stopped" and event["reason"] == reason]


def test_trashed_targets_end_their_path(runs, rest):
    """Docs: retrieve endpoints expose ``in_trash``; whether a trashed object is returned or 404 is not stated."""
    ws, url = runs.workspace, runs.urls["full_1"]
    assert rest.call("GET", f"data_sources/{ws['trash_ds']}")["in_trash"] is True
    assert rest.status("POST", f"data_sources/{ws['trash_ds']}/query", {})[0] == 404
    assert ws["trash_ds"] not in {r[0] for r in pg(url, "SELECT data_source_id::text FROM notion.data_sources")}
    assert ws["gone_page"] not in {r[0] for r in pg(url, "SELECT page_id::text FROM notion.pages")}
    assert ws["gone_db"] not in {r[0] for r in pg(url, "SELECT database_id::text FROM notion.databases")}
    trashed = {event["locus"]["entity_id"] for event in stops(runs, "full_1", "in_trash")}
    assert {ws["trash_ds"], ws["gone_page"]} <= trashed


def test_full_page_databases_are_child_database_blocks(runs, rest):
    """Docs (block#child-database): child_database blocks come from Create a database; full-page
    databases and block ID = database ID are not stated."""
    ws = runs.workspace
    blocks = {block["id"] for block in rest.children(ws.root) if block["type"] == "child_database"}
    assert ws["targets_db"] in blocks
    assert rest.call("GET", f"data_sources/{ws['targets_ds']}")["parent"]["database_id"] == ws["targets_db"]


def test_ui_made_full_page_database_is_reached_without_search(runs, rest):
    """As above, for a database made in the Notion UI, hidden from search in the filtered run."""
    ui = runs.workspace.ui
    if "database" not in ui:
        pytest.skip("no UI-made 'UI Full Page' database on the root page (README → Live integration tests)")
    assert ui["database"] in {block["id"] for block in rest.children(runs.workspace.root) if block["type"] == "child_database"}
    databases = dict(pg(runs.urls["filtered"], "SELECT database_id::text, is_linked FROM notion.databases"))
    assert databases.get(ui["database"]) is False
    assert ui["data_source"] in {r[0] for r in pg(runs.urls["filtered"], "SELECT data_source_id::text FROM notion.catalog_tables")}


def _check_linked(rest, url, container, data_source_id, parent):
    status, listing = rest.status("GET", "views", query={"database_id": container})
    assert status == 200 and listing["results"]
    views = [rest.call("GET", f"views/{ref['id']}") for ref in listing["results"]]
    assert all(view["parent"]["database_id"] == container for view in views)
    assert all(view["data_source_id"] == data_source_id for view in views)
    [row] = pg(url, "SELECT is_linked, parent_id::text FROM notion.databases WHERE database_id = :d", d=container)
    assert row == (True, parent)
    stored = dict(pg(url, "SELECT view_id::text, database_id::text FROM notion.views"))
    assert all(stored.get(view["id"]) == container for view in views)


def test_linked_databases_are_listed_by_database_id(runs, rest):
    """Docs (list-views): database_id is the 'ID of a Notion database (collection view block)';
    linked databases are not named explicitly."""
    ws = runs.workspace
    _check_linked(rest, runs.urls["full_1"], ws["api_linked_db"], ws["main_ds"], ws.root)
    _check_linked(rest, runs.urls["full_1"], ws["hidden_linked_db"], ws["bymention_ds"], ws["hidden_home"])


def test_ui_made_linked_database_is_listed_by_database_id(runs, rest):
    """As above, for a linked database made in the Notion UI."""
    ui = runs.workspace.ui
    if "linked" not in ui:
        pytest.skip("no UI-made linked view of 'UI Full Page' on the root page (README → Live integration tests)")
    _check_linked(rest, runs.urls["full_1"], ui["linked"], ui["data_source"], runs.workspace.root)


def test_values_over_25_items_are_complete(runs, rest):
    """Docs (page-property-values#paginated-page-properties): has_more is explicit only for relations."""
    ws = runs.workspace
    properties = rest.call("GET", f"pages/{ws['many']}")["properties"]
    assert len(properties["Links"]["relation"]) <= 25
    [(links, name, notes)] = pg(runs.urls["full_1"], 'SELECT "Links", "Name", "Notes" FROM rows."Main %s" WHERE _page_id = :p'
                                % ws.tag, p=ws["many"])
    assert sorted(links) == sorted(ws["targets"])
    assert mentions(name) == 30 and mentions(notes) == 30


def test_database_reached_only_by_id_is_classified(runs):
    """Docs: database mentions carry ``database.id``; link_to_page carries ``database_id``;
    telling original from linked by ID is not documented (filtered run: no search, no row bodies)."""
    ws, url = runs.workspace, runs.urls["filtered"]
    databases = dict(pg(url, "SELECT database_id::text, is_linked FROM notion.databases"))
    tables = {r[0] for r in pg(url, "SELECT data_source_id::text FROM notion.catalog_tables")}
    assert databases.get(ws["bymention_db"]) is False and ws["bymention_ds"] in tables
    assert databases.get(ws["bylink_db"]) is False and ws["bylink_ds"] in tables
    assert databases.get(ws["hidden_linked_db"]) is True
    rows = {r[0] for r in pg(url, 'SELECT _page_id::text FROM rows."RowOnly %s"' % ws.tag)}
    assert ws["mentioned_row"] in rows


def test_synced_copies_are_pointers_and_lead_to_their_original(runs, rest):
    """Docs (block#synced-block): a duplicate's synced_from.block_id identifies the original;
    a copy's has_children and child listing are not stated."""
    ws = runs.workspace
    copy = rest.call("GET", f"blocks/{ws['synced_copy']}")
    assert copy["synced_block"]["synced_from"]["block_id"] == ws["synced_original"]
    url = runs.urls["full_1"]
    [(content,)] = pg(url, "SELECT content FROM notion.blocks WHERE block_id = :b", b=ws["synced_copy"])
    assert content == {"synced_from": copy["synced_block"]["synced_from"]}
    assert pg(url, "SELECT count(*) FROM notion.blocks WHERE parent_block_id = :b", b=ws["synced_copy"]) == [(0,)]
    assert pg(url, "SELECT count(*) FROM notion.blocks WHERE content::text LIKE '%synced content%'") == [(1,)]
    assert pg(runs.urls["filtered"], "SELECT count(*) FROM notion.blocks WHERE page_id = :p", p=ws["synced_page"]) == [(2,)]


def test_dashboards_and_widgets_are_captured(runs, rest):
    """Docs (view): data_source_id is null for dashboards; dashboard_view_id marks widgets;
    configuration.rows[].widgets[].view_id lists them."""
    ws = runs.workspace
    dashboard = rest.call("GET", f"views/{ws['dashboard']}")
    assert dashboard["data_source_id"] is None
    assert ws["widget"] in [w["view_id"] for r in dashboard["configuration"]["rows"] for w in r["widgets"]]
    stored = {r[0]: r[1:] for r in pg(runs.urls["full_1"], "SELECT view_id::text, database_id::text, data_source_id::text, "
                                                            "dashboard_view_id::text FROM notion.views")}
    assert stored[ws["dashboard"]] == (ws["main_db"], None, None)
    assert stored[ws["widget"]] == (ws["main_db"], ws["targets_ds"], ws["dashboard"])
    assert ws["widgetless_dashboard"] in stored
    filtered = {r[0] for r in pg(runs.urls["filtered"], "SELECT view_id::text FROM notion.views")}
    assert ws["widgetless_dashboard"] not in filtered, "README → Coverage limits: widget-less dashboard in an unreached linked database"


def test_windowing_primitives(runs, rest):
    """Docs (query-large-data-sources): created_time is stored to the minute; rows sharing the
    boundary timestamp appear in both windows."""
    ws = runs.workspace
    sort = [{"timestamp": "created_time", "direction": "ascending"}]
    rows = rest.call("POST", f"data_sources/{ws['targets_ds']}/query", {"sorts": sort})["results"]
    stamps = [row["created_time"] for row in rows]
    assert all(stamp.endswith(":00.000Z") for stamp in stamps) and stamps == sorted(stamps)
    boundary = rows[len(rows) // 2]["created_time"]
    window = rest.call("POST", f"data_sources/{ws['targets_ds']}/query", {
        "sorts": sort, "filter": {"timestamp": "created_time", "created_time": {"on_or_after": boundary}}})["results"]
    assert {row["id"] for row in rows if row["created_time"] >= boundary} <= {row["id"] for row in window}


def test_archived_rows_are_absent(runs):
    """Docs (query-a-data-source): 'By default, this endpoint returns non-archived pages'."""
    ws = runs.workspace
    if not ws["archive_available"]:
        pytest.skip("this workspace's plan does not offer archiving (archive_content)")
    rows = {r[0] for r in pg(runs.urls["full_1"], 'SELECT _page_id::text FROM rows."Main %s"' % ws.tag)}
    assert ws["archived"] not in rows


def test_dates_are_stored_as_returned(runs, rest):
    """Docs (page-property-values#date): {start, end, time_zone}; whether a written time_zone
    is returned is not stated (README: it may come back as an offset only)."""
    ws = runs.workspace
    stored = dict(pg(runs.urls["full_1"], 'SELECT _page_id::text, "When" FROM rows."Main %s" WHERE "When" IS NOT NULL' % ws.tag))
    for key, page_id in ws["dates"].items():
        returned = rest.call("GET", f"pages/{page_id}")["properties"]["When"]["date"]
        assert stored[page_id] == returned, key
    assert stored[ws["dates"]["date_only"]]["start"] == DATES["date_only"]["start"]


def test_lock_state_is_captured(runs):
    """Docs (patch-page): is_locked controls editing in Notion; the page/database object tables don't list it."""
    ws, url = runs.workspace, runs.urls["full_1"]
    assert pg(url, "SELECT is_locked FROM notion.pages WHERE page_id = :p", p=ws["locked_page"]) == [(True,)]
    assert pg(url, "SELECT is_locked FROM notion.databases WHERE database_id = :d", d=ws["targets_db"]) == [(True,)]
    assert pg(url, "SELECT is_locked FROM notion.pages WHERE page_id = :p", p=ws.root) == [(False,)]


def test_two_snapshots_are_identical(runs):
    """Two snapshots of an unchanged workspace into two empty databases are identical."""
    first, second = dump(runs.urls["full_1"]), dump(runs.urls["full_2"])
    assert first == second and sum(len(rows) for rows in first.values()) > 100
