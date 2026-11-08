"""Setup and teardown of the live test workspace, all under one root page.

Setup builds, through the REST API, every fixture the live tests need. Titles carry a
per-run tag so leftovers of an earlier run can never collide with this one. Teardown
moves the root page (and so everything under it, the UI-made items included) to the trash.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from live_rest import NotionREST, NotionRESTError, mention, paragraph, plain, text

UI_DATABASE_TITLE = "UI Full Page"
SEARCH_WAIT_S = 180.0
ARCHIVE_UNAVAILABLE = "archive_content"

DATES = {
    "date_only": {"start": "2026-09-15"},
    "datetime": {"start": "2026-09-15T09:30:00.000+02:00"},
    "range": {"start": "2026-09-15", "end": "2026-09-18"},
    "zoned": {"start": "2026-09-15T09:00:00", "end": "2026-09-15T10:00:00", "time_zone": "America/Los_Angeles"},
}


@dataclass
class Workspace:
    """IDs of everything the live tests use."""

    root: str
    tag: str
    ids: Dict[str, Any] = field(default_factory=dict)
    ui: Dict[str, str] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        return self.ids[key]

    def title(self, base: str) -> str:
        return f"{base} {self.tag}"


class Builder:
    def __init__(self, rest: NotionREST, workspace: Workspace):
        self.rest = rest
        self.ws = workspace

    def database(self, parent: str, base: str, properties: Dict[str, Any], inline: bool = True):
        db = self.rest.call("POST", "databases", {
            "parent": {"type": "page_id", "page_id": parent},
            "title": [text(self.ws.title(base))],
            "is_inline": inline,
            "initial_data_source": {"properties": properties},
        })
        return db["id"], db["data_sources"][0]["id"]

    def row(self, data_source_id: str, title: Any, **properties: Any) -> str:
        name = title if isinstance(title, list) else [text(title)]
        return self.rest.call("POST", "pages", {
            "parent": {"type": "data_source_id", "data_source_id": data_source_id},
            "properties": {"Name": {"title": name}, **properties},
        })["id"]

    def page(self, parent: str, title: str, blocks: Optional[List[Dict[str, Any]]] = None) -> str:
        body: Dict[str, Any] = {"parent": {"type": "page_id", "page_id": parent},
                                "properties": {"title": {"title": [text(title)]}}}
        if blocks:
            body["children"] = blocks
        return self.rest.call("POST", "pages", body)["id"]

    def linked(self, parent: str, data_source_id: str, name: str) -> Dict[str, str]:
        view = self.rest.call("POST", "views", {"create_database": {"parent": {"type": "page_id", "page_id": parent}},
                                                "data_source_id": data_source_id, "name": name, "type": "table"})
        return {"view": view["id"], "database": view["parent"]["database_id"]}


def build(rest: NotionREST, root: str) -> Workspace:
    """Create every API-built fixture under ``root`` and return their IDs."""
    ws = Workspace(root=root, tag=uuid.uuid4().hex[:6])
    b = Builder(rest, ws)
    ids = ws.ids
    name = {"Name": {"title": {}}}

    ids["trash_db"], ids["trash_ds"] = b.database(root, "TrashMe", name)
    ids["trash_row"] = b.row(ids["trash_ds"], "doomed")
    ids["targets_db"], ids["targets_ds"] = b.database(root, "Targets", name, inline=False)
    ids["targets"] = [b.row(ids["targets_ds"], f"T{i:02d}") for i in range(30)]
    ids["main_db"], ids["main_ds"] = b.database(root, "Main", {
        **name,
        "Notes": {"rich_text": {}},
        "Links": {"relation": {"data_source_id": ids["targets_ds"], "single_property": {}}},
        "When": {"date": {}},
        "Gone": {"relation": {"data_source_id": ids["trash_ds"], "single_property": {}}},
    })
    main = ids["main_ds"]
    many_mentions = [mention("page", target) for target in ids["targets"]]
    ids["many"] = b.row(main, [text("Many ")] + many_mentions, Notes={"rich_text": many_mentions},
                        Links={"relation": [{"id": target} for target in ids["targets"]]})
    ids["dates"] = {key: b.row(main, key, When={"date": value}) for key, value in DATES.items()}
    ids["pointer"] = b.row(main, "points at trash", Gone={"relation": [{"id": ids["trash_row"]}]})
    ids["archived"] = b.row(main, "archived")
    try:
        rest.call("PATCH", f"pages/{ids['archived']}", {"is_archived": True})
        ids["archive_available"] = True
    except NotionRESTError as error:
        if ARCHIVE_UNAVAILABLE not in (error.message or ""):
            raise
        ids["archive_available"] = False

    ids["holder"] = b.row(main, "hidden holder")
    ids["synced_page"] = b.page(ids["holder"], ws.title("Synced A"), [
        {"type": "synced_block", "synced_block": {"synced_from": None, "children": [paragraph([text("synced content")])]}}])
    ids["synced_original"] = rest.children(ids["synced_page"])[0]["id"]
    ids["hidden_home"] = b.page(ids["holder"], ws.title("Hidden home"))
    ids["bymention_db"], ids["bymention_ds"] = b.database(ids["hidden_home"], "ByMention", name)
    b.row(ids["bymention_ds"], "m1")
    ids["bylink_db"], ids["bylink_ds"] = b.database(ids["hidden_home"], "ByLink", name, inline=False)
    b.row(ids["bylink_ds"], "l1")
    ids["rowonly_db"], ids["rowonly_ds"] = b.database(ids["hidden_home"], "RowOnly", name)
    ids["mentioned_row"] = b.row(ids["rowonly_ds"], "mentioned row")
    hidden_linked = b.linked(ids["hidden_home"], ids["bymention_ds"], "hidden linked")
    ids["hidden_linked_view"], ids["hidden_linked_db"] = hidden_linked["view"], hidden_linked["database"]
    ids["widgetless_db"] = b.linked(ids["holder"], ids["targets_ds"], "widgetless host")["database"]
    ids["widgetless_dashboard"] = rest.call("POST", "views", {
        "database_id": ids["widgetless_db"], "data_source_id": ids["targets_ds"], "name": "empty dashboard", "type": "dashboard"})["id"]

    ids["dashboard"] = rest.call("POST", "views", {"database_id": ids["main_db"], "data_source_id": main,
                                                   "name": "overview", "type": "dashboard"})["id"]
    ids["widget"] = rest.call("POST", "views", {"view_id": ids["dashboard"], "data_source_id": ids["targets_ds"],
                                                "name": "targets widget", "type": "table"})["id"]
    api_linked = b.linked(root, main, "api linked")
    ids["api_linked_view"], ids["api_linked_db"] = api_linked["view"], api_linked["database"]
    ids["locked_page"] = b.page(root, ws.title("Locked page"), [paragraph([text("locked")])])
    rest.call("PATCH", f"pages/{ids['locked_page']}", {"is_locked": True})
    rest.call("PATCH", f"databases/{ids['targets_db']}", {"is_locked": True})

    ids["gone_page"] = b.page(root, ws.title("Gone page"), [paragraph([text("gone")])])
    ids["gone_db"], ids["gone_ds"] = b.database(root, "GoneDB", name)

    rest.call("PATCH", f"blocks/{root}/children", {"children": [
        paragraph([text("mentions: "), mention("page", ids["gone_page"]), mention("database", ids["gone_db"]),
                   mention("database", ids["bymention_db"]), mention("database", ids["hidden_linked_db"]),
                   mention("page", ids["mentioned_row"])]),
        {"type": "link_to_page", "link_to_page": {"type": "database_id", "database_id": ids["bylink_db"]}},
        {"type": "synced_block", "synced_block": {"synced_from": {"type": "block_id", "block_id": ids["synced_original"]}}},
    ]})
    ids["synced_copy"] = [block["id"] for block in rest.children(root) if block["type"] == "synced_block"][0]

    for path in (f"pages/{ids['gone_page']}", f"databases/{ids['gone_db']}", f"databases/{ids['trash_db']}"):
        rest.call("PATCH", path, {"in_trash": True})
    ws.ui = find_ui_fixtures(rest, root)
    return ws


def find_ui_fixtures(rest: NotionREST, root: str) -> Dict[str, str]:
    """Locate the optional UI-made fixtures on the root page (see README → Live integration tests)."""
    found: Dict[str, str] = {}
    blocks = [block for block in rest.children(root) if block["type"] == "child_database"]
    for block in blocks:
        if block["child_database"]["title"] == UI_DATABASE_TITLE:
            status, database = rest.status("GET", f"databases/{block['id']}")
            if status == 200 and database.get("data_sources"):
                found["database"] = block["id"]
                found["data_source"] = database["data_sources"][0]["id"]
    if "data_source" in found:
        for block in blocks:
            if block["id"] == found["database"]:
                continue
            status, listing = rest.status("GET", "views", query={"database_id": block["id"]})
            views = [rest.call("GET", f"views/{ref['id']}") for ref in listing.get("results", [])] if status == 200 else []
            if views and all(view["data_source_id"] == found["data_source"] for view in views):
                found["linked"] = block["id"]
    return found


def wait_for_search(rest: NotionREST, ws: Workspace) -> None:
    """Wait (bounded) until search indexes the fixtures it is expected to return."""
    data_sources = {ws["targets_ds"], ws["main_ds"], ws["bymention_ds"], ws["bylink_ds"], ws["rowonly_ds"]}
    pages = {ws["synced_page"], ws["hidden_home"], ws["locked_page"]}
    deadline = time.monotonic() + SEARCH_WAIT_S
    while True:
        indexed_sources = {result["id"] for result in rest.search("data_source")}
        indexed_pages = {result["id"] for result in rest.search("page")}
        if data_sources <= indexed_sources and pages <= indexed_pages:
            return
        if time.monotonic() > deadline:
            raise TimeoutError("Search did not index the live fixtures within %.0f s" % SEARCH_WAIT_S)
        time.sleep(10)


def teardown(rest: NotionREST, root: str) -> None:
    """Move the root page, with every fixture under it (UI-made ones included), to the trash."""
    rest.call("PATCH", f"pages/{root}", {"in_trash": True})
    status, page = rest.status("GET", f"pages/{root}")
    if status == 200 and not page.get("in_trash"):
        raise AssertionError(f"Root page {root} is not in the trash after teardown")


def title_of(obj: Dict[str, Any]) -> str:
    return plain(obj.get("title"))
