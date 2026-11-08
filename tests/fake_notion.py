"""In-memory fake of the Notion API, served through ``httpx.MockTransport``.

Objects are shaped after the examples of the Notion API reference.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from notion2pg_bulk.json_codec import dumps

JSON = Dict[str, Any]


def new_id() -> str:
    return str(uuid.uuid4())


def rich_text(content: str, **annotations: Any) -> JSON:
    return {
        "type": "text",
        "text": {"content": content, "link": None},
        "annotations": {
            "bold": False, "italic": False, "strikethrough": False,
            "underline": False, "code": False, "color": "default", **annotations,
        },
        "plain_text": content,
        "href": None,
    }


def page_mention(page_id: str, kind: str = "page") -> JSON:
    return {
        "type": "mention",
        "mention": {"type": kind, kind: {"id": page_id}},
        "annotations": {"bold": False, "italic": False, "strikethrough": False,
                        "underline": False, "code": False, "color": "default"},
        "plain_text": "Untitled",
        "href": f"https://www.notion.so/{page_id.replace('-', '')}",
    }


def user(user_id: Optional[str] = None) -> JSON:
    return {"object": "user", "id": user_id or new_id()}


def data_source(ds_id: str, database_id: str, title: str, properties: Dict[str, JSON], **extra: Any) -> JSON:
    return {
        "object": "data_source",
        "id": ds_id,
        "created_time": "2026-01-01T00:00:00.000Z",
        "last_edited_time": "2026-01-01T00:00:00.000Z",
        "created_by": user(),
        "last_edited_by": user(),
        "properties": properties,
        "parent": {"type": "database_id", "database_id": database_id},
        "database_parent": {"type": "page_id", "page_id": new_id()},
        "title": [rich_text(title)] if title else [],
        "description": [],
        "icon": None,
        "archived": False,
        "in_trash": False,
        **extra,
    }


def database(database_id: str, title: str, data_source_ids: List[str], parent: Optional[JSON] = None, **extra: Any) -> JSON:
    return {
        "object": "database",
        "id": database_id,
        "title": [rich_text(title)],
        "description": [rich_text("A description")],
        "parent": parent or {"type": "workspace", "workspace": True},
        "is_inline": False,
        "in_trash": False,
        "is_locked": False,
        "show_data_source_title": False,
        "icon": {"type": "emoji", "emoji": "🐞"},
        "cover": None,
        "url": f"https://www.notion.so/{database_id.replace('-', '')}",
        "public_url": None,
        "data_sources": [{"id": ds, "name": "x"} for ds in data_source_ids],
        **extra,
    }


def page(page_id: str, parent: JSON, properties: Dict[str, JSON], created_time: str = "2026-01-01T00:00:00.000Z", **extra: Any) -> JSON:
    return {
        "object": "page",
        "id": page_id,
        "created_time": created_time,
        "last_edited_time": created_time,
        "created_by": user(),
        "last_edited_by": user(),
        "cover": None,
        "icon": {"type": "emoji", "emoji": "🥑"},
        "parent": parent,
        "in_trash": False,
        "is_archived": False,
        "is_locked": False,
        "properties": properties,
        "url": f"https://www.notion.so/{page_id.replace('-', '')}",
        "public_url": None,
        **extra,
    }


def title_value(text: str) -> JSON:
    return {"id": "title", "type": "title", "title": [rich_text(text)] if text else []}


def block(block_type: str, payload: JSON, has_children: bool = False, block_id: Optional[str] = None, parent: Optional[JSON] = None) -> JSON:
    return {
        "object": "block",
        "id": block_id or new_id(),
        "parent": parent or {"type": "page_id", "page_id": new_id()},
        "created_time": "2026-01-01T00:00:00.000Z",
        "last_edited_time": "2026-01-01T00:00:00.000Z",
        "created_by": user(),
        "last_edited_by": user(),
        "has_children": has_children,
        "in_trash": False,
        "type": block_type,
        block_type: payload,
    }


def paragraph(text: str, has_children: bool = False, block_id: Optional[str] = None, **payload: Any) -> JSON:
    return block("paragraph", {"rich_text": [rich_text(text)], "color": "default", **payload}, has_children, block_id)


def view(view_id: str, database_id: str, data_source_id: Optional[str], view_type: str = "table", **extra: Any) -> JSON:
    return {
        "object": "view",
        "id": view_id,
        "parent": {"type": "database_id", "database_id": database_id},
        "data_source_id": data_source_id,
        "name": f"{view_type} view",
        "type": view_type,
        "filter": None,
        "sorts": None,
        "configuration": {"type": view_type} if view_type != "dashboard" else None,
        "created_time": "2026-01-01T00:00:00.000Z",
        "created_by": user(),
        "last_edited_time": "2026-01-01T00:00:00.000Z",
        "last_edited_by": user(),
        "url": f"https://www.notion.so/{database_id.replace('-', '')}?v={view_id.replace('-', '')}",
        **extra,
    }


def respond_json(status: int, body: Any) -> httpx.Response:
    """A JSON response that keeps Decimal values exact; like Notion, it carries a fresh request_id."""
    if isinstance(body, dict):
        body = {**body, "request_id": body.get("request_id") or new_id()}
    return httpx.Response(status, content=dumps(body).encode(), headers={"content-type": "application/json"})


def error_body(status: int, code: str, message: str = "error") -> httpx.Response:
    return respond_json(status, {"object": "error", "status": status, "code": code,
                                        "message": message, "request_id": new_id()})


Fault = Callable[[httpx.Request], Optional[httpx.Response]]


@dataclass
class FakeNotion:
    """A Notion workspace in memory."""

    data_sources: Dict[str, JSON] = field(default_factory=dict)
    databases: Dict[str, JSON] = field(default_factory=dict)
    pages: Dict[str, JSON] = field(default_factory=dict)
    rows: Dict[str, List[str]] = field(default_factory=dict)
    query_extras: Dict[str, List[JSON]] = field(default_factory=dict)
    children: Dict[str, List[JSON]] = field(default_factory=dict)
    blocks: Dict[str, JSON] = field(default_factory=dict)
    views: Dict[str, JSON] = field(default_factory=dict)
    property_items: Dict[Tuple[str, str], List[JSON]] = field(default_factory=dict)
    search_pages: List[str] = field(default_factory=list)
    search_data_sources: List[str] = field(default_factory=list)
    query_cap: int = 10_000
    page_size_cap: int = 100
    faults: List[Fault] = field(default_factory=list)
    requests: List[httpx.Request] = field(default_factory=list)

    # ------------------------------------------------------------ building

    def add_data_source(self, ds: JSON, *, searchable: bool = True) -> None:
        self.data_sources[ds["id"]] = ds
        self.rows.setdefault(ds["id"], [])
        if searchable:
            self.search_data_sources.append(ds["id"])

    def add_database(self, db: JSON) -> None:
        self.databases[db["id"]] = db

    def add_row(self, ds_id: str, row: JSON) -> None:
        self.pages[row["id"]] = row
        self.rows[ds_id].append(row["id"])

    def add_page(self, pg: JSON, *, searchable: bool = True) -> None:
        self.pages[pg["id"]] = pg
        if searchable:
            self.search_pages.append(pg["id"])

    def set_children(self, parent_id: str, blocks: List[JSON]) -> None:
        self.children[parent_id] = blocks
        for child in blocks:
            self.blocks[child["id"]] = child

    def add_view(self, v: JSON) -> None:
        self.views[v["id"]] = v

    # ------------------------------------------------------------ transport

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for fault in self.faults:
            response = fault(request)
            if response is not None:
                return response
        path = request.url.raw_path.decode().split("?")[0]
        parts = path.split("/")[2:]
        method = request.method
        query = dict(request.url.params)
        body = json.loads(request.content) if request.content else {}
        if method == "POST" and parts == ["search"]:
            return self._search(body)
        if method == "GET" and parts[0] == "data_sources" and len(parts) == 2:
            return self._get(self.data_sources, parts[1])
        if method == "POST" and parts[0] == "data_sources" and parts[2:] == ["query"]:
            return self._query(parts[1], body)
        if method == "GET" and parts[0] == "databases" and len(parts) == 2:
            return self._get(self.databases, parts[1])
        if method == "GET" and parts[0] == "pages" and len(parts) == 2:
            return self._get(self.pages, parts[1])
        if method == "GET" and parts[0] == "pages" and len(parts) == 4 and parts[2] == "properties":
            return self._property(parts[1], parts[3], query)
        if method == "GET" and parts[0] == "blocks" and len(parts) == 2:
            return self._get(self.blocks, parts[1])
        if method == "GET" and parts[0] == "blocks" and parts[2:] == ["children"]:
            return self._list(self.children.get(parts[1], []), query, extra={"type": "block", "block": {}})
        if method == "GET" and parts == ["views"]:
            return self._views(query)
        if method == "GET" and parts[0] == "views" and len(parts) == 2:
            return self._get(self.views, parts[1])
        return error_body(400, "invalid_request_url", f"unhandled {method} {path}")

    def count(self, method: str, prefix: str) -> int:
        return sum(1 for r in self.requests if r.method == method and r.url.path.startswith("/v1/" + prefix))

    # ------------------------------------------------------------ endpoints

    def _get(self, store: Dict[str, JSON], object_id: str) -> httpx.Response:
        if object_id not in store:
            return error_body(404, "object_not_found", f"Could not find {object_id}")
        return respond_json(200, store[object_id])

    def _page(self, items: List[JSON], cursor: Optional[str], size: Optional[int]) -> Tuple[List[JSON], Optional[str]]:
        start = int(cursor) if cursor else 0
        size = min(int(size or 100), self.page_size_cap)
        chunk = items[start : start + size]
        end = start + len(chunk)
        return chunk, (str(end) if end < len(items) else None)

    def _list(self, items: List[JSON], params: Dict[str, Any], extra: Optional[JSON] = None) -> httpx.Response:
        chunk, cursor = self._page(items, params.get("start_cursor"), params.get("page_size"))
        return respond_json(200, {"object": "list", "results": chunk, "next_cursor": cursor,
                                         "has_more": cursor is not None, **(extra or {})})

    def _search(self, body: JSON) -> httpx.Response:
        kind = body["filter"]["value"]
        if kind == "page":
            items = [self.pages[i] for i in self.search_pages]
        else:
            items = [self.data_sources[i] for i in self.search_data_sources]
        return self._list(items, body, extra={"type": "page_or_data_source", "page_or_data_source": {}})

    def _query(self, ds_id: str, body: JSON) -> httpx.Response:
        if ds_id not in self.data_sources:
            return error_body(404, "object_not_found")
        items = [self.pages[i] for i in self.rows[ds_id]] + self.query_extras.get(ds_id, [])
        sorts = body.get("sorts") or []
        if sorts == [{"timestamp": "created_time", "direction": "ascending"}]:
            items = sorted(items, key=lambda o: o["created_time"])
        window = body.get("filter")
        if window:
            assert window["timestamp"] == "created_time"
            bound = window["created_time"]["on_or_after"]
            items = [o for o in items if o["created_time"] >= bound]
        capped = len(items) > self.query_cap
        items = items[: self.query_cap]
        chunk, cursor = self._page(items, body.get("start_cursor"), body.get("page_size"))
        payload: JSON = {"object": "list", "results": chunk, "next_cursor": cursor,
                         "has_more": cursor is not None, "type": "page_or_data_source",
                         "page_or_data_source": {}}
        if capped:
            payload["request_status"] = {"type": "incomplete", "incomplete_reason": "query_result_limit_reached"}
        return respond_json(200, payload)

    def _property(self, page_id: str, raw_property_id: str, params: Dict[str, Any]) -> httpx.Response:
        key = (page_id, raw_property_id)
        if key not in self.property_items:
            return error_body(404, "object_not_found")
        items = self.property_items[key]
        chunk, cursor = self._page(items, params.get("start_cursor"), params.get("page_size"))
        prop_type = items[0]["type"] if items else "relation"
        return respond_json(200, {
            "object": "list", "results": chunk, "next_cursor": cursor, "has_more": cursor is not None,
            "type": "property_item",
            "property_item": {"id": key[1], "next_url": None, "type": prop_type, prop_type: {}},
        })

    def _views(self, params: Dict[str, Any]) -> httpx.Response:
        if "data_source_id" in params:
            ds_id = params["data_source_id"]
            refs = [v for v in self.views.values() if v["data_source_id"] == ds_id]
        else:
            db_id = params["database_id"]
            refs = [v for v in self.views.values() if v["parent"]["database_id"] == db_id]
            if not refs and db_id not in self.databases:
                return error_body(404, "object_not_found", f"Could not find database with ID: {db_id}")
        refs = [{"object": "view", "id": v["id"]} for v in refs]
        return self._list(refs, params, extra={"type": "view", "view": {}})
