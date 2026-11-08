"""Fetch: Notion API objects → records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Literal, Mapping, Optional, Sequence, Tuple

from .errors import SnapshotIntegrityError
from .mapping import PAGINATED_TYPES, column_type, column_value
from .notion_api import NotionAPI
from .records import (
    BlockRecord,
    DatabaseRecord,
    DataSourceRecord,
    PageRecord,
    Parent,
    RowRecord,
    ViewRecord,
    parse_data_source,
    parse_database,
    parse_page,
    parse_view,
    sanitize_icon,
)

MAX_BLOCK_DEPTH = 100
PAGE_RESPONSE_ITEM_CAP = 25
CREATED_TIME_ASCENDING = [{"timestamp": "created_time", "direction": "ascending"}]
RESULT_LIMIT_REASON = "query_result_limit_reached"
UNCAPTURED_BLOCK_TYPES = frozenset({"image", "video", "audio", "file", "pdf", "embed"})
UNWALKED_BLOCK_TYPES = frozenset({"child_page", "child_database", "unsupported"})
SELECT_TYPES = frozenset({"select", "status"})


# ---------------------------------------------------------------- discovery

def discover_data_source_ids(api: NotionAPI) -> List[str]:
    """:returns: IDs of the data sources found by search, in result order, de-duplicated."""
    ids: Dict[str, None] = {}
    for result in api.search("data_source"):
        ids.setdefault(result["id"], None)
    return list(ids)


def fetch_data_source(api: NotionAPI, data_source_id: str) -> DataSourceRecord:
    """Retrieve one data source with its full schema."""
    return parse_data_source(api.retrieve_data_source(data_source_id))


def fetch_database(api: NotionAPI, database_id: str) -> Tuple[DatabaseRecord, List[str]]:
    """Retrieve one original database.

    :returns: The database and the IDs of the data sources it holds.
    """
    obj = api.retrieve_database(database_id)
    return parse_database(obj), [entry["id"] for entry in obj.get("data_sources") or []]


def discover_standalone_pages(api: NotionAPI) -> List[PageRecord]:
    """:returns: Pages found by search that are not data source rows.

    Pages parented by a data source are rows (or database templates) and are
    read through queries instead; pages parented by ``database_id`` are never
    returned by queries, so they are kept here.
    """
    found: Dict[str, PageRecord] = {}
    for result in api.search("page"):
        if result["parent"]["type"] == "data_source_id" or not is_live(result):
            continue
        found.setdefault(result["id"], parse_page(result, is_row=False))
    return list(found.values())


def is_live(obj: Mapping[str, Any]) -> bool:
    """Whether a page is in scope: neither trashed nor archived."""
    return not obj.get("in_trash") and not obj.get("is_archived")


# ---------------------------------------------------------------- rows

def may_be_truncated(value: Mapping[str, Any]) -> bool:
    """Whether a page property value may be missing items.

    ``has_more`` is documented for relations only; for people and for mentions
    in title/rich_text the docs only state the 25-item cap, so reaching it
    also counts.
    """
    if value.get("has_more"):
        return True
    items = value.get(value["type"]) or []
    if value["type"] == "people":
        return len(items) >= PAGE_RESPONSE_ITEM_CAP
    if value["type"] in ("title", "rich_text"):
        return sum(1 for item in items if item.get("type") == "mention") >= PAGE_RESPONSE_ITEM_CAP
    return False


def complete_value(api: NotionAPI, page_id: str, value: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a page property value with every item, re-reading it when truncated.

    Page responses cap ``relation``, ``people`` and ``title``/``rich_text``
    mentions at 25 and give no cursor: a value that may be truncated (see
    :func:`may_be_truncated`) is read from the property item endpoint, from
    its first item.

    :raises SnapshotIntegrityError: ``has_more`` on a non-paginated type, or an
        unexpected property item response.
    """
    if not may_be_truncated(value):
        return value
    prop_type = value["type"]
    if prop_type not in PAGINATED_TYPES:
        raise SnapshotIntegrityError(f"Property {value['id']} of page {page_id} ({prop_type}) has has_more=true")
    items: List[Any] = []
    for response in api.property_items(page_id, value["id"]):
        if response.get("object") != "list":
            raise SnapshotIntegrityError(f"Property {value['id']} of page {page_id} did not return a paginated list")
        for item in response.get("results", []):
            if item.get("type") != prop_type:
                raise SnapshotIntegrityError(
                    f"Property {value['id']} of page {page_id} returned a {item.get('type')!r} item, expected {prop_type!r}"
                )
            items.append(item[prop_type])
    return {**value, prop_type: items, "has_more": False}


def _option_ids(value: Mapping[str, Any]) -> List[str]:
    raw = value.get(value["type"])
    if value["type"] in SELECT_TYPES:
        return [raw["id"]] if raw else []
    if value["type"] == "multi_select":
        return [option["id"] for option in raw or []]
    return []


def schema_mismatches(obj: Mapping[str, Any], data_source: DataSourceRecord) -> List[str]:
    """List how a row disagrees with the schema read earlier (the schema can
    change while the snapshot runs).

    :returns: Unknown property IDs, changed types and unknown option IDs.
    """
    mismatches = []
    for value in obj["properties"].values():
        prop = data_source.property(value["id"])
        if prop is None:
            mismatches.append(f"unknown property {value['id']}")
        elif prop.type != value["type"]:
            mismatches.append(f"property {value['id']} is {value['type']}, schema says {prop.type}")
        else:
            known = {option.option_id for option in prop.options}
            mismatches.extend(
                f"unknown option {option_id} of property {value['id']}"
                for option_id in _option_ids(value)
                if option_id not in known
            )
    return mismatches


def parse_row(api: NotionAPI, obj: Mapping[str, Any], data_source: DataSourceRecord) -> RowRecord:
    """Convert a row page into its page record and complete column values (by property ID).

    :raises SnapshotIntegrityError: The row does not match ``data_source``'s schema.
    """
    mismatches = schema_mismatches(obj, data_source)
    if mismatches:
        raise SnapshotIntegrityError(
            f"Row {obj['id']} does not match the schema of data source {data_source.data_source_id}: "
            + "; ".join(mismatches)
        )
    values: Dict[str, Any] = {}
    for value in obj["properties"].values():
        prop = data_source.property(value["id"])
        if column_type(prop.type) is None:
            continue
        values[prop.property_id] = column_value(complete_value(api, obj["id"], value))
    return RowRecord(page=parse_page(obj, is_row=True), values=values)


@dataclass(frozen=True)
class QueryBatch:
    """One query response page: new rows, and child data sources (wikis) seen in it."""

    rows: List[Mapping[str, Any]]
    child_data_source_ids: List[str]


def query_rows(api: NotionAPI, data_source_id: str) -> Iterator[QueryBatch]:
    """Yield every row page of a data source, one batch per query response page.

    A single query stops at 10,000 results. Queries sort by ``created_time``
    ascending; whenever one is cut off, the next starts ``on_or_after`` the
    last ``created_time`` seen. Results are de-duplicated by ID. Non-page
    results (child data sources of wikis) advance the window and are reported
    separately; they are not rows.

    :raises SnapshotIntegrityError: A window cannot advance (more than 10,000
        results share one ``created_time`` minute), or a query is incomplete
        for another reason.
    """
    seen: set = set()
    window_start: Optional[str] = None
    while True:
        body: Dict[str, Any] = {"sorts": CREATED_TIME_ASCENDING}
        if window_start is not None:
            body["filter"] = {"timestamp": "created_time", "created_time": {"on_or_after": window_start}}
        limit_reached = False
        last_created: Optional[str] = None
        for response in api.query_data_source(data_source_id, body):
            status = response.get("request_status") or {}
            if status.get("type") == "incomplete":
                if status.get("incomplete_reason") != RESULT_LIMIT_REASON:
                    raise SnapshotIntegrityError(
                        f"Query of data source {data_source_id} incomplete: {status.get('incomplete_reason')!r}"
                    )
                limit_reached = True
            results = response.get("results", [])
            for result in results:
                if last_created is None or result["created_time"] > last_created:
                    last_created = result["created_time"]
            fresh = [result for result in results if result["id"] not in seen]
            seen.update(result["id"] for result in fresh)
            batch = QueryBatch(
                rows=[result for result in fresh if result["object"] == "page"],
                child_data_source_ids=[result["id"] for result in fresh if result["object"] == "data_source"],
            )
            if batch.rows or batch.child_data_source_ids:
                yield batch
        if not limit_reached:
            return
        if last_created is None or last_created == window_start:
            raise SnapshotIntegrityError(
                f"Data source {data_source_id}: more than the per-query result limit share created_time "
                f"{last_created}; the 10,000-row window cannot advance"
            )
        window_start = last_created


# ---------------------------------------------------------------- views

def list_view_ids(api: NotionAPI, data_source_ids: Iterable[str], database_ids: Iterable[str]) -> List[str]:
    """List view IDs, de-duplicated.

    Listing by data source includes linked views elsewhere in the workspace;
    listing by (original) database includes dashboards, which have no data source.

    :param data_source_ids: Data sources to list views for.
    :param database_ids: Original databases to list views for.
    """
    view_ids: Dict[str, None] = {}
    for data_source_id in data_source_ids:
        view_ids.update((view_id, None) for view_id in api.list_views(data_source_id=data_source_id))
    for database_id in database_ids:
        view_ids.update((view_id, None) for view_id in api.list_views(database_id=database_id))
    return list(view_ids)


def fetch_view(api: NotionAPI, view_id: str) -> ViewRecord:
    """Retrieve one view."""
    return parse_view(api.retrieve_view(view_id))


def fetch_views(api: NotionAPI, data_source_ids: Iterable[str], database_ids: Iterable[str]) -> List[ViewRecord]:
    """Retrieve every view listed for the given data sources and original databases, once each."""
    return [fetch_view(api, view_id) for view_id in list_view_ids(api, data_source_ids, database_ids)]


def linked_database_from_block(block: BlockRecord) -> DatabaseRecord:
    """A linked database container from its ``child_database`` block: ID, title and location."""
    parent = Parent("block_id", block.parent_block_id) if block.parent_block_id else Parent("page_id", block.page_id)
    return DatabaseRecord(
        database_id=block.block_id,
        is_linked=True,
        block_title=(block.content or {}).get("title"),
        parent=parent,
    )


# ---------------------------------------------------------------- bodies

def is_captured(block: Mapping[str, Any]) -> bool:
    """Whether a block is stored: media and embed blocks are not.

    An embed reads back with only a ``url``, a temporary signed link when it
    is backed by an uploaded file, so hosted and external embeds cannot be
    told apart.
    """
    return block["type"] not in UNCAPTURED_BLOCK_TYPES


def _block_content(block: Mapping[str, Any]) -> Dict[str, Any]:
    content = dict(block.get(block["type"]) or {})
    if "icon" in content:
        content["icon"] = sanitize_icon(content["icon"])
    return content


def walks_into(block: Mapping[str, Any]) -> bool:
    """Whether a block's children belong to this body.

    Child pages and child databases have bodies of their own, synced-block
    copies only point at their original, and the content of unsupported
    blocks is not exposed.
    """
    if not block.get("has_children") or block["type"] in UNWALKED_BLOCK_TYPES:
        return False
    if block["type"] == "synced_block":
        return (block.get("synced_block") or {}).get("synced_from") is None
    return True


def fetch_page_body(api: NotionAPI, page_id: str) -> List[BlockRecord]:
    """Walk a page body iteratively (explicit stack) and return its blocks.

    :raises SnapshotIntegrityError: Nesting deeper than ``MAX_BLOCK_DEPTH``.
    """
    records: List[BlockRecord] = []
    stack: List[Tuple[str, Optional[str], int]] = [(page_id, None, 0)]
    while stack:
        container_id, parent_block_id, depth = stack.pop()
        for position, block in enumerate(api.block_children(container_id)):
            if not is_captured(block):
                continue
            block_type = block["type"]
            records.append(
                BlockRecord(
                    block_id=block["id"],
                    page_id=page_id,
                    parent_block_id=parent_block_id,
                    position=position,
                    type=block_type,
                    content=_block_content(block),
                )
            )
            if walks_into(block):
                if depth + 1 >= MAX_BLOCK_DEPTH:
                    raise SnapshotIntegrityError(
                        f"Block nesting under page {page_id} exceeds {MAX_BLOCK_DEPTH} levels at block {block['id']}"
                    )
                stack.append((block["id"], block["id"], depth + 1))
    return records


# ---------------------------------------------------------------- references (discovery paths)

RefKind = Literal["page", "block", "database", "original_database", "data_source", "view"]
Ref = Tuple[RefKind, str]
"""A documented path to an entity: ``database`` may be original or linked (resolved
through its views); ``original_database`` is the parent of a data source."""

_PARENT_KINDS: Dict[str, RefKind] = {
    "page_id": "page",
    "block_id": "block",
    "database_id": "database",
    "data_source_id": "data_source",
}
_MENTION_KINDS: Dict[str, RefKind] = {"page": "page", "database": "database"}


def parent_reference(parent: Optional[Parent]) -> Optional[Ref]:
    """:returns: The reference to a parent, or ``None`` (workspace, agent, unknown)."""
    if parent is None or parent.id is None or parent.type not in _PARENT_KINDS:
        return None
    return _PARENT_KINDS[parent.type], parent.id


def mention_references(value: Any) -> Iterator[Ref]:
    """Yield the pages and databases mentioned anywhere in a JSON value (rich text)."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            mention = item.get("mention")
            if item.get("type") == "mention" and isinstance(mention, dict):
                kind = _MENTION_KINDS.get(mention.get("type"))
                target = mention.get(mention.get("type")) if kind else None
                if kind and isinstance(target, dict) and target.get("id"):
                    yield kind, target["id"]
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)


def block_references(block: BlockRecord) -> Iterator[Ref]:
    """Yield what a block leads to: child pages, ``link_to_page`` targets, mentions.

    ``child_database`` blocks are resolved separately (they carry a location).
    """
    if block.type == "child_page":
        yield "page", block.block_id
    if block.type == "link_to_page":
        target_type = block.content.get("type")
        if target_type in _PARENT_KINDS and block.content.get(target_type):
            yield _PARENT_KINDS[target_type], block.content[target_type]
    yield from mention_references(block.content)


def data_source_references(data_source: DataSourceRecord) -> Iterator[Ref]:
    """Yield the containing database (up), relation targets and mentions (sideways)."""
    if data_source.parent.type == "data_source_id":
        yield "data_source", data_source.parent.id
    yield "original_database", data_source.database_id
    for prop in data_source.properties:
        if prop.type == "relation":
            target = (prop.config.get("relation") or {}).get("data_source_id")
            if target:
                yield "data_source", target
    yield from mention_references([data_source.title, data_source.description])


def database_references(database: DatabaseRecord) -> Iterator[Ref]:
    """Yield the parent (up) and mentions in title and description of a database."""
    parent = parent_reference(database.parent)
    if parent is not None:
        yield parent
    yield from mention_references([database.title, database.description])


def page_references(page: PageRecord) -> Iterator[Ref]:
    """Yield the parent (up) and title mentions of a standalone page."""
    parent = parent_reference(page.parent)
    if parent is not None:
        yield parent
    yield from mention_references(page.title)


def view_references(view: ViewRecord) -> Iterator[Ref]:
    """Yield the data source, the dashboard and the widget views a view names."""
    if view.data_source_id:
        yield "data_source", view.data_source_id
    if view.dashboard_view_id:
        yield "view", view.dashboard_view_id
    stack: List[Any] = [view.content]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for widget in item.get("widgets") or []:
                if isinstance(widget, dict) and widget.get("view_id"):
                    yield "view", widget["view_id"]
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)


def fetch_referenced_page(api: NotionAPI, page_id: str) -> Optional[Mapping[str, Any]]:
    """Retrieve a page reached through a discovery path.

    :returns: The page object, or ``None`` if it is not found, trashed or archived.
    """
    obj = api.retrieve_page(page_id, missing_ok=True)
    return obj if obj is not None and is_live(obj) else None
