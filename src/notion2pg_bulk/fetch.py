"""Fetch: Notion API objects → records."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .errors import SnapshotIntegrityError
from .mapping import PAGINATED_TYPES, column_type, column_value
from .notion_api import NotionAPI
from .records import (
    BlockRecord,
    DatabaseRecord,
    DataSourceRecord,
    PageRecord,
    RowRecord,
    ViewRecord,
    parse_data_source,
    parse_database,
    parse_page,
    parse_view,
)

MAX_BLOCK_DEPTH = 100
CREATED_TIME_ASCENDING = [{"timestamp": "created_time", "direction": "ascending"}]
RESULT_LIMIT_REASON = "query_result_limit_reached"
MEDIA_BLOCK_TYPES = frozenset({"image", "video", "audio", "file", "pdf"})
HOSTED_EMBED_MARKERS = frozenset({"file", "file_upload"})
UNWALKED_BLOCK_TYPES = frozenset({"child_page", "child_database"})
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


def fetch_database(api: NotionAPI, database_id: str) -> DatabaseRecord:
    """Retrieve one original database."""
    return parse_database(api.retrieve_database(database_id))


def discover_standalone_pages(api: NotionAPI) -> List[PageRecord]:
    """:returns: Pages found by search that are not data source rows.

    Pages parented by a data source are rows (or database templates) and are
    read through queries instead; pages parented by ``database_id`` are never
    returned by queries, so they are kept here.
    """
    found: Dict[str, PageRecord] = {}
    for result in api.search("page"):
        if result["parent"]["type"] == "data_source_id":
            continue
        found.setdefault(result["id"], parse_page(result, is_row=False))
    return list(found.values())


# ---------------------------------------------------------------- rows

def complete_value(api: NotionAPI, page_id: str, value: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a page property value with every item, re-reading it when truncated.

    Page responses cap ``relation``, ``people`` and ``title``/``rich_text``
    mentions at 25 and only set ``has_more`` (no cursor): the full value is
    read from the property item endpoint, from its first item.

    :raises SnapshotIntegrityError: ``has_more`` on a non-paginated type, or an
        unexpected property item response.
    """
    if not value.get("has_more"):
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


def query_rows(api: NotionAPI, data_source_id: str) -> Iterator[List[Mapping[str, Any]]]:
    """Yield every row page of a data source, one list per query response page.

    A single query stops at 10,000 results. Queries sort by ``created_time``
    ascending; whenever one is cut off, the next starts ``on_or_after`` the
    last ``created_time`` seen. Rows are de-duplicated by ID. Non-page results
    (child data sources of wikis) advance the window but are not rows.

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
            batch = [result for result in results if result["object"] == "page" and result["id"] not in seen]
            seen.update(result["id"] for result in batch)
            if batch:
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

def fetch_views(api: NotionAPI, data_sources: Sequence[DataSourceRecord]) -> List[ViewRecord]:
    """:returns: Every view listed for the given data sources, retrieved once each."""
    view_ids: Dict[str, None] = {}
    for data_source in data_sources:
        for view_id in api.list_views(data_source_id=data_source.data_source_id):
            view_ids.setdefault(view_id, None)
    return [parse_view(api.retrieve_view(view_id)) for view_id in view_ids]


# ---------------------------------------------------------------- bodies

def is_captured(block: Mapping[str, Any]) -> bool:
    """Whether a block is stored: media blocks and Notion-hosted embeds are not."""
    block_type = block["type"]
    if block_type in MEDIA_BLOCK_TYPES:
        return False
    if block_type == "embed":
        payload = block.get("embed") or {}
        return "file_upload" not in payload and payload.get("type") not in HOSTED_EMBED_MARKERS
    return True


def walks_into(block: Mapping[str, Any]) -> bool:
    """Whether a block's children belong to this body.

    Child pages and child databases have bodies of their own, and synced-block
    copies only point at their original.
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
                    content=block.get(block_type) or {},
                )
            )
            if walks_into(block):
                if depth + 1 >= MAX_BLOCK_DEPTH:
                    raise SnapshotIntegrityError(
                        f"Block nesting under page {page_id} exceeds {MAX_BLOCK_DEPTH} levels at block {block['id']}"
                    )
                stack.append((block["id"], block["id"], depth + 1))
    return records


def database_ids_of(data_sources: Iterable[DataSourceRecord]) -> List[str]:
    """:returns: The containing database IDs, in first-seen order."""
    ids: Dict[str, None] = {}
    for data_source in data_sources:
        ids.setdefault(data_source.database_id, None)
    return list(ids)
