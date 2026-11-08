"""Fetch: Notion API objects → records."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .errors import SnapshotIntegrityError
from .mapping import column_type, column_value
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
    """:returns: Pages found by search that are not data source rows."""
    found: Dict[str, PageRecord] = {}
    for result in api.search("page"):
        if result["parent"]["type"] in ("data_source_id", "database_id"):
            continue
        found.setdefault(result["id"], parse_page(result, is_row=False))
    return list(found.values())


# ---------------------------------------------------------------- rows

def parse_row(obj: Mapping[str, Any], data_source: DataSourceRecord) -> RowRecord:
    """Convert a row page into its page record and column values (by property ID)."""
    values: Dict[str, Any] = {}
    for value in obj["properties"].values():
        prop = data_source.property(value["id"])
        if prop is None or column_type(prop.type) is None:
            continue
        values[prop.property_id] = column_value(value)
    return RowRecord(page=parse_page(obj, is_row=True), values=values)


def fetch_rows(api: NotionAPI, data_source: DataSourceRecord) -> Iterator[List[RowRecord]]:
    """Yield the rows of a data source, one list per query response page."""
    for response in api.query_data_source(data_source.data_source_id, {}):
        yield [parse_row(obj, data_source) for obj in response.get("results", [])]


# ---------------------------------------------------------------- views

def fetch_views(api: NotionAPI, data_sources: Sequence[DataSourceRecord]) -> List[ViewRecord]:
    """:returns: Every view listed for the given data sources, retrieved once each."""
    view_ids: Dict[str, None] = {}
    for data_source in data_sources:
        for view_id in api.list_views(data_source_id=data_source.data_source_id):
            view_ids.setdefault(view_id, None)
    return [parse_view(api.retrieve_view(view_id)) for view_id in view_ids]


# ---------------------------------------------------------------- bodies

def _walks_into(block: Mapping[str, Any]) -> bool:
    return bool(block.get("has_children")) and block["type"] != "child_database"


def fetch_page_body(api: NotionAPI, page_id: str) -> List[BlockRecord]:
    """Walk a page body iteratively (explicit stack) and return its blocks.

    :raises SnapshotIntegrityError: Nesting deeper than ``MAX_BLOCK_DEPTH``.
    """
    records: List[BlockRecord] = []
    stack: List[Tuple[str, Optional[str], int]] = [(page_id, None, 0)]
    while stack:
        container_id, parent_block_id, depth = stack.pop()
        for position, block in enumerate(api.block_children(container_id)):
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
            if _walks_into(block):
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
