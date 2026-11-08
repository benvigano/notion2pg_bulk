"""Records: the snapshot's Notion-independent representation of each entity.

Fetch functions produce these records from API objects; load functions write
them to PostgreSQL.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, Mapping, Optional, Tuple

from .errors import SnapshotIntegrityError

JSON = Any


@dataclass(frozen=True)
class Parent:
    """Parent reference stored as type plus Notion ID (``None`` for the workspace)."""

    type: str
    id: Optional[str]


@dataclass(frozen=True)
class DatabaseRecord:
    """A database (data source container), original or linked."""

    database_id: str
    is_linked: bool
    title: JSON = None
    block_title: Optional[str] = None
    description: JSON = None
    icon: JSON = None
    parent: Optional[Parent] = None
    is_inline: Optional[bool] = None
    is_locked: Optional[bool] = None
    show_data_source_title: Optional[bool] = None


@dataclass(frozen=True)
class OptionRecord:
    """A select, multi-select or status option."""

    option_id: str
    position: int
    name: str
    color: Optional[str]
    description: Optional[str]
    status_group_id: Optional[str]
    status_group_name: Optional[str]


@dataclass(frozen=True)
class PropertyRecord:
    """A data source property (schema entry) with its full configuration.

    ``column_name`` is the property name when the snapshot first saw it; it
    stays fixed if the property is renamed while the snapshot runs.
    """

    property_id: str
    name: str
    type: str
    description: Optional[str]
    config: Mapping[str, JSON]
    options: Tuple[OptionRecord, ...]
    column_name: str


@dataclass(frozen=True)
class DataSourceRecord:
    """A data source with its schema."""

    data_source_id: str
    database_id: str
    parent: Parent
    title: JSON
    table_name: str
    description: JSON
    icon: JSON
    sync_metadata: JSON
    properties: Tuple[PropertyRecord, ...]

    def property(self, property_id: str) -> Optional[PropertyRecord]:
        """:returns: The property with ``property_id``, if any."""
        for prop in self.properties:
            if prop.property_id == property_id:
                return prop
        return None

    def refreshed(self, fresh: "DataSourceRecord") -> "DataSourceRecord":
        """Adopt the schema of ``fresh`` while keeping the table and column names
        already assigned (Notion renames during a run do not move data) and the
        properties deleted meanwhile (earlier rows hold values for them).
        """
        fresh_ids = {prop.property_id for prop in fresh.properties}
        properties = tuple(
            replace(prop, column_name=known.column_name) if (known := self.property(prop.property_id)) else prop
            for prop in fresh.properties
        ) + tuple(prop for prop in self.properties if prop.property_id not in fresh_ids)
        return replace(fresh, table_name=self.table_name, properties=properties)


@dataclass(frozen=True)
class PageRecord:
    """A page: standalone, or a data source row (``title`` is then ``None``)."""

    page_id: str
    parent: Parent
    title: JSON
    icon: JSON
    is_locked: Optional[bool]
    public_url: Optional[str]


@dataclass(frozen=True)
class RowRecord:
    """A data source row: its page plus one column value per property ID."""

    page: PageRecord
    values: Mapping[str, JSON]


@dataclass(frozen=True)
class BlockRecord:
    """A block of a page body."""

    block_id: str
    page_id: str
    parent_block_id: Optional[str]
    position: int
    type: str
    content: JSON


@dataclass(frozen=True)
class ViewRecord:
    """A database view (dashboards and their widgets included)."""

    view_id: str
    database_id: str
    data_source_id: Optional[str]
    dashboard_view_id: Optional[str]
    name: Optional[str]
    type: str
    content: Mapping[str, JSON]


# ---------------------------------------------------------------- parsing helpers

HOSTED_FILE_ICON_TYPES = frozenset({"file", "file_upload"})
# request_id is part of the response, not of the view object, and differs on every
# response: keeping it would make two snapshots of an unchanged view differ.
VIEW_EXCLUDED_KEYS = frozenset(
    {"object", "id", "parent", "url", "created_time", "created_by", "last_edited_time", "last_edited_by", "request_id"}
)


def plain_text(rich_text: Optional[list]) -> str:
    """:returns: The concatenated ``plain_text`` of a rich-text array."""
    return "".join(item.get("plain_text", "") for item in rich_text or [])


def parse_parent(parent: Mapping[str, Any]) -> Parent:
    """Convert an API parent object.

    :raises SnapshotIntegrityError: The parent type carries no ID field.
    """
    parent_type = parent["type"]
    if parent_type == "workspace":
        return Parent("workspace", None)
    if parent_type not in parent:
        raise SnapshotIntegrityError(f"Parent of type {parent_type!r} has no {parent_type!r} field")
    return Parent(parent_type, parent[parent_type])


def sanitize_icon(icon: Optional[Mapping[str, Any]]) -> JSON:
    """Drop Notion-hosted file icons, keep every other icon type as returned."""
    if not icon or icon.get("type") in HOSTED_FILE_ICON_TYPES:
        return None
    return dict(icon)


def parse_options(prop: Mapping[str, Any]) -> Tuple[OptionRecord, ...]:
    """:returns: The options of a select, multi-select or status property."""
    prop_type = prop["type"]
    if prop_type not in ("select", "multi_select", "status"):
        return ()
    config = prop.get(prop_type) or {}
    groups: Dict[str, Mapping[str, Any]] = {}
    for group in config.get("groups") or []:
        for option_id in group.get("option_ids") or []:
            groups[option_id] = group
    return tuple(
        OptionRecord(
            option_id=option["id"],
            position=position,
            name=option["name"],
            color=option.get("color"),
            description=option.get("description"),
            status_group_id=groups.get(option["id"], {}).get("id"),
            status_group_name=groups.get(option["id"], {}).get("name"),
        )
        for position, option in enumerate(config.get("options") or [])
    )


def parse_data_source(obj: Mapping[str, Any]) -> DataSourceRecord:
    """Convert a retrieved data source object."""
    parent = parse_parent(obj["parent"])
    database_id = obj["parent"].get("database_id")
    if database_id is None:
        raise SnapshotIntegrityError(f"Data source {obj['id']} has no containing database_id")
    properties = tuple(
        PropertyRecord(
            property_id=prop["id"],
            name=name,
            type=prop["type"],
            description=prop.get("description"),
            config=dict(prop),
            options=parse_options(prop),
            column_name=name,
        )
        for name, prop in obj["properties"].items()
    )
    return DataSourceRecord(
        data_source_id=obj["id"],
        database_id=database_id,
        parent=parent,
        title=obj.get("title") or [],
        table_name=plain_text(obj.get("title")),
        description=obj.get("description") or None,
        icon=sanitize_icon(obj.get("icon")),
        sync_metadata=obj.get("sync_metadata"),
        properties=properties,
    )


def parse_database(obj: Mapping[str, Any]) -> DatabaseRecord:
    """Convert a retrieved (original) database object."""
    return DatabaseRecord(
        database_id=obj["id"],
        is_linked=False,
        title=obj.get("title") or None,
        description=obj.get("description") or None,
        icon=sanitize_icon(obj.get("icon")),
        parent=parse_parent(obj["parent"]),
        is_inline=obj.get("is_inline"),
        is_locked=obj.get("is_locked"),
        show_data_source_title=obj.get("show_data_source_title"),
    )


def page_title(obj: Mapping[str, Any]) -> JSON:
    """:returns: The rich-text title of a page (its ``title``-typed property)."""
    for value in (obj.get("properties") or {}).values():
        if value.get("type") == "title":
            return value.get("title") or None
    return None


def parse_page(obj: Mapping[str, Any], is_row: bool) -> PageRecord:
    """Convert a page object.

    :param obj: Page object.
    :param is_row: Whether the page is a data source row (its title then lives
        in the row table).
    """
    return PageRecord(
        page_id=obj["id"],
        parent=parse_parent(obj["parent"]),
        title=None if is_row else page_title(obj),
        icon=sanitize_icon(obj.get("icon")),
        is_locked=obj.get("is_locked"),
        public_url=obj.get("public_url"),
    )


def parse_view(obj: Mapping[str, Any]) -> ViewRecord:
    """Convert a retrieved view object."""
    return ViewRecord(
        view_id=obj["id"],
        database_id=obj["parent"]["database_id"],
        data_source_id=obj.get("data_source_id"),
        dashboard_view_id=obj.get("dashboard_view_id"),
        name=obj.get("name"),
        type=obj["type"],
        content={key: value for key, value in obj.items() if key not in VIEW_EXCLUDED_KEYS},
    )
