"""PostgreSQL schema: shared tables in ``notion``, one row table per data source in ``rows``."""

from __future__ import annotations

from typing import Iterable, Tuple

from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Index, Integer, MetaData, Table, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from .mapping import column_type, json_type
from .records import DataSourceRecord, PropertyRecord

SHARED_SCHEMA = "notion"
ROWS_SCHEMA = "rows"
ROW_ID_COLUMN = "_page_id"

_UUID = UUID(as_uuid=False)

metadata = MetaData(schema=SHARED_SCHEMA)

catalog_tables = Table(
    "catalog_tables",
    metadata,
    Column("data_source_id", _UUID, primary_key=True),
    Column("table_name", Text, nullable=False, unique=True),
)

catalog_columns = Table(
    "catalog_columns",
    metadata,
    Column("data_source_id", _UUID, primary_key=True),
    Column("property_id", Text, primary_key=True),
    Column("column_name", Text, nullable=False),
    UniqueConstraint("data_source_id", "column_name"),
)

databases = Table(
    "databases",
    metadata,
    Column("database_id", _UUID, primary_key=True),
    Column("is_linked", Boolean, nullable=False),
    Column("title", json_type()),
    Column("block_title", Text),
    Column("description", json_type()),
    Column("icon", json_type()),
    Column("parent_type", Text),
    Column("parent_id", _UUID),
    Column("is_inline", Boolean),
    Column("is_locked", Boolean),
    Column("show_data_source_title", Boolean),
)

data_sources = Table(
    "data_sources",
    metadata,
    Column("data_source_id", _UUID, primary_key=True),
    Column("database_id", _UUID, nullable=False),
    Column("parent_type", Text, nullable=False),
    Column("parent_id", _UUID, nullable=False),
    Column("title", json_type(), nullable=False),
    Column("description", json_type()),
    Column("icon", json_type()),
    Column("sync_metadata", json_type()),
)

properties = Table(
    "properties",
    metadata,
    Column("data_source_id", _UUID, primary_key=True),
    Column("property_id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("type", Text, nullable=False),
    Column("description", Text),
    Column("config", json_type(), nullable=False),
)

options = Table(
    "options",
    metadata,
    Column("data_source_id", _UUID, primary_key=True),
    Column("property_id", Text, primary_key=True),
    Column("option_id", Text, primary_key=True),
    Column("position", Integer, nullable=False),
    Column("name", Text, nullable=False),
    Column("color", Text),
    Column("description", Text),
    Column("status_group_id", Text),
    Column("status_group_name", Text),
    ForeignKeyConstraint(
        ["data_source_id", "property_id"],
        [properties.c.data_source_id, properties.c.property_id],
        ondelete="CASCADE",
    ),
)

views = Table(
    "views",
    metadata,
    Column("view_id", _UUID, primary_key=True),
    Column("database_id", _UUID, nullable=False),
    Column("data_source_id", _UUID),
    Column("dashboard_view_id", _UUID),
    Column("name", Text),
    Column("type", Text, nullable=False),
    Column("content", json_type(), nullable=False),
)

pages = Table(
    "pages",
    metadata,
    Column("page_id", _UUID, primary_key=True),
    Column("parent_type", Text, nullable=False),
    Column("parent_id", _UUID),
    Column("title", json_type()),
    Column("icon", json_type()),
    Column("is_locked", Boolean),
    Column("public_url", Text),
)

blocks = Table(
    "blocks",
    metadata,
    Column("block_id", _UUID, primary_key=True),
    Column("page_id", _UUID, nullable=False),
    Column("parent_block_id", _UUID),
    Column("position", Integer, nullable=False),
    Column("type", Text, nullable=False),
    Column("content", json_type(), nullable=False),
    Index("blocks_page_id_idx", "page_id"),
    Index("blocks_parent_block_id_idx", "parent_block_id"),
)


def property_columns(data_source: DataSourceRecord) -> Iterable[Tuple[PropertyRecord, Column]]:
    """Yield each column-bearing property with its column, in schema order."""
    for prop in data_source.properties:
        sql_type = column_type(prop.type)
        if sql_type is not None:
            yield prop, Column(prop.column_name, sql_type)


def row_table(data_source: DataSourceRecord) -> Table:
    """Build the row table of a data source: ``_page_id`` plus one column per property.

    :param data_source: Data source with its current schema.
    :returns: A table bound to its own :class:`MetaData`.
    """
    columns = [column for _, column in property_columns(data_source)]
    return Table(
        data_source.table_name,
        MetaData(),
        Column(ROW_ID_COLUMN, _UUID, primary_key=True),
        *columns,
        schema=ROWS_SCHEMA,
    )
