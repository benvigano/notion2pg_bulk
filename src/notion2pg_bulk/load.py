"""Load records into PostgreSQL.

Every load function is an idempotent upsert: loading the same records again
leaves the database in the same state. Child sets (the options of a
property, the blocks of a page body) are replaced as a whole.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence, TypeVar

from sqlalchemy import Column, Connection, Table, and_, delete, not_, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import CreateSchema, CreateTable

from . import schema as tables
from .records import BlockRecord, DatabaseRecord, DataSourceRecord, PageRecord, RowRecord, ViewRecord
from .schema import ROW_ID_COLUMN, ROWS_SCHEMA, SHARED_SCHEMA, property_columns, row_table

BATCH_SIZE = 500

T = TypeVar("T")


MAX_STATEMENT_PARAMETERS = 65_535


def _batches(items: Sequence[T], parameters_per_item: int) -> Iterator[Sequence[T]]:
    # A multi-row INSERT binds one parameter per row and column, and the PostgreSQL
    # wire protocol caps a statement at 65,535 parameters (a 16-bit count), so wide
    # row tables need smaller batches than BATCH_SIZE.
    size = max(1, min(BATCH_SIZE, MAX_STATEMENT_PARAMETERS // max(1, parameters_per_item)))
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _upsert(conn: Connection, table: Table, rows: Sequence[Mapping[str, Any]]) -> None:
    """Insert ``rows`` in bounded batches, updating every non-key column on conflict."""
    if not rows:
        return
    keys = [column.name for column in table.primary_key.columns]
    for batch in _batches(rows, len(table.columns)):
        statement = insert(table).values(list(batch))
        updates = {column.name: statement.excluded[column.name] for column in table.columns if column.name not in keys}
        if updates:
            statement = statement.on_conflict_do_update(index_elements=keys, set_=updates)
        else:
            statement = statement.on_conflict_do_nothing(index_elements=keys)
        conn.execute(statement)


def create_shared_schema(conn: Connection) -> None:
    """Create the ``notion`` and ``rows`` schemas and the shared tables (if absent)."""
    conn.execute(CreateSchema(SHARED_SCHEMA, if_not_exists=True))
    conn.execute(CreateSchema(ROWS_SCHEMA, if_not_exists=True))
    tables.metadata.create_all(conn, checkfirst=True)


# ---------------------------------------------------------------- databases and data sources

def _database_row(record: DatabaseRecord) -> Dict[str, Any]:
    return {
        "database_id": record.database_id,
        "is_linked": record.is_linked,
        "title": record.title,
        "block_title": record.block_title,
        "description": record.description,
        "icon": record.icon,
        "parent_type": record.parent.type if record.parent else None,
        "parent_id": record.parent.id if record.parent else None,
        "is_inline": record.is_inline,
        "is_locked": record.is_locked,
        "show_data_source_title": record.show_data_source_title,
    }


def load_databases(conn: Connection, records: Iterable[DatabaseRecord]) -> None:
    """Upsert databases (original or linked)."""
    _upsert(conn, tables.databases, [_database_row(record) for record in records])


def _ensure_row_table(conn: Connection, data_source: DataSourceRecord) -> Table:
    """Create the row table, or add the columns it lacks; return the full table."""
    table = row_table(data_source)
    if not conn.dialect.has_table(conn, table.name, schema=ROWS_SCHEMA):
        conn.execute(CreateTable(table))
        return table
    existing = {column["name"] for column in conn.dialect.get_columns(conn, table.name, schema=ROWS_SCHEMA)}
    for column in table.columns:
        if column.name not in existing:
            _add_column(conn, table, column)
    return table


def _add_column(conn: Connection, table: Table, column: Column) -> None:
    # SQLAlchemy Core has no ALTER TABLE ... ADD COLUMN construct; identifiers are
    # quoted by the dialect's preparer and the type is compiled by the dialect.
    preparer = conn.dialect.identifier_preparer
    conn.exec_driver_sql(
        f"ALTER TABLE {preparer.format_table(table)} "
        f"ADD COLUMN {preparer.format_column(column)} {column.type.compile(dialect=conn.dialect)}"
    )


def load_data_source(conn: Connection, data_source: DataSourceRecord) -> Table:
    """Upsert a data source with its properties, options and catalog entries,
    and create or extend its row table.

    :returns: The row table matching the data source's current schema.
    """
    ds_id = data_source.data_source_id
    _upsert(
        conn,
        tables.data_sources,
        [
            {
                "data_source_id": ds_id,
                "database_id": data_source.database_id,
                "parent_type": data_source.parent.type,
                "parent_id": data_source.parent.id,
                "title": data_source.title,
                "description": data_source.description,
                "icon": data_source.icon,
                "sync_metadata": data_source.sync_metadata,
            }
        ],
    )
    _upsert(conn, tables.catalog_tables, [{"data_source_id": ds_id, "table_name": data_source.table_name}])

    property_ids = [prop.property_id for prop in data_source.properties]
    conn.execute(
        delete(tables.properties).where(
            and_(tables.properties.c.data_source_id == ds_id, not_(tables.properties.c.property_id.in_(property_ids)))
        )
    )
    _upsert(
        conn,
        tables.properties,
        [
            {
                "data_source_id": ds_id,
                "property_id": prop.property_id,
                "name": prop.name,
                "type": prop.type,
                "description": prop.description,
                "config": dict(prop.config),
            }
            for prop in data_source.properties
        ],
    )
    option_keys = [(prop.property_id, option.option_id) for prop in data_source.properties for option in prop.options]
    stale_options = delete(tables.options).where(tables.options.c.data_source_id == ds_id)
    if option_keys:
        stale_options = stale_options.where(
            not_(tuple_(tables.options.c.property_id, tables.options.c.option_id).in_(option_keys))
        )
    conn.execute(stale_options)
    _upsert(
        conn,
        tables.options,
        [
            {
                "data_source_id": ds_id,
                "property_id": prop.property_id,
                "option_id": option.option_id,
                "position": option.position,
                "name": option.name,
                "color": option.color,
                "description": option.description,
                "status_group_id": option.status_group_id,
                "status_group_name": option.status_group_name,
            }
            for prop in data_source.properties
            for option in prop.options
        ],
    )

    columns = list(property_columns(data_source))
    conn.execute(
        delete(tables.catalog_columns).where(
            and_(
                tables.catalog_columns.c.data_source_id == ds_id,
                not_(tables.catalog_columns.c.property_id.in_([prop.property_id for prop, _ in columns])),
            )
        )
    )
    _upsert(
        conn,
        tables.catalog_columns,
        [{"data_source_id": ds_id, "property_id": prop.property_id, "column_name": column.name} for prop, column in columns],
    )
    return _ensure_row_table(conn, data_source)


# ---------------------------------------------------------------- pages and rows

def _page_row(record: PageRecord) -> Dict[str, Any]:
    return {
        "page_id": record.page_id,
        "parent_type": record.parent.type,
        "parent_id": record.parent.id,
        "title": record.title,
        "icon": record.icon,
        "is_locked": record.is_locked,
        "public_url": record.public_url,
    }


def load_pages(conn: Connection, records: Iterable[PageRecord]) -> None:
    """Upsert pages (standalone pages and rows alike)."""
    _upsert(conn, tables.pages, [_page_row(record) for record in records])


def load_rows(conn: Connection, data_source: DataSourceRecord, table: Table, rows: Sequence[RowRecord]) -> None:
    """Upsert data source rows: their pages, and their values in the row table.

    :param data_source: Data source the rows belong to.
    :param table: Its row table (from :func:`load_data_source`).
    :param rows: Rows whose values are keyed by property ID.
    """
    load_pages(conn, [row.page for row in rows])
    column_of = {prop.property_id: column.name for prop, column in property_columns(data_source)}
    values: List[Dict[str, Any]] = []
    for row in rows:
        record = {name: None for name in column_of.values()}
        record[ROW_ID_COLUMN] = row.page.page_id
        for property_id, value in row.values.items():
            record[column_of[property_id]] = value
        values.append(record)
    _upsert(conn, table, values)


# ---------------------------------------------------------------- bodies and views

def load_page_body(conn: Connection, page_id: str, blocks: Sequence[BlockRecord]) -> None:
    """Replace the body of one page with ``blocks``."""
    block_ids = [block.block_id for block in blocks]
    conn.execute(
        delete(tables.blocks).where(
            and_(tables.blocks.c.page_id == page_id, not_(tables.blocks.c.block_id.in_(block_ids)))
        )
    )
    _upsert(
        conn,
        tables.blocks,
        [
            {
                "block_id": block.block_id,
                "page_id": block.page_id,
                "parent_block_id": block.parent_block_id,
                "position": block.position,
                "type": block.type,
                "content": block.content,
            }
            for block in blocks
        ],
    )


def load_views(conn: Connection, records: Iterable[ViewRecord]) -> None:
    """Upsert views."""
    _upsert(
        conn,
        tables.views,
        [
            {
                "view_id": record.view_id,
                "database_id": record.database_id,
                "data_source_id": record.data_source_id,
                "dashboard_view_id": record.dashboard_view_id,
                "name": record.name,
                "type": record.type,
                "content": dict(record.content),
            }
            for record in records
        ],
    )
