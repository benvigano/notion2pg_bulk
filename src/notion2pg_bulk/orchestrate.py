"""Snapshot orchestration: Notion workspace → empty PostgreSQL database, in one transaction."""

from __future__ import annotations

import uuid
from collections import deque
from typing import Deque, Dict, List, Optional

from sqlalchemy import Connection, Engine, Table, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from . import fetch, load
from .errors import SnapshotIntegrityError
from .naming import check_column_names, check_workspace_names
from .notion_api import NotionAPI
from .notion_http import NotionHTTP, Plan
from .records import DatabaseRecord, DataSourceRecord, PageRecord
from .target import assert_target_empty
from .telemetry import Telemetry, scope

CONNECT_TIMEOUT_S = 10
STATEMENT_TIMEOUT = "60s"
IDLE_IN_TRANSACTION_TIMEOUT = "40min"


def snapshot(
    notion_token: str,
    database_url: str,
    *,
    plan: Plan = "free",
    row_bodies: bool = True,
    propagation_id: Optional[str] = None,
) -> None:
    """Snapshot the Notion workspace into an empty PostgreSQL database.

    Everything (DDL and data) is written in a single transaction: on any error
    it rolls back, the target stays empty, and the error propagates.

    :param notion_token: Notion integration token.
    :param database_url: SQLAlchemy URL of the empty target database.
    :param plan: Notion workspace plan (``free``, ``plus``, ``business``,
        ``enterprise``); sets the request budget.
    :param row_bodies: Whether to fetch the body (blocks) of every data source row.
    :param propagation_id: Identifier carried by every telemetry event;
        generated when omitted.
    :raises TargetNotEmptyError: The target holds user objects (raised before
        any Notion request).
    :raises Notion2PgError: Any other failure of the snapshot.
    """
    telemetry = Telemetry(propagation_id or str(uuid.uuid4()))
    engine = create_target_engine(database_url)
    try:
        with NotionHTTP(notion_token, plan, telemetry) as http:
            run(engine, NotionAPI(http), row_bodies=row_bodies)
    finally:
        engine.dispose()


def create_target_engine(database_url: str) -> Engine:
    """Create the engine for the target, with psycopg 3 and a connect timeout.

    :param database_url: SQLAlchemy URL; a bare ``postgresql://`` URL uses psycopg 3.
    """
    url = make_url(database_url)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return create_engine(url, poolclass=NullPool, connect_args={"connect_timeout": CONNECT_TIMEOUT_S})


def run(engine: Engine, api: NotionAPI, *, row_bodies: bool) -> None:
    """Run the snapshot in one transaction on ``engine``.

    :param engine: Engine of the empty target database.
    :param api: Notion API access.
    :param row_bodies: Whether to fetch data source row bodies.
    """
    with engine.begin() as conn:
        assert_target_empty(conn)
        conn.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'"))
        conn.execute(text(f"SET LOCAL idle_in_transaction_session_timeout = '{IDLE_IN_TRANSACTION_TIMEOUT}'"))
        _Snapshot(conn, api, row_bodies).run()


class _Snapshot:
    def __init__(self, conn: Connection, api: NotionAPI, row_bodies: bool):
        self.conn = conn
        self.api = api
        self.row_bodies = row_bodies
        self.data_sources: Dict[str, DataSourceRecord] = {}
        self.databases: Dict[str, DatabaseRecord] = {}
        self.row_tables: Dict[str, Table] = {}
        self.standalone_pages: Dict[str, PageRecord] = {}
        self.row_ids: Dict[str, List[str]] = {}

    def run(self) -> None:
        with scope(phase="discovery"):
            self._discover()
        with scope(phase="schema"):
            self._load_schema()
        with scope(phase="rows"):
            for data_source in list(self.data_sources.values()):
                with scope(entity_type="data_source", entity_id=data_source.data_source_id):
                    self._load_rows(data_source)
        with scope(phase="views"):
            self._load_views()
        with scope(phase="bodies"):
            self._load_bodies()

    def _discover(self) -> None:
        pending = fetch.discover_data_source_ids(self.api)
        while pending:
            for data_source_id in pending:
                with scope(entity_type="data_source", entity_id=data_source_id):
                    self.data_sources[data_source_id] = fetch.fetch_data_source(self.api, data_source_id)
            listed: Dict[str, None] = {}
            for database_id in fetch.database_ids_of(self.data_sources.values()):
                if database_id in self.databases:
                    continue
                with scope(entity_type="database", entity_id=database_id):
                    self.databases[database_id], held = fetch.fetch_database(self.api, database_id)
                listed.update((data_source_id, None) for data_source_id in held)
            pending = [data_source_id for data_source_id in listed if data_source_id not in self.data_sources]
        for page in fetch.discover_standalone_pages(self.api):
            self.standalone_pages[page.page_id] = page
        check_workspace_names(self.data_sources.values())

    def _load_schema(self) -> None:
        load.create_shared_schema(self.conn)
        load.load_databases(self.conn, self.databases.values())
        for data_source in self.data_sources.values():
            self.row_tables[data_source.data_source_id] = load.load_data_source(self.conn, data_source)
        load.load_pages(self.conn, self.standalone_pages.values())

    def _load_rows(self, data_source: DataSourceRecord) -> None:
        ds_id = data_source.data_source_id
        ids = self.row_ids.setdefault(ds_id, [])
        for objects in fetch.query_rows(self.api, ds_id):
            if any(fetch.schema_mismatches(obj, data_source) for obj in objects):
                data_source = self._refresh_schema(data_source)
            rows = [fetch.parse_row(self.api, obj, data_source) for obj in objects]
            load.load_rows(self.conn, data_source, self.row_tables[ds_id], rows)
            ids.extend(row.page.page_id for row in rows)

    def _refresh_schema(self, data_source: DataSourceRecord) -> DataSourceRecord:
        """Re-read a data source whose rows disagree with its schema, and extend its tables.

        :raises SnapshotIntegrityError: A property changed type, or a new
            property cannot be named.
        """
        merged = data_source.refreshed(fetch.fetch_data_source(self.api, data_source.data_source_id))
        changed = [
            prop.property_id
            for prop in merged.properties
            if (known := data_source.property(prop.property_id)) is not None and known.type != prop.type
        ]
        if changed:
            raise SnapshotIntegrityError(
                f"Data source {data_source.data_source_id}: properties changed type during the snapshot: "
                + ", ".join(changed)
            )
        check_column_names(merged)
        self.data_sources[merged.data_source_id] = merged
        self.row_tables[merged.data_source_id] = load.load_data_source(self.conn, merged)
        return merged

    def _load_views(self) -> None:
        views = fetch.fetch_views(self.api, list(self.data_sources), list(self.databases))
        load.load_views(self.conn, views)
        load.load_databases(self.conn, fetch.linked_databases_from_views(views, self.databases))

    def _load_bodies(self) -> None:
        """Walk page bodies; child pages found on the way are added and walked too."""
        known = set(self.standalone_pages)
        queue: Deque[str] = deque(self.standalone_pages)
        if self.row_bodies:
            for ids in self.row_ids.values():
                queue.extend(ids)
        known.update(row_id for ids in self.row_ids.values() for row_id in ids)
        while queue:
            page_id = queue.popleft()
            with scope(entity_type="page", entity_id=page_id):
                blocks = fetch.fetch_page_body(self.api, page_id)
                load.load_page_body(self.conn, page_id, blocks)
                linked = [
                    fetch.linked_database_from_block(block)
                    for block in blocks
                    if block.type == "child_database" and block.block_id not in self.databases
                ]
                load.load_databases(self.conn, linked)
                for block in blocks:
                    if block.type == "child_page" and block.block_id not in known:
                        known.add(block.block_id)
                        with scope(entity_type="page", entity_id=block.block_id):
                            child = fetch.fetch_child_page(self.api, block.block_id)
                        if child is not None:
                            load.load_pages(self.conn, [child])
                            queue.append(child.page_id)
