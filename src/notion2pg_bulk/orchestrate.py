"""Snapshot orchestration: Notion workspace → empty PostgreSQL database, in one transaction."""

from __future__ import annotations

import uuid
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import Connection, Engine, Table, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from . import fetch, json_codec, load
from .errors import SnapshotIntegrityError
from .naming import check_column_names, check_workspace_names
from .notion_api import NotionAPI
from .notion_http import NotionHTTP, Plan
from .records import BlockRecord, DatabaseRecord, DataSourceRecord, PageRecord, ViewRecord
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
    return create_engine(
        url,
        poolclass=NullPool,
        connect_args={"connect_timeout": CONNECT_TIMEOUT_S},
        json_serializer=json_codec.dumps,
    )


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
        self.linked: Dict[str, DatabaseRecord] = {}
        self.views: Dict[str, ViewRecord] = {}
        self.row_tables: Dict[str, Table] = {}
        self.standalone_pages: Dict[str, PageRecord] = {}
        self.row_ids: Dict[str, List[str]] = {}

    def run(self) -> None:
        with scope(phase="discovery"):
            self._add_data_sources(fetch.discover_data_source_ids(self.api))
            for page in fetch.discover_standalone_pages(self.api):
                self.standalone_pages[page.page_id] = page
            check_workspace_names(self.data_sources.values())
        with scope(phase="schema"):
            load.create_shared_schema(self.conn)
            self._load_schema(list(self.databases.values()), list(self.data_sources.values()))
            load.load_pages(self.conn, self.standalone_pages.values())
        with scope(phase="rows"):
            for data_source in list(self.data_sources.values()):
                self._load_rows(data_source)
        with scope(phase="views"):
            self._load_views(fetch.list_view_ids(self.api, list(self.data_sources), list(self.databases)))
        with scope(phase="bodies"):
            self._load_bodies()

    def _add_data_sources(self, data_source_ids: Iterable[str]) -> Tuple[List[DataSourceRecord], List[DatabaseRecord]]:
        """Fetch data sources and their databases, following each database's data source list.

        :returns: The data sources and databases that were not known yet.
        """
        new_sources: List[DataSourceRecord] = []
        new_databases: List[DatabaseRecord] = []
        pending = [data_source_id for data_source_id in dict.fromkeys(data_source_ids) if data_source_id not in self.data_sources]
        while pending:
            for data_source_id in pending:
                with scope(entity_type="data_source", entity_id=data_source_id):
                    record = fetch.fetch_data_source(self.api, data_source_id)
                self.data_sources[data_source_id] = record
                new_sources.append(record)
            listed: Dict[str, None] = {}
            for database_id in fetch.database_ids_of(new_sources):
                if database_id in self.databases:
                    continue
                with scope(entity_type="database", entity_id=database_id):
                    database, held = fetch.fetch_database(self.api, database_id)
                self.databases[database_id] = database
                self.linked.pop(database_id, None)
                new_databases.append(database)
                listed.update((data_source_id, None) for data_source_id in held)
            pending = [data_source_id for data_source_id in listed if data_source_id not in self.data_sources]
        return new_sources, new_databases

    def _load_schema(self, databases: List[DatabaseRecord], data_sources: List[DataSourceRecord]) -> None:
        load.load_databases(self.conn, databases)
        for data_source in data_sources:
            self.row_tables[data_source.data_source_id] = load.load_data_source(self.conn, data_source)

    def _load_rows(self, data_source: DataSourceRecord) -> None:
        ds_id = data_source.data_source_id
        ids = self.row_ids.setdefault(ds_id, [])
        with scope(entity_type="data_source", entity_id=ds_id):
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

    def _load_views(self, view_ids: Iterable[str]) -> None:
        """Retrieve and load the views not fetched yet, and record the linked
        containers they reveal (by ID only, never replacing a block-derived record).
        """
        fetched = [fetch.fetch_view(self.api, view_id) for view_id in dict.fromkeys(view_ids) if view_id not in self.views]
        self.views.update((view.view_id, view) for view in fetched)
        load.load_views(self.conn, fetched)
        revealed = [
            record
            for record in fetch.linked_databases_from_views(fetched, self.databases)
            if record.database_id not in self.linked
        ]
        self._record_linked(revealed)

    def _record_linked(self, records: List[DatabaseRecord]) -> None:
        self.linked.update((record.database_id, record) for record in records)
        load.load_databases(self.conn, records)

    def _resolve_container(self, block: BlockRecord) -> List[str]:
        """Resolve a ``child_database`` block whose ID is not a known database.

        :returns: Row IDs of data sources discovered through it.
        """
        # Search is not a reliable inventory and can miss data sources, so an unknown
        # child_database may be an original database rather than a linked one: its
        # views name its data sources. If they are all known, the block is linked.
        with scope(phase="views", entity_type="database", entity_id=block.block_id):
            listed = self.api.list_views(database_id=block.block_id)
            self._load_views(listed)
        unknown = [
            view.data_source_id
            for view_id in listed
            if (view := self.views[view_id]).data_source_id is not None and view.data_source_id not in self.data_sources
        ]
        new_row_ids: List[str] = []
        if unknown:
            with scope(phase="discovery"):
                sources, databases = self._add_data_sources(unknown)
                check_workspace_names(self.data_sources.values())
            with scope(phase="schema"):
                self._load_schema(databases, sources)
            with scope(phase="rows"):
                for source in sources:
                    self._load_rows(source)
                    new_row_ids.extend(self.row_ids[source.data_source_id])
            with scope(phase="views"):
                self._load_views(
                    fetch.list_view_ids(
                        self.api,
                        [source.data_source_id for source in sources],
                        [database.database_id for database in databases],
                    )
                )
        if block.block_id not in self.databases:
            self._record_linked([fetch.linked_database_from_block(block)])
        return new_row_ids

    def _load_bodies(self) -> None:
        """Walk page bodies; child pages and data sources found on the way are added too."""
        known = set(self.standalone_pages)
        queue: Deque[str] = deque(self.standalone_pages)
        if self.row_bodies:
            for ids in self.row_ids.values():
                queue.extend(ids)
        known.update(row_id for ids in self.row_ids.values() for row_id in ids)
        resolved: set = set()
        while queue:
            page_id = queue.popleft()
            with scope(entity_type="page", entity_id=page_id):
                blocks = fetch.fetch_page_body(self.api, page_id)
                load.load_page_body(self.conn, page_id, blocks)
            for block in blocks:
                if block.type == "child_database" and block.block_id not in self.databases and block.block_id not in resolved:
                    resolved.add(block.block_id)
                    row_ids = self._resolve_container(block)
                    known.update(row_ids)
                    if self.row_bodies:
                        queue.extend(row_ids)
                elif block.type == "child_page" and block.block_id not in known:
                    known.add(block.block_id)
                    with scope(entity_type="page", entity_id=block.block_id):
                        child = fetch.fetch_child_page(self.api, block.block_id)
                    if child is not None:
                        load.load_pages(self.conn, [child])
                        queue.append(child.page_id)
