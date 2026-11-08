"""Snapshot orchestration: Notion workspace → empty PostgreSQL database, in one transaction."""

from __future__ import annotations

import uuid
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Set

from sqlalchemy import Connection, Engine, Table, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from . import fetch, json_codec, load
from .errors import SnapshotIntegrityError
from .naming import check_column_names, check_workspace_names
from .notion_api import NotionAPI
from .notion_http import NotionHTTP, Plan
from .records import (
    BlockRecord,
    DatabaseRecord,
    DataSourceRecord,
    PageRecord,
    ViewRecord,
    parse_data_source,
    parse_database,
    parse_page,
    parse_parent,
    parse_view,
)
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
    """Discovery and loading, run until nothing new is found.

    Search is only the starting point. Discovery then follows documented paths:
    up (parents), down (page bodies, child pages, the views of child
    databases) and sideways (data sources named by relations, views and
    dashboard widgets; pages and databases named by ``link_to_page`` blocks and
    mentions; child data sources returned by queries). Records found are
    queued for loading; nothing is written until the data sources found by the
    first pass have passed the naming checks.
    """

    def __init__(self, conn: Connection, api: NotionAPI, row_bodies: bool):
        self.conn = conn
        self.api = api
        self.row_bodies = row_bodies
        self.writing = False
        self.data_sources: Dict[str, DataSourceRecord] = {}
        self.databases: Dict[str, DatabaseRecord] = {}
        self.containers: Dict[str, Optional[BlockRecord]] = {}
        self.views: Dict[str, ViewRecord] = {}
        self.pages: Set[str] = set()
        self.rows: Set[str] = set()
        self.row_tables: Dict[str, Table] = {}
        self.attempted: Set[fetch.Ref] = set()
        self.refs: Deque[fetch.Ref] = deque()
        self.unloaded_sources: List[DataSourceRecord] = []
        self.unloaded_databases: List[DatabaseRecord] = []
        self.unloaded_pages: List[PageRecord] = []
        self.unloaded_views: List[ViewRecord] = []
        self.unqueried: Deque[str] = deque()
        self.unlisted_databases: Deque[str] = deque()
        self.bodies: Deque[str] = deque()

    # ------------------------------------------------------------ driver

    def run(self) -> None:
        with scope(phase="discovery"):
            for data_source_id in fetch.discover_data_source_ids(self.api):
                with scope(entity_type="data_source", entity_id=data_source_id):
                    self._accept_data_source(fetch.fetch_data_source(self.api, data_source_id))
            for page in fetch.discover_standalone_pages(self.api):
                self._accept_page(page)
            self._follow_references()
        with scope(phase="schema"):
            load.create_shared_schema(self.conn)
            self.writing = True
            self._flush()
        while self._step():
            self._flush()
        with scope(phase="schema"):
            self._record_containers()

    def _step(self) -> bool:
        """Do the next unit of work; ``False`` when nothing is left."""
        if self.refs:
            with scope(phase="discovery"):
                self._follow_references()
        elif self.unqueried:
            data_source_id = self.unqueried.popleft()
            with scope(phase="rows", entity_type="data_source", entity_id=data_source_id):
                self._load_rows(self.data_sources[data_source_id])
            with scope(phase="views", entity_type="data_source", entity_id=data_source_id):
                self._accept_listed_views(self.api.list_views(data_source_id=data_source_id))
        elif self.unlisted_databases:
            database_id = self.unlisted_databases.popleft()
            with scope(phase="views", entity_type="database", entity_id=database_id):
                self._accept_listed_views(self.api.list_views(database_id=database_id))
        elif self.bodies:
            page_id = self.bodies.popleft()
            with scope(phase="bodies", entity_type="page", entity_id=page_id):
                self._walk(page_id)
        else:
            return False
        return True

    def _flush(self) -> None:
        """Write the records found since the last flush (data sources after the naming checks)."""
        if self.unloaded_sources:
            check_workspace_names(self.data_sources.values())
        with scope(phase="schema"):
            load.load_databases(self.conn, self.unloaded_databases)
            for data_source in self.unloaded_sources:
                self.row_tables[data_source.data_source_id] = load.load_data_source(self.conn, data_source)
                self.unqueried.append(data_source.data_source_id)
            load.load_pages(self.conn, self.unloaded_pages)
        with scope(phase="views"):
            load.load_views(self.conn, self.unloaded_views)
        self.unloaded_databases, self.unloaded_sources, self.unloaded_pages, self.unloaded_views = [], [], [], []

    # ------------------------------------------------------------ references

    def _refer(self, refs: Iterable[fetch.Ref]) -> None:
        for ref in refs:
            if ref not in self.attempted and not self._known(ref):
                self.attempted.add(ref)
                self.refs.append(ref)

    def _known(self, ref: fetch.Ref) -> bool:
        kind, entity_id = ref
        if kind == "page":
            return entity_id in self.pages or entity_id in self.rows
        if kind == "data_source":
            return entity_id in self.data_sources
        if kind in ("database", "original_database"):
            return entity_id in self.databases
        if kind == "view":
            return entity_id in self.views
        return False

    def _follow_references(self) -> None:
        """Resolve queued references until none is left; lookups end quietly on a 404."""
        while self.refs:
            kind, entity_id = self.refs.popleft()
            if self._known((kind, entity_id)):
                continue
            with scope(entity_type=kind, entity_id=entity_id):
                if kind == "page":
                    self._resolve_page(entity_id)
                elif kind == "block":
                    block = self.api.retrieve_block(entity_id, missing_ok=True)
                    if block is not None:
                        self._refer(filter(None, [fetch.parent_reference(parse_parent(block["parent"]))]))
                elif kind == "data_source":
                    obj = self.api.retrieve_data_source(entity_id, missing_ok=True)
                    if obj is not None:
                        self._accept_data_source(parse_data_source(obj))
                elif kind == "original_database":
                    obj = self.api.retrieve_database(entity_id, missing_ok=True)
                    if obj is not None:
                        self._accept_database(obj)
                elif kind == "database":
                    self._resolve_container(entity_id, None)
                elif kind == "view":
                    obj = self.api.retrieve_view(entity_id, missing_ok=True)
                    if obj is not None:
                        self._accept_view(parse_view(obj))

    def _resolve_page(self, page_id: str) -> None:
        obj = fetch.fetch_referenced_page(self.api, page_id)
        if obj is None:
            return
        if obj["parent"]["type"] == "data_source_id":
            self._refer([("data_source", obj["parent"]["data_source_id"])])
            return
        self._accept_page(parse_page(obj, is_row=False))

    def _resolve_container(self, database_id: str, block: Optional[BlockRecord]) -> None:
        """Follow a database that may be original or linked through its views.

        Its data sources come from its views; it is an original database only if
        it turns out to contain one of them, otherwise it is recorded as linked.
        A database whose views can't be listed (not found) is not recorded.
        """
        with scope(phase="views", entity_type="database", entity_id=database_id):
            listed = self.api.list_views(database_id=database_id, missing_ok=True)
        if listed is None:
            return
        self.containers.setdefault(database_id, block)
        self._accept_listed_views(listed)

    # ------------------------------------------------------------ accepting records

    def _accept_data_source(self, data_source: Optional[DataSourceRecord]) -> None:
        if data_source is None or data_source.data_source_id in self.data_sources:
            return
        self.data_sources[data_source.data_source_id] = data_source
        self.unloaded_sources.append(data_source)
        self._refer(fetch.data_source_references(data_source))

    def _accept_database(self, obj: Mapping[str, Any]) -> None:
        database = parse_database(obj)
        if database.database_id in self.databases:
            return
        self.databases[database.database_id] = database
        self.unloaded_databases.append(database)
        self.unlisted_databases.append(database.database_id)
        self._refer(fetch.database_references(database))
        for entry in obj.get("data_sources") or []:
            if entry["id"] not in self.data_sources:
                with scope(entity_type="data_source", entity_id=entry["id"]):
                    self._accept_data_source(fetch.fetch_data_source(self.api, entry["id"]))

    def _accept_page(self, page: PageRecord) -> None:
        if page.page_id in self.pages:
            return
        self.pages.add(page.page_id)
        self.unloaded_pages.append(page)
        self.bodies.append(page.page_id)
        self._refer(fetch.page_references(page))

    def _accept_listed_views(self, view_ids: Iterable[str]) -> None:
        for view_id in view_ids:
            if view_id not in self.views:
                with scope(entity_type="view", entity_id=view_id):
                    self._accept_view(fetch.fetch_view(self.api, view_id))

    def _accept_view(self, view: ViewRecord) -> None:
        if view.view_id in self.views:
            return
        self.views[view.view_id] = view
        self.unloaded_views.append(view)
        self.containers.setdefault(view.database_id, None)
        self._refer(fetch.view_references(view))

    # ------------------------------------------------------------ rows and bodies

    def _load_rows(self, data_source: DataSourceRecord) -> None:
        ds_id = data_source.data_source_id
        for batch in fetch.query_rows(self.api, ds_id):
            if any(fetch.schema_mismatches(obj, data_source) for obj in batch.rows):
                data_source = self._refresh_schema(data_source)
            rows = [fetch.parse_row(self.api, obj, data_source) for obj in batch.rows]
            load.load_rows(self.conn, data_source, self.row_tables[ds_id], rows)
            self.rows.update(row.page.page_id for row in rows)
            if self.row_bodies:
                self.bodies.extend(row.page.page_id for row in rows)
            self._refer(ref for row in rows for ref in fetch.mention_references(list(row.values.values())))
            self._refer(("data_source", child) for child in batch.child_data_source_ids)

    def _walk(self, page_id: str) -> None:
        blocks = fetch.fetch_page_body(self.api, page_id)
        load.load_page_body(self.conn, page_id, blocks)
        for block in blocks:
            if block.type == "child_database":
                if block.block_id not in self.databases:
                    self.containers[block.block_id] = block
                    self._refer([("database", block.block_id)])
            self._refer(fetch.block_references(block))

    def _record_containers(self) -> None:
        """Record every database seen (view parent or ``child_database`` block)
        that is not an original database as a linked database.
        """
        linked = []
        for database_id, block in self.containers.items():
            if database_id in self.databases:
                continue
            if block is not None:
                linked.append(fetch.linked_database_from_block(block))
            else:
                linked.append(DatabaseRecord(database_id=database_id, is_linked=True))
        load.load_databases(self.conn, linked)

    def _refresh_schema(self, data_source: DataSourceRecord) -> DataSourceRecord:
        """Re-read a data source whose rows disagree with its schema, and extend its tables.

        :raises SnapshotIntegrityError: The data source was trashed meanwhile, a
            property changed type, or a new property cannot be named.
        """
        fresh = fetch.fetch_data_source(self.api, data_source.data_source_id)
        if fresh is None:
            raise SnapshotIntegrityError(f"Data source {data_source.data_source_id} was trashed during the snapshot")
        merged = data_source.refreshed(fresh)
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

