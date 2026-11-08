"""
Main migration class for Notion workspace to PostgreSQL migration.
"""

import json
import re
from typing import Dict, List, Any, Optional
from notion_client import Client, RetryOptions
from sqlalchemy import Engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

from .schema_mapper import NotionPropertyMapper
from .rate_limiter import RateLimiter

# Fixed column names in database_content tables; user properties may not use these.
_BUILTIN_COLUMN_NAMES = frozenset({
    "notion_id", "created_time", "last_edited_time", "created_by",
    "last_edited_by", "cover", "icon", "url", "page_content",
})


class NotionMigrator:
    """Main class for migrating Notion workspace to PostgreSQL."""

    def __init__(
        self,
        notion_token: str,
        db_connection: Engine,
        fetch_page_content: bool = False,
    ):
        """
        Initialize the migrator.

        Args:
            notion_token: Notion integration token
            db_connection: SQLAlchemy engine for PostgreSQL connection
            fetch_page_content: Fetch and store each page's full block tree in a
                ``page_content`` JSONB column.  Requires one or more extra API calls
                per database row (blocks.children.list, recursively), which
                significantly slows migration for large databases.  When False (the
                default) the column is omitted entirely and only the structured
                property data available from the bulk query is stored.
        """
        self.notion = Client(
            auth=notion_token,
            notion_version="2026-03-11",
            timeout_ms=30_000,
            retry=RetryOptions(
                max_retries=5,
                initial_retry_delay_ms=500,
                max_retry_delay_ms=30_000,
            ),
        )
        self.db_engine = db_connection
        self.fetch_page_content = fetch_page_content
        self.rate_limiter = RateLimiter(requests_per_second=2.5)
        self.metadata = MetaData()
        self.property_mapper = NotionPropertyMapper()

        # Track created tables per schema (keyed by Notion DB ID)
        self.created_tables: Dict[str, Table] = {}
        self.lookup_tables: Dict[str, Table] = {}
        self.computed_fields_tables: Dict[str, Table] = {}
        self.property_meta_tables: Dict[str, Table] = {}
        self.view_tables: Dict[str, Table] = {}

        # Standalone pages discovered via workspace search
        self.standalone_pages: List[Dict] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the complete migration process."""
        self._test_database_connection()
        self._require_empty_database()
        self._test_notion_connection()

        databases = self._get_databases()
        if not databases:
            raise ValueError(
                "No Notion databases found. Share databases with your integration "
                "(Notion integration settings → Access)."
            )
        self._check_table_name_collisions(databases)
        self.standalone_pages = self._discover_standalone_pages()

        for db_info in databases:
            self._create_table_schema(db_info)
        for db_info in databases:
            self._migrate_database_data(db_info)
        for db_info in databases:
            self._migrate_views(db_info)
        if self.standalone_pages:
            self._migrate_standalone_pages()

    # ------------------------------------------------------------------
    # Pre-flight checks
    # ------------------------------------------------------------------

    def _require_empty_database(self) -> None:
        """Require a database with no user schemas (except public) and no user objects."""
        with self.db_engine.connect() as conn:
            extra_schemas = conn.execute(text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN (
                    'pg_catalog', 'information_schema', 'pg_toast', 'public'
                )
            """)).fetchall()
            if extra_schemas:
                names = ", ".join(row[0] for row in extra_schemas)
                raise ValueError(
                    f"Database is not empty: found schema(s): {names}. "
                    "Use a new empty database or drop all non-public schemas."
                )

            existing_objects = conn.execute(text("""
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND table_type IN ('BASE TABLE', 'VIEW')
            """)).fetchall()
            if existing_objects:
                lines = [
                    f"  - {schema}.{name} ({kind})"
                    for schema, name, kind in existing_objects
                ]
                raise ValueError(
                    "Database is not empty: found existing table(s) or view(s):\n"
                    + "\n".join(lines)
                    + "\nUse a new empty database or drop all objects."
                )

    def _check_table_name_collisions(self, databases: List[Dict]) -> None:
        """Raise if sanitized Notion database titles would map to the same PostgreSQL table."""
        by_table_name: Dict[str, List[str]] = {}
        for db_info in databases:
            title = db_info["title"]
            table_name = self._clean_table_name(title)
            by_table_name.setdefault(table_name, []).append(title)

        collisions = {n: t for n, t in by_table_name.items() if len(t) > 1}
        if not collisions:
            return

        lines = []
        for table_name, titles in sorted(collisions.items()):
            notion_names = "', '".join(titles)
            lines.append(f"  '{notion_names}' → PostgreSQL table '{table_name}'")
        raise ValueError(
            "Multiple Notion databases map to the same PostgreSQL table name after sanitization:\n"
            + "\n".join(lines)
        )

    def _test_database_connection(self) -> None:
        """Test PostgreSQL database connection."""
        with self.db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    def _test_notion_connection(self) -> None:
        """Test Notion API connection."""
        self.rate_limiter.rate_limited_call(self.notion.users.me)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _get_databases(self) -> List[Dict[str, Any]]:
        """Discover all accessible data sources and retrieve their full schemas."""
        raw_data_sources: List[Dict[str, Any]] = []
        start_cursor = None

        while True:
            response = self.rate_limiter.rate_limited_call(
                self.notion.search,
                filter={"property": "object", "value": "data_source"},
                start_cursor=start_cursor,
            )

            for item in response.get("results", []):
                raw_data_sources.append(item)


            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        detailed: List[Dict[str, Any]] = []
        for i, item in enumerate(raw_data_sources, 1):
            data_source_id = item["id"]
            database_id = (item.get("parent") or {}).get("database_id")
            if not database_id:
                raise ValueError(
                    f"Data source {data_source_id} has no parent database_id; "
                    "cannot migrate without a database container."
                )

            data_source_details = self.rate_limiter.rate_limited_call(
                self.notion.request,
                f"data_sources/{data_source_id}",
                "GET",
            )
            database_details = self.rate_limiter.rate_limited_call(
                self.notion.databases.retrieve,
                database_id=database_id,
            )


            title = self._extract_data_source_title(data_source_details, database_details)
            detailed.append({
                "id": data_source_id,
                "database_id": database_id,
                "title": title,
                "details": data_source_details,
                "database_details": database_details,
            })

        return detailed

    def _extract_data_source_title(
        self, data_source: Dict[str, Any], database: Dict[str, Any]
    ) -> str:
        """Prefer the data source title; fall back to the parent database title."""
        for obj in (data_source, database):
            title_property = obj.get("title", [])
            if title_property:
                text = "".join(item.get("plain_text", "") for item in title_property)
                if text.strip():
                    return text.strip()
        return f"Untitled_{data_source['id'][:8]}"

    def _discover_standalone_pages(self) -> List[Dict]:
        """Discover all standalone pages (non-database-row pages) accessible to the integration."""
        pages = []
        start_cursor = None

        while True:
            kwargs: Dict[str, Any] = {
                "filter": {"property": "object", "value": "page"},
            }
            if start_cursor:
                kwargs["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.search, **kwargs
            )

            for page in response.get("results", []):
                parent = page.get("parent", {})
                parent_type = parent.get("type", "")

                # Database row pages are already captured in database_content.*
                if parent_type in ("database_id", "data_source_id"):
                    continue


                # Determine parent_id
                if parent_type == "workspace":
                    parent_id = None
                elif parent_type == "page_id":
                    parent_id = parent.get("page_id")
                elif parent_type == "block_id":
                    parent_id = parent.get("block_id")
                else:
                    parent_id = None

                title_prop = page.get("properties", {}).get("title", {})
                title_items = title_prop.get("title", [])
                title = "".join(
                    item.get("plain_text", "") for item in title_items
                ).strip() or "Untitled"

                entry: Dict[str, Any] = {
                    "page_id": page["id"],
                    "title": title,
                    "parent_type": parent_type,
                    "parent_id": parent_id,
                    "created_time": page.get("created_time"),
                    "last_edited_time": page.get("last_edited_time"),
                    "created_by": page.get("created_by", {}).get("id"),
                    "last_edited_by": page.get("last_edited_by", {}).get("id"),
                    "cover": json.dumps(page["cover"]) if page.get("cover") else None,
                    "icon": json.dumps(page["icon"]) if page.get("icon") else None,
                    "url": page.get("url"),
                }

                pages.append(entry)


            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        return pages

    # ------------------------------------------------------------------
    # Migration plan UI
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Schema creation
    # ------------------------------------------------------------------

    def _create_table_schema(self, db_info: Dict) -> None:
        """Create all PostgreSQL tables for one Notion data source."""
        from sqlalchemy import Column, String, Text, Boolean
        from sqlalchemy.dialects.postgresql import JSONB

        data_source_id = db_info["id"]
        database_id = db_info["database_id"]
        title = db_info["title"]
        details = db_info["details"]
        database_details = db_info.get("database_details") or {}
        properties = details["properties"]
        table_name = self._clean_table_name(title)

        # ---- database_content columns (built-ins first, then user properties) ----
        content_columns = [
            Column("notion_id", String(36), primary_key=True),
            Column("created_time", String(40)),
            Column("last_edited_time", String(40)),
            Column("created_by", Text),
            Column("last_edited_by", Text),
            Column("cover", JSONB),
            Column("icon", JSONB),
            Column("url", Text),
        ]
        if self.fetch_page_content:
            content_columns.append(Column("page_content", JSONB))

        lookup_table_configs = []
        computed_properties = []
        # Maps original prop_name → pg column name for supported non-computed properties.
        non_computed_props: List[tuple] = []  # (prop_name, prop_config, pg_column_or_None)

        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")

            if prop_type in ["formula", "rollup"]:
                computed_properties.append((prop_name, prop_type, prop_config))
                continue

            prop_col_name = self._clean_property_col_name(prop_name)
            column = self.property_mapper.get_postgres_column(prop_col_name, prop_config)
            if column is not None:
                content_columns.append(column)
                non_computed_props.append((prop_name, prop_config, prop_col_name))
            else:
                non_computed_props.append((prop_name, prop_config, None))

            if self.property_mapper.needs_lookup_table(prop_config):
                lookup_table_configs.append((prop_name, prop_config))

        content_table = Table(
            table_name, self.metadata, *content_columns, schema="database_content"
        )
        db_description = database_details.get("description") or details.get("description")
        if db_description:
            description_text = "".join(
                item.get("plain_text", "") for item in db_description
            )
            if description_text.strip():
                content_table.comment = description_text.strip()

        # ---- database_select_options lookup tables ----
        for prop_name, prop_config in lookup_table_configs:
            option_table_name = self.property_mapper.get_lookup_table_name(table_name, prop_name)
            option_table = Table(
                option_table_name,
                self.metadata,
                Column("id", String(36), primary_key=True),
                Column("value", String(255), nullable=False, unique=True),
                Column("color", String(50)),
                schema="database_select_options",
            )
            self.lookup_tables[f"{table_name}_{prop_name}"] = option_table

        # ---- database_computed_fields table (formula + rollup definitions) ----
        computed_fields_table = Table(
            table_name,
            self.metadata,
            Column("property_name", Text, primary_key=True),
            Column("property_type", Text, nullable=False),
            Column("config", JSONB, nullable=False),
            schema="database_computed_fields",
        )
        self.computed_fields_tables[data_source_id] = computed_fields_table

        # ---- database_property_meta table (non-computed property specifications) ----
        # One row per non-formula/non-rollup property. The full Notion config is stored
        # in the config column, giving consumers (e.g. /schema endpoint, restoration
        # scripts) direct access to relation targets, select option lists, and all
        # other per-property metadata without re-querying the Notion API.
        property_meta_table = Table(
            table_name,
            self.metadata,
            Column("property_id", Text, primary_key=True),
            Column("property_name", Text, nullable=False),
            Column("notion_type", Text, nullable=False),
            Column("pg_column", Text),   # NULL when the property type is not supported
            Column("description", Text),
            Column("config", JSONB, nullable=False),
            schema="database_property_meta",
        )
        self.property_meta_tables[data_source_id] = property_meta_table

        # ---- database_views table ----
        view_table = Table(
            table_name,
            self.metadata,
            Column("view_id", Text, primary_key=True),
            Column("name", Text, nullable=False),
            Column("view_type", Text, nullable=False),
            Column("config", JSONB, nullable=False),
            schema="database_views",
        )
        self.view_tables[data_source_id] = view_table

        # ---- database_meta table (one row per data source) ----
        meta_columns = [
            Column("notion_data_source_id", String(36), primary_key=True),
            Column("notion_database_id", String(36), nullable=False),
            Column("title", Text),
            Column("description", JSONB),
            Column("created_time", String(40)),
            Column("last_edited_time", String(40)),
            Column("created_by", Text),
            Column("last_edited_by", Text),
            Column("cover", JSONB),
            Column("icon", JSONB),
            Column("url", Text),
            Column("is_inline", Boolean),
            Column("data_sources", JSONB),
        ]

        meta_table = Table(
            table_name, self.metadata, *meta_columns, schema="database_meta"
        )

        self.created_tables[data_source_id] = content_table

        # ---- Create all schemas ----
        with self.db_engine.connect() as conn:
            for schema in (
                "database_content", "database_select_options",
                "database_computed_fields", "database_property_meta",
                "database_views", "database_meta",
            ):
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        self.metadata.create_all(self.db_engine)

        # ---- Populate computed_fields ----
        if computed_properties:
            rows = [
                {
                    "property_name": prop_name,
                    "property_type": prop_type,
                    "config": json.dumps(prop_config),
                }
                for prop_name, prop_type, prop_config in computed_properties
            ]
            with self.db_engine.connect() as conn:
                conn.execute(computed_fields_table.insert(), rows)
                conn.commit()

        # ---- Populate database_meta ----
        meta_row: Dict[str, Any] = {
            "notion_data_source_id": data_source_id,
            "notion_database_id": database_id,
            "title": title,
            "description": json.dumps(database_details.get("description") or []),
            "created_time": database_details.get("created_time"),
            "last_edited_time": database_details.get("last_edited_time"),
            "created_by": (database_details.get("created_by") or {}).get("id"),
            "last_edited_by": (database_details.get("last_edited_by") or {}).get("id"),
            "cover": json.dumps(database_details["cover"]) if database_details.get("cover") else None,
            "icon": json.dumps(database_details["icon"]) if database_details.get("icon") else None,
            "url": database_details.get("url"),
            "is_inline": database_details.get("is_inline"),
            "data_sources": json.dumps(database_details.get("data_sources") or []),
        }

        with self.db_engine.connect() as conn:
            conn.execute(meta_table.insert(), meta_row)
            conn.commit()

        # ---- Populate database_property_meta ----
        if non_computed_props:
            prop_meta_rows = [
                {
                    "property_id": prop_config.get("id", ""),
                    "property_name": prop_name,
                    "notion_type": prop_config.get("type") or "unknown",
                    "pg_column": pg_col,
                    "description": (prop_config.get("description") or "").strip() or None,
                    "config": json.dumps(prop_config),
                }
                for prop_name, prop_config, pg_col in non_computed_props
            ]
            with self.db_engine.connect() as conn:
                conn.execute(property_meta_table.insert(), prop_meta_rows)
                conn.commit()


    # ------------------------------------------------------------------
    # Data migration
    # ------------------------------------------------------------------

    def _migrate_database_data(self, db_info: Dict) -> None:
        """Migrate all row data from a Notion data source."""
        data_source_id = db_info["id"]
        table = self.created_tables[data_source_id]
        properties = db_info["details"]["properties"]

        start_cursor = None
        total_pages = 0
        page_batches = 0

        while True:
            body: Dict[str, Any] = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.request,
                f"data_sources/{data_source_id}/query",
                "POST",
                body=body,
            )
            page_batches += 1

            pages = response.get("results", [])
            if pages:
                self._insert_pages_batch(table, pages, properties)
                total_pages += len(pages)


            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        self._populate_lookup_tables(table.name, properties)
        self._add_select_foreign_keys(table.name, properties)

    def _migrate_views(self, db_info: Dict) -> int:
        """Fetch and store all view configurations for a Notion database."""
        data_source_id = db_info["id"]
        database_id = db_info["database_id"]
        view_table = self.view_tables[data_source_id]

        start_cursor = None
        view_ids: List[str] = []

        while True:
            query: Dict[str, Any] = {"database_id": database_id}
            if start_cursor:
                query["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.request, "views", "GET", query=query
            )

            for view_ref in response.get("results", []):
                view_ids.append(view_ref["id"])

            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        if not view_ids:
            return 0

        rows = []
        for view_id in view_ids:
            view = self.rate_limiter.rate_limited_call(
                self.notion.request, f"views/{view_id}", "GET"
            )
            rows.append({
                "view_id": view["id"],
                "name": (view.get("name") or ""),
                "view_type": (view.get("type") or "unknown"),
                "config": json.dumps(view),
            })

        if rows:
            with self.db_engine.connect() as conn:
                stmt = insert(view_table).values(rows)
                stmt = stmt.on_conflict_do_nothing(index_elements=["view_id"])
                conn.execute(stmt)
                conn.commit()

        return len(rows)

    def _insert_pages_batch(
        self, table: Table, pages: List[Dict], properties: Dict
    ) -> None:
        """Insert a batch of database row pages into database_content."""
        if not pages:
            return

        rows = []
        for page in pages:

            row_data: Dict[str, Any] = {
                "notion_id": page["id"],
                "created_time": page.get("created_time"),
                "last_edited_time": page.get("last_edited_time"),
                "created_by": (page.get("created_by") or {}).get("id"),
                "last_edited_by": (page.get("last_edited_by") or {}).get("id"),
                "cover": json.dumps(page["cover"]) if page.get("cover") else None,
                "icon": json.dumps(page["icon"]) if page.get("icon") else None,
                "url": page.get("url"),
            }

            page_properties = page.get("properties", {})
            for prop_name, prop_config in properties.items():
                prop_type = prop_config.get("type")
                if prop_type in ["formula", "rollup"]:
                    continue

                prop_col = self._clean_property_col_name(prop_name)
                if prop_col not in table.c:
                    continue

                prop_data = self._resolve_paginated_property(
                    page["id"], page_properties.get(prop_name) or {}
                )

                if prop_type == "date":
                    row_data[prop_col] = self.property_mapper.extract_date_range(prop_data)
                else:
                    row_data[prop_col] = self.property_mapper.extract_property_value(
                        prop_data, prop_type
                    )

            if self.fetch_page_content:
                row_data["page_content"] = json.dumps(
                    self._fetch_page_content_jsonb(page["id"])
                )

            rows.append(row_data)

        if rows:
            with self.db_engine.connect() as conn:
                conn.execute(table.insert(), rows)
                conn.commit()

    def _resolve_paginated_property(
        self, page_id: str, prop_data: Dict
    ) -> Dict:
        """
        Fetch all pages of a truncated property item (has_more: true).

        The Notion API caps array properties (relation, people, rich_text, title)
        at 25 items per query response. When has_more is True this method pages
        through pages.properties.retrieve to collect the complete list.
        """
        if not prop_data.get("has_more"):
            return prop_data

        prop_id = prop_data.get("id", "")
        prop_type = prop_data.get("type", "")
        if not prop_id or not prop_type:
            return prop_data

        all_items = list(prop_data.get(prop_type, []))
        start_cursor = prop_data.get("next_cursor")

        while start_cursor:
            response = self.rate_limiter.rate_limited_call(
                self.notion.pages.properties.retrieve,
                page_id=page_id,
                property_id=prop_id,
                start_cursor=start_cursor,
            )
            all_items.extend(response.get(prop_type, []))
            if not response.get("has_more"):
                break
            start_cursor = response.get("next_cursor")

        result = dict(prop_data)
        result[prop_type] = all_items
        result["has_more"] = False
        return result

    def _populate_lookup_tables(self, table_name: str, properties: Dict) -> None:
        """Populate option tables for select, multi-select, and status properties."""
        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")
            if prop_type not in ["select", "multi_select", "status"]:
                continue

            lookup_key = f"{table_name}_{prop_name}"
            if lookup_key not in self.lookup_tables:
                continue

            option_table = self.lookup_tables[lookup_key]
            options = prop_config.get(prop_type, {}).get("options", [])

            if options:
                rows = [
                    {
                        "id": opt["id"],
                        "value": opt["name"],
                        "color": opt.get("color", "default"),
                    }
                    for opt in options
                ]
                with self.db_engine.connect() as conn:
                    stmt = insert(option_table).values(rows)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
                    conn.execute(stmt)
                    conn.commit()

    def _add_select_foreign_keys(self, table_name: str, properties: Dict) -> None:
        """Add FK constraints from select/status columns to their option tables."""
        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")
            if prop_type not in ("select", "status"):
                continue

            prop_col = self._clean_property_col_name(prop_name)
            option_table_name = self.property_mapper.get_lookup_table_name(
                table_name, prop_name
            )
            constraint_name = f"fk_{table_name}_{prop_col}"

            with self.db_engine.connect() as conn:
                conn.execute(text(f"""
                    ALTER TABLE database_content.{table_name}
                    ADD CONSTRAINT {constraint_name}
                    FOREIGN KEY ({prop_col})
                    REFERENCES database_select_options.{option_table_name}(value)
                    ON DELETE SET NULL
                """))
                conn.commit()

    # ------------------------------------------------------------------
    # Page content (lossless JSONB block tree)
    # ------------------------------------------------------------------

    def _fetch_page_content_jsonb(self, block_id: str) -> List[Dict]:
        """
        Recursively fetch all children of a block and return them as a nested list.

        Each entry is a dict with id, type, content (type-specific data),
        audit fields, and a children list (recursively populated).
        child_database blocks are not recursed into.
        """
        results = []
        start_cursor = None

        while True:
            kwargs: Dict[str, Any] = {"block_id": block_id, "page_size": 100}
            if start_cursor:
                kwargs["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.blocks.children.list, **kwargs
            )

            for block in response.get("results", []):
                b_type = block.get("type", "unsupported")
                entry: Dict[str, Any] = {
                    "id": block.get("id"),
                    "type": b_type,
                    "content": block.get(b_type, {}),
                    "created_time": block.get("created_time"),
                    "last_edited_time": block.get("last_edited_time"),
                    "created_by": (block.get("created_by") or {}).get("id"),
                    "last_edited_by": (block.get("last_edited_by") or {}).get("id"),
                    "children": [],
                }
                if block.get("has_children") and b_type != "child_database":
                    entry["children"] = self._fetch_page_content_jsonb(block["id"])
                results.append(entry)

            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        return results

    # ------------------------------------------------------------------
    # Standalone pages migration
    # ------------------------------------------------------------------

    def _create_standalone_pages_schema(self) -> None:
        """Create the standalone_pages schema and its two tables."""
        page_tree_cols = """
            page_id          TEXT PRIMARY KEY,
            title            TEXT,
            parent_type      TEXT NOT NULL,
            parent_id        TEXT,
            depth            INTEGER NOT NULL,
            created_time     TEXT,
            last_edited_time TEXT,
            created_by       TEXT,
            last_edited_by   TEXT,
            cover            JSONB,
            icon             JSONB,
            url              TEXT
        """

        block_content_cols = """
            block_id        TEXT PRIMARY KEY,
            page_id         TEXT NOT NULL
                REFERENCES standalone_pages.page_tree(page_id),
            parent_block_id TEXT,
            block_type      TEXT NOT NULL,
            position        INTEGER NOT NULL,
            has_children    BOOLEAN NOT NULL DEFAULT FALSE,
            created_time     TEXT,
            last_edited_time TEXT,
            created_by       TEXT,
            last_edited_by   TEXT,
            content         JSONB NOT NULL
        """

        with self.db_engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS standalone_pages"))
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS standalone_pages.page_tree ({page_tree_cols})"
            ))
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS standalone_pages.block_content ({block_content_cols})"
            ))
            conn.commit()

    def _migrate_standalone_pages(self) -> None:
        """Insert page_tree rows and recursively fetch block content for standalone pages."""
        page_map: Dict[str, Dict] = {p["page_id"]: p for p in self.standalone_pages}
        depth_cache: Dict[str, int] = {}

        def compute_depth(page_id: str) -> int:
            if page_id in depth_cache:
                return depth_cache[page_id]
            page = page_map.get(page_id)
            if not page or page["parent_type"] == "workspace" or page["parent_id"] is None:
                depth_cache[page_id] = 0
                return 0
            parent_id = page["parent_id"]
            if parent_id not in page_map:
                depth_cache[page_id] = 1
                return 1
            result = compute_depth(parent_id) + 1
            depth_cache[page_id] = result
            return result

        self._create_standalone_pages_schema()

        for page in self.standalone_pages:
            page_id = page["page_id"]
            depth = compute_depth(page_id)

            insert_data: Dict[str, Any] = {
                "page_id": page_id,
                "title": page["title"],
                "parent_type": page["parent_type"],
                "parent_id": page["parent_id"],
                "depth": depth,
                "created_time": page.get("created_time"),
                "last_edited_time": page.get("last_edited_time"),
                "created_by": page.get("created_by"),
                "last_edited_by": page.get("last_edited_by"),
                "cover": page.get("cover"),
                "icon": page.get("icon"),
                "url": page.get("url"),
            }

            cols = ", ".join(insert_data.keys())
            params = ", ".join(f":{k}" for k in insert_data.keys())

            with self.db_engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO standalone_pages.page_tree ({cols})
                    VALUES ({params})
                    ON CONFLICT (page_id) DO NOTHING
                """), insert_data)
                conn.commit()

            self._fetch_blocks_recursive(page_id, page_id, None, 0)


    def _fetch_blocks_recursive(
        self,
        page_id: str,
        block_id: str,
        parent_block_id: Optional[str],
        position_start: int,
    ) -> None:
        """Fetch children of a block and insert them into standalone_pages.block_content."""
        start_cursor = None
        position = position_start

        while True:
            kwargs: Dict[str, Any] = {"block_id": block_id, "page_size": 100}
            if start_cursor:
                kwargs["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.blocks.children.list, **kwargs
            )

            blocks = response.get("results", [])

            if blocks:
                for block in blocks:
                    b_id = block.get("id", "")
                    b_type = block.get("type", "unsupported")
                    b_content = block.get(b_type, {})
                    has_children = block.get("has_children", False)

                    row: Dict[str, Any] = {
                        "block_id": b_id,
                        "page_id": page_id,
                        "parent_block_id": parent_block_id,
                        "block_type": b_type,
                        "position": position,
                        "has_children": has_children,
                        "created_time": block.get("created_time"),
                        "last_edited_time": block.get("last_edited_time"),
                        "created_by": (block.get("created_by") or {}).get("id"),
                        "last_edited_by": (block.get("last_edited_by") or {}).get("id"),
                        "content": json.dumps(b_content),
                    }
                    position += 1

                    with self.db_engine.connect() as conn:
                        conn.execute(text("""
                            INSERT INTO standalone_pages.block_content
                                (block_id, page_id, parent_block_id, block_type,
                                 position, has_children,
                                 created_time, last_edited_time, created_by, last_edited_by,
                                 content)
                            VALUES
                                (:block_id, :page_id, :parent_block_id, :block_type,
                                 :position, :has_children,
                                 :created_time, :last_edited_time, :created_by, :last_edited_by,
                                 :content::jsonb)
                            ON CONFLICT (block_id) DO NOTHING
                        """), row)
                        conn.commit()

                for block in blocks:
                    if block.get("has_children") and block.get("type") != "child_database":
                        self._fetch_blocks_recursive(page_id, block["id"], block["id"], 0)

            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _clean_table_name(self, name: str) -> str:
        """Sanitize a name to a PostgreSQL-compatible identifier (max 50 chars)."""
        cleaned = re.sub(r"[^\w]", "_", name)
        cleaned = re.sub(r"_+", "_", cleaned)
        cleaned = cleaned.strip("_")
        if not cleaned or cleaned[0].isdigit():
            cleaned = f"table_{cleaned}"
        if len(cleaned) > 50:
            cleaned = cleaned[:50].rstrip("_")
        return cleaned.lower()

    def _clean_property_col_name(self, prop_name: str) -> str:
        """Return the property_ prefixed column name for a Notion user property."""
        cleaned = re.sub(r"[^\w]", "_", prop_name)
        cleaned = re.sub(r"_+", "_", cleaned)
        cleaned = cleaned.strip("_")
        if not cleaned or cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        # Truncate so that the full name with 'property_' prefix stays within
        # PostgreSQL's 63-char limit.
        if len(cleaned) > 50:
            cleaned = cleaned[:50].rstrip("_")
        return f"property_{cleaned.lower()}"
