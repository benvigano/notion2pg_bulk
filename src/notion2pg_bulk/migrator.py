"""
Main migration class for Notion workspace to PostgreSQL migration.
"""

import json
import re
from typing import Dict, List, Any, Optional
from tabulate import tabulate
from notion_client import Client, RetryOptions
from sqlalchemy import Engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

from .schema_mapper import NotionPropertyMapper
from .progress_tracker import ProgressTracker
from .rate_limiter import RateLimiter

# Fixed column names in database_content tables; user properties may not use these.
_BUILTIN_COLUMN_NAMES = frozenset({
    "notion_id", "created_time", "last_edited_time", "created_by",
    "last_edited_by", "cover", "icon", "url", "page_content",
    "in_trash", "archived",
})


class NotionMigrator:
    """Main class for migrating Notion workspace to PostgreSQL."""

    def __init__(
        self,
        notion_token: str,
        db_connection: Engine,
        interactive_mode: bool = True,
        include_trashed: bool = False,
        fetch_page_content: bool = False,
        include_standalone_pages: bool = True,
        ignored_unsupported_property_types: Optional[List[str]] = None,
    ):
        """
        Initialize the migrator.

        Args:
            notion_token: Notion integration token
            db_connection: SQLAlchemy engine for PostgreSQL connection
            interactive_mode: Enable interactive mode with progress bars and confirmation
            include_trashed: Include archived/trashed items and add in_trash/archived columns
            fetch_page_content: Fetch and store each page's full block tree in a
                ``page_content`` JSONB column.  Requires one or more extra API calls
                per database row (blocks.children.list, recursively), which
                significantly slows migration for large databases.  When False (the
                default) the column is omitted entirely and only the structured
                property data available from the bulk query is stored.
            include_standalone_pages: Discover and migrate standalone pages (pages that
                are not rows inside a Notion database) together with their full recursive
                block tree into the ``standalone_pages`` schema.  Enabled by default.
                Disable to skip the workspace-wide page search and all block fetching for
                standalone pages, which can noticeably reduce migration time for large
                workspaces with many pages.
            ignored_unsupported_property_types: Unsupported property types to skip silently
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
        self.interactive_mode = interactive_mode
        self.include_trashed = include_trashed
        self.fetch_page_content = fetch_page_content
        self.include_standalone_pages = include_standalone_pages
        self.ignored_unsupported_property_types = {
            t.lower() for t in (ignored_unsupported_property_types or [])
        }
        self.progress = ProgressTracker(interactive_mode)
        self.rate_limiter = RateLimiter(requests_per_second=2.5)
        self.metadata = MetaData()
        self.property_mapper = NotionPropertyMapper()

        # Track created tables per schema (keyed by Notion DB ID)
        self.created_tables: Dict[str, Table] = {}
        self.lookup_tables: Dict[str, Table] = {}
        self.computed_fields_tables: Dict[str, Table] = {}
        self.property_meta_tables: Dict[str, Table] = {}
        self.view_tables: Dict[str, Table] = {}

        # Track skipped properties for the migration plan summary
        self.skipped_properties: List[Dict[str, str]] = []

        # Standalone pages discovered via workspace search
        self.standalone_pages: List[Dict] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the complete migration process."""
        try:
            self._test_database_connection()
            self._require_empty_database()
            self._test_notion_connection()

            # Phase 1: Discover Notion databases
            self.progress.start_phase("🔎 Discovering Notion databases", None)
            databases = self._get_databases()
            self.progress.finish_phase()

            if not databases:
                raise ValueError(
                    "No Notion databases found. Share databases with your integration "
                    "(Notion integration settings → Access)."
                )

            self._check_table_name_collisions(databases)

            # Phase 2: Discover standalone pages (skipped when include_standalone_pages=False)
            if self.include_standalone_pages:
                self.progress.start_phase("🔎 Discovering standalone pages", None)
                self.standalone_pages = self._discover_standalone_pages()
                self.progress.finish_phase()

            # Show migration plan and ask for confirmation
            if self.interactive_mode:
                self._show_migration_plan(databases)
                if not self._get_user_confirmation():
                    self.progress.log("Migration cancelled by user.")
                    return
            else:
                self._warn_about_unignored_unsupported_properties(databases)

            # Phase 3: Create PostgreSQL schemas and table definitions
            self.progress.start_phase("Creating PostgreSQL schema", len(databases))
            for db_info in databases:
                self._create_table_schema(db_info)
                self.progress.update(1)
            self.progress.finish_phase()

            # Phase 4: Migrate database row data
            self.progress.start_phase("Migrating database data", len(databases))
            for db_info in databases:
                self._migrate_database_data(db_info)
                self.progress.update(1)
            self.progress.finish_phase()

            # Phase 5: Migrate database views
            self.progress.start_phase("Migrating database views", len(databases))
            total_views = 0
            for db_info in databases:
                total_views += self._migrate_views(db_info)
                self.progress.update(1)
            self.progress.finish_phase()

            # Phase 6: Migrate standalone pages
            if self.include_standalone_pages and self.standalone_pages:
                self.progress.start_phase(
                    "Migrating standalone pages", len(self.standalone_pages)
                )
                self._migrate_standalone_pages()
                self.progress.finish_phase()

            if self.interactive_mode:
                parts = []
                if total_views:
                    parts.append(f"{total_views} view(s) saved to database_views")
                if self.standalone_pages:
                    parts.append(
                        f"{len(self.standalone_pages)} standalone page(s) migrated to "
                        "standalone_pages"
                    )
                if parts:
                    self.progress.log("✅ Migration completed — " + "; ".join(parts))
                else:
                    self.progress.log("✅ Migration completed successfully")

        finally:
            self.progress.cleanup()

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
        if self.interactive_mode:
            self.progress.log("🔗 Connected to PostgreSQL database successfully")

    def _test_notion_connection(self) -> None:
        """Test Notion API connection."""
        self.rate_limiter.rate_limited_call(self.notion.users.me)
        if self.interactive_mode:
            self.progress.log("🔗 Successfully connected to Notion API")

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _get_databases(self) -> List[Dict[str, Any]]:
        """Discover all accessible data sources and retrieve their full schemas."""
        raw_data_sources: List[Dict[str, Any]] = []
        start_cursor = None
        request_count = 0

        while True:
            response = self.rate_limiter.rate_limited_call(
                self.notion.search,
                filter={"property": "object", "value": "data_source"},
                start_cursor=start_cursor,
            )
            request_count += 1
            self.progress.update(1)

            for item in response.get("results", []):
                if not self.include_trashed and (
                    item.get("in_trash") or item.get("archived")
                ):
                    continue
                raw_data_sources.append(item)

            self.progress.set_postfix(found=len(raw_data_sources), requests=request_count)

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

            request_count += 2
            self.progress.update(2)
            self.progress.set_postfix(
                found=len(raw_data_sources),
                details=f"{i}/{len(raw_data_sources)}",
                requests=request_count,
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

    def _extract_database_title(self, database: Dict[str, Any]) -> str:
        """Extract the plain-text title from a Notion database object."""
        title_property = database.get("title", [])
        if title_property:
            return "".join(item.get("plain_text", "") for item in title_property)
        return f"Untitled_{database['id'][:8]}"

    def _discover_standalone_pages(self) -> List[Dict]:
        """Discover all standalone pages (non-database-row pages) accessible to the integration."""
        pages = []
        start_cursor = None
        request_count = 0

        while True:
            kwargs: Dict[str, Any] = {
                "filter": {"property": "object", "value": "page"},
            }
            if start_cursor:
                kwargs["start_cursor"] = start_cursor

            response = self.rate_limiter.rate_limited_call(
                self.notion.search, **kwargs
            )
            request_count += 1
            self.progress.update(1)

            for page in response.get("results", []):
                parent = page.get("parent", {})
                parent_type = parent.get("type", "")

                # Database row pages are already captured in database_content.*
                if parent_type in ("database_id", "data_source_id"):
                    continue

                if not self.include_trashed and (
                    page.get("in_trash") or page.get("archived")
                ):
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
                if self.include_trashed:
                    entry["in_trash"] = page.get("in_trash", False)
                    entry["archived"] = page.get("archived", False)

                pages.append(entry)

            self.progress.set_postfix(found=len(pages), requests=request_count)

            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")

        return pages

    # ------------------------------------------------------------------
    # Migration plan UI
    # ------------------------------------------------------------------

    def _show_migration_plan(self, databases: List[Dict]) -> None:
        """Print the migration plan table to the console."""
        self.progress.log("\nMigration Plan:")
        self.progress.log("=" * 70)
        self.progress.log(f"\nNotion databases to migrate: {len(databases)}")
        self.progress.log("")

        data = []
        for db_info in databases:
            title = db_info["title"]
            details = db_info["details"]
            properties = details["properties"]

            formula_count = sum(1 for p in properties.values() if p.get("type") == "formula")
            rollup_count = sum(1 for p in properties.values() if p.get("type") == "rollup")
            relation_count = sum(1 for p in properties.values() if p.get("type") == "relation")
            plain_count = sum(
                1 for p in properties.values()
                if p.get("type") not in {"formula", "rollup", "relation"}
                and self._is_property_type_supported(p.get("type"))
            )
            prop_desc_count = sum(
                1 for p in properties.values()
                if (p.get("description") or "").strip()
            )
            db_has_description = "✓" if any(
                item.get("plain_text", "").strip()
                for item in details.get("description", [])
            ) else "-"

            table_name = self._clean_table_name(title)
            data.append({
                "Notion Database": f"'{title}'",
                "Destination Table": f"database_content.'{table_name}'",
                "DB Desc.": db_has_description,
                "Properties": plain_count,
                "Relations": relation_count,
                "Formulas": formula_count,
                "Rollups": rollup_count,
                "Total": plain_count + relation_count + formula_count + rollup_count,
                "Prop. Desc.": prop_desc_count,
            })

        headers = [
            "Notion Database", "Destination Table", "DB Desc.",
            "Properties", "Relations", "Formulas", "Rollups", "Total", "Prop. Desc.",
        ]
        table_data = [
            [
                row["Notion Database"], row["Destination Table"], row["DB Desc."],
                row["Properties"], row["Relations"], row["Formulas"],
                row["Rollups"], row["Total"], row["Prop. Desc."],
            ]
            for row in data
        ]

        pretty_table = tabulate(
            table_data,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=[25, 32, 8, 10, 9, 8, 8, 6, 10],
        )
        self.progress.log(pretty_table)

        if self.standalone_pages:
            self.progress.log(
                f"\nStandalone pages discovered: {len(self.standalone_pages)}"
                " (block content will be extracted recursively)"
            )

        self._show_migration_analysis(databases)

        self.progress.log("")
        self.progress.log("=" * 70)

    def _get_user_confirmation(self) -> bool:
        """Ask the user for confirmation before proceeding."""
        try:
            response = input("\nProceed with migration? (y/N): ").strip().lower()
            return response in ["y", "yes"]
        except (EOFError, KeyboardInterrupt):
            return False

    def _show_migration_analysis(self, databases: List[Dict]) -> None:
        """Print skipped properties and out-of-scope relation targets."""
        from collections import defaultdict

        unsupported = self._collect_unignored_unsupported_properties(databases)

        if unsupported:
            self.progress.log("\nℹ️ Skipped fields (unsupported property type):")
            by_db: Dict[str, List] = defaultdict(list)
            db_ids: Dict[str, str] = {}
            for prop in unsupported:
                by_db[prop["original_db_name"]].append(f"{prop['property']} ({prop['type']})")
                db_ids[prop["original_db_name"]] = prop["db_id"]
            for db_name, props in by_db.items():
                self.progress.log(f"- From Table: '{db_name}' ({db_ids[db_name]})")
                for p in props:
                    self.progress.log(f"  - Field: '{p}'")
            self.progress.log(f"Total: {len(unsupported)} field(s) will be skipped")
            self.progress.log("")

        self._show_missing_relations(databases)

    def _collect_unignored_unsupported_properties(self, databases: List[Dict]) -> List[Dict]:
        """Return unsupported properties that are not explicitly ignored."""
        unsupported: List[Dict] = []
        for db_info in databases:
            db_id = db_info["id"]
            title = db_info["title"]
            for prop_name, prop_config in db_info["details"]["properties"].items():
                prop_type = prop_config.get("type") or "unknown"
                if prop_type in {"formula", "rollup"}:
                    continue
                if self._is_property_type_supported(prop_type):
                    continue
                if prop_type.lower() in self.ignored_unsupported_property_types:
                    continue
                unsupported.append({
                    "property": prop_name,
                    "type": prop_type,
                    "original_db_name": title,
                    "db_id": db_id,
                })
        return unsupported

    def _warn_about_unignored_unsupported_properties(self, databases: List[Dict]) -> None:
        """Warn in non-interactive mode when unsupported properties are not ignored."""
        unsupported = self._collect_unignored_unsupported_properties(databases)
        if not unsupported:
            return

        print("⚠️  Unsupported Notion properties will be skipped:")
        for prop in unsupported:
            print(
                "  - "
                f"{prop['original_db_name']}.{prop['property']} "
                f"({prop['type']}, data source {prop['db_id']})"
            )
        print(
            "Migration will continue. Use the matching ignore flag to suppress "
            "known unsupported property types."
        )

    def _is_property_type_supported(self, property_type: str) -> bool:
        """Return True if the property type is migrated to a column."""
        supported_types = {
            "title", "rich_text", "number", "select", "multi_select", "status",
            "date", "checkbox", "url", "email", "phone_number",
            "unique_id", "relation", "people", "files",
            "created_time", "created_by", "last_edited_time", "last_edited_by",
        }
        return property_type in supported_types

    def _show_missing_relations(self, databases: List[Dict]) -> None:
        """Warn about relation properties pointing to databases outside the migration scope."""
        from collections import defaultdict

        migrated_ids = set()
        for db in databases:
            migrated_ids.add(db["id"])
            migrated_ids.add(db["database_id"])

        issues = []
        for db_info in databases:
            for prop_name, prop_config in db_info["details"]["properties"].items():
                if prop_config.get("type") != "relation":
                    continue
                relation_config = prop_config.get("relation", {})
                target_id = (
                    relation_config.get("data_source_id")
                    or relation_config.get("database_id")
                )
                if target_id and target_id not in migrated_ids:
                    target_name = self._get_database_name_by_id(target_id)
                    issues.append({
                        "source_field": prop_name,
                        "source_table_original": db_info["title"],
                        "source_db_id": db_info["id"],
                        "target_db_name": target_name or f"Unknown ({target_id})",
                        "target_db_id": target_id,
                    })

        if issues:
            self.progress.log("\nℹ️ Table(s) referenced in relation fields, but not found:")
            by_target = defaultdict(list)
            for issue in issues:
                by_target[issue["target_db_name"]].append(issue)
            for target_name, target_issues in by_target.items():
                target_db_id = target_issues[0]["target_db_id"]
                self.progress.log(f"- Table: '{target_name}' ({target_db_id})")
                self.progress.log("  - Referenced by:")
                for issue in target_issues:
                    self.progress.log(
                        f"    - Field: '{issue['source_field']}' of table "
                        f"'{issue['source_table_original']}' ({issue['source_db_id']})"
                    )
                self.progress.log("")

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
        if self.include_trashed:
            content_columns.append(Column("in_trash", Boolean, nullable=False, server_default="false"))
            content_columns.append(Column("archived", Boolean, nullable=False, server_default="false"))

        lookup_table_configs = []
        computed_properties = []
        # Maps original prop_name → pg column name for supported non-computed properties.
        non_computed_props: List[tuple] = []  # (prop_name, prop_config, pg_column_or_None)

        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")

            if prop_type in ["formula", "rollup"]:
                self.skipped_properties.append({
                    "table": table_name,
                    "property": prop_name,
                    "type": prop_type,
                    "reason": "Computed property — definition stored in database_computed_fields",
                    "original_db_name": title,
                    "db_id": data_source_id,
                })
                computed_properties.append((prop_name, prop_type, prop_config))
                continue

            prop_col_name = self._clean_property_col_name(prop_name)
            column = self.property_mapper.get_postgres_column(prop_col_name, prop_config)
            if column is not None:
                content_columns.append(column)
                non_computed_props.append((prop_name, prop_config, prop_col_name))
            else:
                if (prop_type or "unknown").lower() not in self.ignored_unsupported_property_types:
                    self.skipped_properties.append({
                        "table": table_name,
                        "property": prop_name,
                        "type": prop_type,
                        "reason": "Unsupported property type",
                        "original_db_name": title,
                        "db_id": data_source_id,
                    })
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
        if self.include_trashed:
            meta_columns.append(Column("in_trash", Boolean))
            meta_columns.append(Column("archived", Boolean))

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
        if self.include_trashed:
            meta_row["in_trash"] = database_details.get("in_trash", False)
            meta_row["archived"] = database_details.get("archived", False)

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

        self.progress.set_postfix(table=table_name)

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

        self.progress.start_detail(f"↳ {table.name} rows", unit="pages")
        try:
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

                self.progress.set_detail_postfix(
                    batches=page_batches,
                    fetched=total_pages,
                    has_more=response.get("has_more", False),
                )
                self.progress.set_postfix(table=table.name, pages=total_pages)

                if not response.get("has_more", False):
                    break
                start_cursor = response.get("next_cursor")
        finally:
            self.progress.finish_detail()

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

        self.progress.set_postfix(
            table=self._clean_table_name(db_info["title"]), views=len(rows)
        )
        return len(rows)

    def _insert_pages_batch(
        self, table: Table, pages: List[Dict], properties: Dict
    ) -> None:
        """Insert a batch of database row pages into database_content."""
        if not pages:
            return

        rows = []
        for page in pages:
            # Skip trashed rows unless include_trashed is set
            if not self.include_trashed and (
                page.get("in_trash") or page.get("archived")
            ):
                self.progress.update_detail(1)
                continue

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
            if self.include_trashed:
                row_data["in_trash"] = page.get("in_trash", False)
                row_data["archived"] = page.get("archived", False)

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
            self.progress.update_detail(1)

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

            try:
                with self.db_engine.connect() as conn:
                    conn.execute(text(f"""
                        ALTER TABLE database_content.{table_name}
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({prop_col})
                        REFERENCES database_select_options.{option_table_name}(value)
                        ON DELETE SET NULL
                    """))
                    conn.commit()
            except Exception as e:
                self.progress.log(
                    f"⚠️  Failed to create foreign key for {table_name}.{prop_col}: {e}"
                )

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
        if self.include_trashed:
            page_tree_cols += ",\n            in_trash BOOLEAN NOT NULL DEFAULT FALSE"
            page_tree_cols += ",\n            archived BOOLEAN NOT NULL DEFAULT FALSE"

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
            if self.include_trashed:
                insert_data["in_trash"] = page.get("in_trash", False)
                insert_data["archived"] = page.get("archived", False)

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

            self.progress.update(1)
            self.progress.set_postfix(page=page["title"][:30])

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

    def _get_database_name_by_id(self, notion_id: str) -> Optional[str]:
        """Retrieve a database or data source title by Notion ID."""
        try:
            response = self.rate_limiter.rate_limited_call(
                self.notion.databases.retrieve,
                database_id=notion_id,
            )
            return self._extract_database_title(response)
        except Exception:
            pass
        try:
            response = self.rate_limiter.rate_limited_call(
                self.notion.request,
                f"data_sources/{notion_id}",
                "GET",
            )
            database_id = (response.get("parent") or {}).get("database_id")
            if database_id:
                database = self.rate_limiter.rate_limited_call(
                    self.notion.databases.retrieve,
                    database_id=database_id,
                )
                return self._extract_data_source_title(response, database)
            title_property = response.get("title", [])
            if title_property:
                return "".join(item.get("plain_text", "") for item in title_property)
        except Exception:
            pass
        return None
