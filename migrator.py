"""
Main migration class for Notion workspace to PostgreSQL migration.
"""

import time
from typing import Dict, List, Any, Optional
from tabulate import tabulate
from notion_client import Client
from notion_client.errors import APIResponseError
from sqlalchemy import Engine, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert

try:
    from .schema_mapper import NotionPropertyMapper
    from .progress_tracker import ProgressTracker
    from .rate_limiter import RateLimiter
except ImportError:
    from schema_mapper import NotionPropertyMapper
    from progress_tracker import ProgressTracker
    from rate_limiter import RateLimiter


class NotionMigrator:
    """Main class for migrating Notion workspace to PostgreSQL."""
    
    def __init__(self, notion_token: str, db_connection: Engine, interactive_mode: bool = True, 
                 extract_page_content: bool = False):
        """
        Initialize the migrator.
        
        Args:
            notion_token: Notion integration token
            db_connection: SQLAlchemy engine for PostgreSQL connection
            interactive_mode: Enable interactive mode with progress bars and validation steps
            extract_page_content: Extract free-form content from page bodies (slower migration)
        """
        self.notion = Client(auth=notion_token)
        self.db_engine = db_connection
        self.interactive_mode = interactive_mode
        self.extract_page_content = extract_page_content
        self.progress = ProgressTracker(interactive_mode)
        self.rate_limiter = RateLimiter(requests_per_second=2.5)
        self.metadata = MetaData()
        self.property_mapper = NotionPropertyMapper()
        
        # Test database connection during initialization
        if self.interactive_mode:
            self._test_database_connection()
        
        # Track created tables and lookup tables
        self.created_tables: Dict[str, Table] = {}
        self.lookup_tables: Dict[str, Table] = {}
        
        # Track skipped properties for summary
        self.skipped_properties: List[Dict[str, str]] = []
        
        # Track embedded databases found in page content
        self.embedded_databases: List[Dict[str, str]] = []
    
    def run(self) -> None:
        """Run the complete migration process."""
        try:
            # Check database schemas and test Notion connection
            if self.interactive_mode:
                self._check_clean_database()
                self._test_notion_connection()
            
            # Phase 1: Discover Notion databases
            self.progress.start_phase("🔎 Discovering Notion databases", None)
            databases = self._get_databases()
            self.progress.finish_phase()
            
            if not databases:
                self.progress.log("No Notion databases found. Please share databases with your integration.")
                return
            
            # Show migration plan with analysis (only in interactive mode)
            if self.interactive_mode:
                self._show_migration_plan(databases)
                
                # Get user confirmation
                if not self._get_user_confirmation():
                    self.progress.log("Migration cancelled by user.")
                    return
            
            # Phase 2: Create PostgreSQL schema
            self.progress.start_phase("Creating PostgreSQL schema", len(databases))
            for db_info in databases:
                self._create_table_schema(db_info)
                self.progress.update(1)
            self.progress.finish_phase()
            
            # Phase 3: Migrate data
            self.progress.start_phase("Migrating data", len(databases))
            for db_info in databases:
                self._migrate_database_data(db_info)
                self.progress.update(1)
            self.progress.finish_phase()
            
            if self.interactive_mode:
                self.progress.log("✅ Migration completed successfully")
                
                # Post-migration analysis (only embedded databases)
                if self.extract_page_content:
                    self.progress.log("\n" + "=" * 70)
                    self.progress.log("MIGRATION NOTES")
                    self.progress.log("=" * 70)
                    self._show_embedded_databases_summary()
            
        finally:
            self.progress.cleanup()
    
    def _check_clean_database(self) -> None:
        """Check that required schemas don't already exist."""
        with self.db_engine.connect() as conn:
            # Check if content schema exists
            result = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'content'"
            ))
            if result.fetchone():
                raise ValueError(
                    "Schema 'content' already exists. "
                    "Please drop existing schemas or use a clean database:\n"
                    "DROP SCHEMA content CASCADE;\n"
                    "DROP SCHEMA select_options CASCADE;"
                )
            
            # Check if select_options schema exists  
            result = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'select_options'"
            ))
            if result.fetchone():
                raise ValueError(
                    "Schema 'select_options' already exists. "
                    "Please drop existing schemas or use a clean database:\n"
                    "DROP SCHEMA content CASCADE;\n"
                    "DROP SCHEMA select_options CASCADE;"
                )
    
    def _test_database_connection(self) -> None:
        """Test PostgreSQL database connection."""
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            if self.interactive_mode:
                self.progress.log("🔗 Connected to PostgreSQL database successfully")
        except Exception as e:
            raise ValueError(f"Failed to connect to PostgreSQL database: {e}")
    
    def _test_notion_connection(self) -> None:
        """Test Notion API connection."""
        try:
            self.rate_limiter.rate_limited_call(self.notion.users.me)
            if self.interactive_mode:
                self.progress.log("🔗 Successfully connected to Notion API")
        except Exception as e:
            raise ValueError(f"Failed to connect to Notion API: {e}")
    
    def _get_databases(self) -> List[Dict[str, Any]]:
        """Get all accessible databases with their detailed schemas."""
        databases = []
        start_cursor = None
        request_count = 0
        
        # First, discover all databases
        while True:
            response = self.rate_limiter.rate_limited_call(
                self.notion.search,
                filter={"property": "object", "value": "database"},
                start_cursor=start_cursor
            )
            
            request_count += 1
            self.progress.update(1)
            
            databases.extend(response.get("results", []))
            self.progress.set_postfix(found=len(databases), requests=request_count)
            
            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")






        # TEMPORARY: Filter for specific databases only
        allowed_databases = ["Data", "Backups", "Automations", "Cloud spaces", "Cloud providers", "External drives"]
        filtered_databases = []
        for db in databases:
            db_title = self._extract_database_title(db)
            if db_title in allowed_databases:
                filtered_databases.append(db)

        databases = filtered_databases






        
        # Get detailed schema for each database
        detailed_databases = []
        for i, db in enumerate(databases, 1):
            db_id = db["id"]
            db_title = self._extract_database_title(db)
            
            # Get detailed database info
            db_details = self.rate_limiter.rate_limited_call(
                self.notion.databases.retrieve,
                database_id=db_id
            )
            
            request_count += 1
            self.progress.update(1)
            self.progress.set_postfix(
                found=len(databases), 
                details=f"{i}/{len(databases)}", 
                requests=request_count
            )
            
            detailed_databases.append({
                "id": db_id,
                "title": db_title,
                "details": db_details
            })
        
        return detailed_databases
    

    
    def _extract_database_title(self, database: Dict[str, Any]) -> str:
        """Extract database title from Notion database object."""
        title_property = database.get("title", [])
        if title_property:
            return "".join(item.get("plain_text", "") for item in title_property)
        return f"Untitled_{database['id'][:8]}"
    

    
    def _show_migration_plan(self, databases: List[Dict]) -> None:
        """Show what will be migrated."""
        self.progress.log(f"\nMigration Plan:")
        self.progress.log("=" * 70)
        self.progress.log(f"\nNotion databases to migrate: {len(databases)}")
        self.progress.log("")
        
        # Prepare data for dataframe
        data = []
        for db_info in databases:
            title = db_info["title"]
            properties = db_info["details"]["properties"]
            
            # Count different types of properties
            property_count = len([p for p in properties.values() 
                                if p.get("type") not in ["formula", "rollup"]])
            
            # Get fields that will get option tables (select and multi-select)
            select_fields = [name for name, prop in properties.items() 
                           if prop.get("type") == "select"]
            multi_select_fields = [name for name, prop in properties.items() 
                                 if prop.get("type") == "multi_select"]
            all_option_fields = select_fields + multi_select_fields
            option_tables_count = len(all_option_fields)
            
            # Count relation properties
            relation_count = len([p for p in properties.values() 
                                if p.get("type") == "relation"])
            
            # Get table name that will be created
            table_name = self._clean_table_name(title)
            
            # Create option table names preview (in select_options schema)
            option_table_names = []
            for field_name in all_option_fields:
                clean_field_name = self._clean_table_name(field_name)
                option_table_names.append(f"select_options.{table_name}__{clean_field_name}")
            
            data.append({
                "Notion Database": f"'{title}'",
                "Destination Table": f"'{table_name}'",
                "Properties": property_count,
                "Relations": relation_count,
                "Select Options Tables": option_tables_count,
                "Option Table Names": ", ".join([f"'{name}'" for name in option_table_names]) if option_table_names else "-"
            })
        
        # Create pretty table
        headers = ["Notion Database", "Destination Table", "Properties", "Relations", "Select Options Tables", "Option Table Names"]
        table_data = []
        
        for row in data:
            table_data.append([
                row["Notion Database"],
                row["Destination Table"], 
                row["Properties"],
                row["Relations"],
                row["Select Options Tables"],
                row["Option Table Names"]
            ])
        
        # Use tabulate for pretty printing
        pretty_table = tabulate(
            table_data,
            headers=headers,
            tablefmt="grid",
            maxcolwidths=[25, 20, 10, 9, 12, 40]
        )
        
        self.progress.log(pretty_table)
        
        # Show analysis as part of migration plan
        self._show_migration_analysis(databases)
        
        self.progress.log("")
        self.progress.log("=" * 70)
    
    def _get_user_confirmation(self) -> bool:
        """Get user confirmation before proceeding with migration."""
        try:
            response = input("\nProceed with migration? (y/N): ").strip().lower()
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            return False
    
    def _show_migration_analysis(self, databases: List[Dict]) -> None:
        """Analyze and show properties that will be skipped and missing relations."""
        skipped_properties = []
        
        # Analyze each database for skipped properties
        for db_info in databases:
            db_id = db_info["id"]
            title = db_info["title"]
            properties = db_info["details"]["properties"]
            
            for prop_name, prop_config in properties.items():
                prop_type = prop_config.get("type")
                
                if prop_type in ["formula", "rollup"]:
                    skipped_properties.append({
                        "property": prop_name,
                        "type": prop_type,
                        "reason": "Computed property - values are derived from other data",
                        "original_db_name": title,
                        "db_id": db_id
                    })
                elif not self._is_property_type_supported(prop_type):
                    skipped_properties.append({
                        "property": prop_name,
                        "type": prop_type,
                        "reason": "Unsupported property type",
                        "original_db_name": title,
                        "db_id": db_id
                    })
        
        if not skipped_properties:
            return
        
        self.progress.log(f"\nℹ️ Skipped Fields (unsupported by Notion API):")
        
        # Group by type (formula vs rollup)
        from collections import defaultdict
        by_type = defaultdict(list)
        for prop in skipped_properties:
            prop_type = prop["type"]
            if prop_type in ["formula", "rollup"]:
                by_type[prop_type].append(prop)
            else:
                by_type["unsupported"].append(prop)
        
        # Show formulas grouped by Notion database
        if "formula" in by_type:
            self.progress.log("Formula Fields:")
            by_db = defaultdict(list)
            for prop in by_type["formula"]:
                original_db_name = prop.get("original_db_name", "unknown")
                db_id = prop.get("db_id", "unknown")
                by_db[original_db_name].append({"property": prop["property"], "db_id": db_id})
            
            for db_name, prop_data in by_db.items():
                db_id = prop_data[0]["db_id"]
                self.progress.log(f"- From Table: '{db_name}' ({db_id})")
                for prop_info in prop_data:
                    self.progress.log(f"  - Field: '{prop_info['property']}'")
            self.progress.log("")
        
        # Show rollups grouped by Notion database
        if "rollup" in by_type:
            self.progress.log("Rollup Fields:")
            by_db = defaultdict(list)
            for prop in by_type["rollup"]:
                original_db_name = prop.get("original_db_name", "unknown")
                db_id = prop.get("db_id", "unknown")
                by_db[original_db_name].append({"property": prop["property"], "db_id": db_id})
            
            for db_name, prop_data in by_db.items():
                db_id = prop_data[0]["db_id"]
                self.progress.log(f"- From Table: '{db_name}' ({db_id})")
                for prop_info in prop_data:
                    self.progress.log(f"  - Field: '{prop_info['property']}'")
            self.progress.log("")
        
        # Show unsupported
        if "unsupported" in by_type:
            self.progress.log("Unsupported fields:")
            by_db = defaultdict(list)
            for prop in by_type["unsupported"]:
                original_db_name = prop.get("original_db_name", "unknown")
                by_db[original_db_name].append(f"{prop['property']} ({prop['type']})")
            
            for db_name, props in by_db.items():
                # Get database ID for this section
                db_id = "unknown"
                for prop in by_type["unsupported"]:
                    if prop.get("original_db_name") == db_name:
                        db_id = prop.get("db_id", "unknown")
                        break
                self.progress.log(f"- From Table: '{db_name}' ({db_id})")
                for prop in props:
                    self.progress.log(f"  - Field: '{prop}'")
            self.progress.log("")
        
        total_skipped = len(skipped_properties)
        if total_skipped > 0:
            self.progress.log(f"Total: {total_skipped} fields will be skipped")
        
        # Also show missing relation references
        self._show_missing_relations(databases)
    
    def _is_property_type_supported(self, property_type: str) -> bool:
        """Check if a property type is supported for migration."""
        supported_types = {
            "title", "rich_text", "number", "select", "multi_select", 
            "date", "checkbox", "url", "email", "phone_number", 
            "relation", "people", "files", "created_time", "created_by", 
            "last_edited_time", "last_edited_by"
        }
        return property_type in supported_types
    
    def _show_missing_relations(self, databases: List[Dict]) -> None:
        """Show relation references to tables not in migration scope."""
        # Get all database IDs that will be migrated
        migrated_table_ids = set(db["id"] for db in databases)
        
        issues = []
        
        # Check each database for relation fields
        for db_info in databases:
            db_id = db_info["id"]
            original_table_name = db_info["title"]
            properties = db_info["details"]["properties"]
            
            # Find relation properties
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") == "relation":
                    relation_config = prop_config.get("relation", {})
                    target_db_id = relation_config.get("database_id")
                    
                    if target_db_id and target_db_id not in migrated_table_ids:
                        # Find the target database name
                        target_db_name = self._get_database_name_by_id(target_db_id)
                        issues.append({
                            "source_field": prop_name,
                            "source_table_original": original_table_name,
                            "source_db_id": db_id,
                            "target_db_name": target_db_name or f"Unknown ({target_db_id})",
                            "target_db_id": target_db_id
                        })
        
        # Report results only if there are issues
        if issues:
            self.progress.log("\nℹ️ Table(s) referenced in relation fields, but not found:")
            
            # Group by target database for cleaner output
            from collections import defaultdict
            by_target = defaultdict(list)
            for issue in issues:
                by_target[issue["target_db_name"]].append(issue)
            
            for target_name, target_issues in by_target.items():
                target_db_id = target_issues[0]["target_db_id"]
                self.progress.log(f"- Table: '{target_name}' ({target_db_id})")
                self.progress.log("  - Referenced by:")
                for issue in target_issues:
                    self.progress.log(f"    - Field: '{issue['source_field']}' of table '{issue['source_table_original']}' ({issue['source_db_id']})")
                self.progress.log("")
    
    def _create_table_schema(self, db_info: Dict) -> None:
        """Create PostgreSQL table schema for a Notion database."""
        db_id = db_info["id"]
        title = db_info["title"]
        details = db_info["details"]
        properties = details["properties"]
        
        # Clean table name (replace spaces and special chars)
        table_name = self._clean_table_name(title)
        
        # Create main table columns
        columns = []
        lookup_table_configs = []
        
        # Add notion_id as primary key
        from sqlalchemy import Column, String, Text
        columns.append(Column("notion_id", String(36), primary_key=True))
        
        # Add additional_page_content column if feature is enabled
        if self.extract_page_content:
            columns.append(Column("additional_page_content", Text, comment="Free-form content from the bottom of the Notion page"))
        
        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")
            
            # Track skipped properties for summary
            if prop_type in ["formula", "rollup"]:
                self.skipped_properties.append({
                    "table": table_name,
                    "property": prop_name,  # Keep original property name
                    "type": prop_type,
                    "reason": "Computed property - values are derived from other data",
                    "original_db_name": title,  # Store original database name
                    "db_id": db_id  # Store database ID
                })
                continue
            
            # Clean property name for SQL compatibility
            clean_prop_name = self._clean_table_name(prop_name)
            column = self.property_mapper.get_postgres_column(clean_prop_name, prop_config)
            if column is not None:
                columns.append(column)
            else:
                # Track unsupported property types
                self.skipped_properties.append({
                    "table": table_name,
                    "property": prop_name,  # Keep original property name
                    "type": prop_type,
                    "reason": "Unsupported property type",
                    "original_db_name": title,  # Store original database name
                    "db_id": db_id  # Store database ID
                })
            
            # Track multi-select properties for lookup tables
            if self.property_mapper.needs_lookup_table(prop_config):
                lookup_table_configs.append((prop_name, prop_config))
        
        # Create main table in 'content' schema
        table = Table(table_name, self.metadata, *columns, schema='content')
        
        # Add table comment
        if details.get("description"):
            description_text = "".join(item.get("plain_text", "") 
                                     for item in details["description"])
            table.comment = description_text
        
        # Create option tables for select and multi-select properties in 'select_options' schema
        for prop_name, prop_config in lookup_table_configs:
            option_table_name = self.property_mapper.get_lookup_table_name(table_name, prop_name)
            option_table = Table(
                option_table_name,
                self.metadata,
                Column("id", String(36), primary_key=True),
                Column("value", String(255), nullable=False, unique=True),
                Column("color", String(50)),
                schema='select_options'
            )
            self.lookup_tables[f"{table_name}_{prop_name}"] = option_table
        
        self.created_tables[db_id] = table
        
        # Create schemas if they don't exist, then create tables
        with self.db_engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS content"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS select_options"))
            conn.commit()
        
        # Create tables in database
        self.metadata.create_all(self.db_engine)
        
        self.progress.set_postfix(table=table_name)
    
    def _migrate_database_data(self, db_info: Dict) -> None:
        """Migrate all data from a Notion database."""
        db_id = db_info["id"]
        table = self.created_tables[db_id]
        details = db_info["details"]
        properties = details["properties"]
        
        # Set current table context for embedded database tracking
        self._current_table_name = table.name
        self._current_table_original_name = db_info["title"]  # Track original name
        
        # Query all pages in the database
        start_cursor = None
        total_pages = 0
        
        while True:
            response = self.rate_limiter.rate_limited_call(
                self.notion.databases.query,
                database_id=db_id,
                start_cursor=start_cursor,
                page_size=100
            )
            
            pages = response.get("results", [])
            if pages:
                self._insert_pages_batch(table, pages, properties)
                total_pages += len(pages)
            
            if not response.get("has_more", False):
                break
            start_cursor = response.get("next_cursor")
            
            self.progress.set_postfix(
                table=table.name,
                pages=total_pages
            )
        
        # Populate lookup tables
        self._populate_lookup_tables(table.name, properties)
        
        # Add foreign key constraints after data is populated
        self._add_select_foreign_keys(table.name, properties)
    
    def _insert_pages_batch(self, table: Table, pages: List[Dict], properties: Dict) -> None:
        """Insert a batch of pages into the PostgreSQL table."""
        if not pages:
            return
        
        rows = []
        for page in pages:
            row_data = {"notion_id": page["id"]}
            
            # Extract page properties
            page_properties = page.get("properties", {})
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") in ["formula", "rollup"]:
                    continue
                
                # Use cleaned property name for SQL compatibility
                clean_prop_name = self._clean_table_name(prop_name)
                prop_data = page_properties.get(prop_name, {})
                value = self.property_mapper.extract_property_value(
                    prop_data, prop_config["type"]
                )
                row_data[clean_prop_name] = value
            
            # Extract page content (blocks below properties) if feature is enabled
            if self.extract_page_content:
                page_content = self._extract_page_content(page["id"])
                row_data["additional_page_content"] = page_content
            
            rows.append(row_data)
        
        # Insert batch
        with self.db_engine.connect() as conn:
            conn.execute(table.insert(), rows)
            conn.commit()
    
    def _populate_lookup_tables(self, table_name: str, properties: Dict) -> None:
        """Populate option tables for select and multi-select properties."""
        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")
            if prop_type not in ["select", "multi_select"]:
                continue
            
            lookup_key = f"{table_name}_{prop_name}"
            if lookup_key not in self.lookup_tables:
                continue
            
            option_table = self.lookup_tables[lookup_key]
            # Get options from either select or multi_select config
            options = prop_config.get(prop_type, {}).get("options", [])
            
            if options:
                rows = [
                    {
                        "id": opt["id"],
                        "value": opt["name"],
                        "color": opt.get("color", "default")
                    }
                    for opt in options
                ]
                
                with self.db_engine.connect() as conn:
                    # Use INSERT ON CONFLICT DO NOTHING for idempotency
                    stmt = insert(option_table).values(rows)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
                    conn.execute(stmt)
                    conn.commit()
    
    def _clean_table_name(self, name: str) -> str:
        """Clean table name to be PostgreSQL compatible."""
        # Replace spaces and special characters with underscores
        import re
        cleaned = re.sub(r'[^\w]', '_', name)
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        # Ensure it's not empty and starts with letter
        if not cleaned or cleaned[0].isdigit():
            cleaned = f"table_{cleaned}"
        # Limit length to reasonable size (PostgreSQL supports up to 63 chars)
        if len(cleaned) > 50:
            cleaned = cleaned[:50].rstrip('_')
        return cleaned.lower()
    

    def _get_database_name_by_id(self, db_id: str) -> str:
        """Get database name by ID from the original discovery results."""
        # This is a simplified lookup - in a full implementation we'd cache this
        try:
            response = self.rate_limiter.rate_limited_call(
                self.notion.databases.retrieve,
                database_id=db_id
            )
            title_property = response.get("title", [])
            if title_property:
                return "".join(item.get("plain_text", "") for item in title_property)
            return None
        except:
            return None
    
    def _extract_page_content(self, page_id: str) -> str:
        """Extract text content from page blocks below database properties."""
        try:
            # Set current context for embedded database tracking
            self._current_page_id = page_id
            
            # Get all blocks for this page
            blocks = []
            start_cursor = None
            
            while True:
                response = self.rate_limiter.rate_limited_call(
                    self.notion.blocks.children.list,
                    block_id=page_id,
                    start_cursor=start_cursor,
                    page_size=100
                )
                
                blocks.extend(response.get("results", []))
                
                if not response.get("has_more", False):
                    break
                start_cursor = response.get("next_cursor")
            
            # Extract text from blocks
            if not blocks:
                return ""
            
            text_content = []
            for block in blocks:
                block_text = self._extract_block_text(block)
                if block_text.strip():
                    text_content.append(block_text)
            
            return "\n\n".join(text_content) if text_content else ""
            
        except Exception as e:
            # If we can't get page content, just return empty string
            # This prevents the migration from failing due to page content issues
            return ""
    
    def _extract_block_text(self, block: Dict[str, Any]) -> str:
        """Extract plain text from a Notion block."""
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        
        # Handle different block types that contain text
        if block_type in ["paragraph", "heading_1", "heading_2", "heading_3", 
                         "bulleted_list_item", "numbered_list_item", "quote", 
                         "callout", "toggle"]:
            rich_text = block_data.get("rich_text", [])
            return self._extract_rich_text_plain(rich_text)
        
        elif block_type == "code":
            rich_text = block_data.get("rich_text", [])
            language = block_data.get("language", "")
            code_text = self._extract_rich_text_plain(rich_text)
            return f"```{language}\n{code_text}\n```" if code_text else ""
        
        elif block_type == "to_do":
            rich_text = block_data.get("rich_text", [])
            checked = block_data.get("checked", False)
            text = self._extract_rich_text_plain(rich_text)
            checkbox = "☑" if checked else "☐"
            return f"{checkbox} {text}" if text else ""
        
        elif block_type == "divider":
            return "---"
        
        elif block_type == "child_database":
            # Handle embedded databases
            database_id = block.get("id", "")
            title = block_data.get("title", "Embedded Database")
            
            # Track this embedded database
            self.embedded_databases.append({
                "database_id": database_id,
                "title": title,
                "parent_table": getattr(self, '_current_table_name', 'unknown'),
                "parent_table_original": getattr(self, '_current_table_original_name', 'unknown'),
                "page_id": getattr(self, '_current_page_id', 'unknown'),
                "migrated": database_id in self.created_tables
            })
            
            # Return reference to PostgreSQL table if migrated
            if database_id in self.created_tables:
                table_name = self._clean_table_name(title)
                return f"[Embedded Database: {title} → PostgreSQL table: content.{table_name}]"
            else:
                return f"[Embedded Database: {title} (ID: {database_id}) - Not migrated]"
        
        # For blocks we don't handle, return empty string
        return ""
    
    def _extract_rich_text_plain(self, rich_text_array: List[Dict]) -> str:
        """Extract plain text from Notion rich text array."""
        if not rich_text_array:
            return ""
        return "".join(item.get("plain_text", "") for item in rich_text_array)
    

    def _add_select_foreign_keys(self, table_name: str, properties: Dict) -> None:
        """Add foreign key constraints from main table select fields to option tables."""
        
        for prop_name, prop_config in properties.items():
            if not self.property_mapper.needs_lookup_table(prop_config):
                continue
                
            prop_type = prop_config.get("type")
            clean_prop_name = self._clean_table_name(prop_name)
            option_table_name = self.property_mapper.get_lookup_table_name(table_name, prop_name)
            
            # Use separate transaction for each constraint to avoid rollback issues
            try:
                with self.db_engine.connect() as conn:
                    if prop_type == "select":
                        # For single select: direct foreign key
                        constraint_name = f"fk_{table_name}_{clean_prop_name}"
                        sql = f"""
                        ALTER TABLE content.{table_name} 
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({clean_prop_name}) 
                        REFERENCES select_options.{option_table_name}(value)
                        ON DELETE SET NULL
                        """
                        conn.execute(text(sql))
                        conn.commit()
                        
                    elif prop_type == "multi_select":
                        # For multi-select: check constraint that validates array elements
                        constraint_name = f"fk_{table_name}_{clean_prop_name}_check"
                        sql = f"""
                        ALTER TABLE content.{table_name} 
                        ADD CONSTRAINT {constraint_name}
                        CHECK (
                            {clean_prop_name} IS NULL OR 
                            (SELECT COUNT(*) 
                             FROM unnest({clean_prop_name}) AS option_value 
                             WHERE option_value NOT IN (
                                 SELECT value FROM select_options.{option_table_name}
                             )) = 0
                        )
                        """
                        conn.execute(text(sql))
                        conn.commit()
                    
            except Exception as e:
                self.progress.log(f"⚠️  Failed to create foreign key for {table_name}.{clean_prop_name}: {e}")
                # Continue with next constraint even if this one fails
    
    def _show_embedded_databases_summary(self) -> None:
        """Show summary of embedded databases found in page content."""
        if not self.embedded_databases:
            return
        
        self.progress.log(f"\nℹ️ Found table(s) embedded as additional content, but source table(s) not found:")
        self.progress.log("=" * 70)
        
        # Group by migrated status
        not_migrated = [db for db in self.embedded_databases if not db["migrated"]]
                
        if not_migrated:
            for db in not_migrated:
                self.progress.log(f"- Table: '{db['title']}' ({db['database_id']})")
                self.progress.log(f"  - Found in record {db.get('page_id', 'unknown')} of table '{db['parent_table_original']}'") 
            self.progress.log("")