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
    
    def __init__(self, notion_token: str, db_connection: Engine, verbose: bool = True, 
                 extract_page_content: bool = False):
        """
        Initialize the migrator.
        
        Args:
            notion_token: Notion integration token
            db_connection: SQLAlchemy engine for PostgreSQL connection
            verbose: Enable verbose output with progress bars
            extract_page_content: Extract free-form content from page bodies (slower migration)
        """
        self.notion = Client(auth=notion_token)
        self.db_engine = db_connection
        self.verbose = verbose
        self.extract_page_content = extract_page_content
        self.progress = ProgressTracker(verbose)
        self.rate_limiter = RateLimiter(requests_per_second=2.5)
        self.metadata = MetaData()
        self.property_mapper = NotionPropertyMapper()
        
        # Track created tables and lookup tables
        self.created_tables: Dict[str, Table] = {}
        self.lookup_tables: Dict[str, Table] = {}
        
        # Track skipped properties for summary
        self.skipped_properties: List[Dict[str, str]] = []
    
    def run(self) -> None:
        """Run the complete migration process."""
        try:
            self.progress.log("🚀 Starting Notion workspace migration to PostgreSQL...")
            
            # Check if schemas already exist
            self._check_clean_database()
            
            # Phase 1: Discover databases
            self.progress.start_phase("🔍 Discovering databases", None)
            databases = self._get_databases()
            self.progress.finish_phase()
            
            if not databases:
                self.progress.log("❌ No databases found. Please share databases with your integration.")
                return
            
            # Show migration plan
            self._show_migration_plan(databases)
            
            # Get user confirmation
            if not self._get_user_confirmation():
                self.progress.log("Migration cancelled by user.")
                return
            
            # Phase 2: Create schema
            self.progress.start_phase("🏗️  Creating database schema", len(databases))
            for db_info in databases:
                self._create_table_schema(db_info)
                self.progress.update(1)
            self.progress.finish_phase()
            
            # Phase 3: Migrate data
            self.progress.start_phase("📊 Migrating data", len(databases))
            for db_info in databases:
                self._migrate_database_data(db_info)
                self.progress.update(1)
            self.progress.finish_phase()
            
            # Phase 4: Validate relations
            self.progress.log("\n🔍 Validating relation references...")
            self._validate_relations()
            
            # Phase 5: Show summary of skipped properties
            self._show_skipped_properties_summary()
            
            self.progress.log("✅ Migration completed successfully!")
            
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
                    "❌ Schema 'content' already exists. "
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
                    "❌ Schema 'select_options' already exists. "
                    "Please drop existing schemas or use a clean database:\n"
                    "DROP SCHEMA content CASCADE;\n"
                    "DROP SCHEMA select_options CASCADE;"
                )
            
            self.progress.log("✅ Database is clean - no conflicting schemas found")
    
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
        
        self.progress.log(f"Found {len(databases)} databases")
        
        # TEMPORARY: Filter for specific databases only
        allowed_databases = ["Data", "Backups", "Automations", "Cloud spaces", "Cloud providers", "External drives"]
        filtered_databases = []
        for db in databases:
            db_title = self._extract_database_title(db)
            if db_title in allowed_databases:
                filtered_databases.append(db)
        
        self.progress.log(f"Filtered to {len(filtered_databases)} allowed databases: {', '.join(allowed_databases)}")
        
        # Get detailed schema for each database
        detailed_databases = []
        for i, db in enumerate(filtered_databases, 1):
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
                found=len(filtered_databases), 
                details=f"{i}/{len(filtered_databases)}", 
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
        self.progress.log(f"\n📋 Migration Plan:")
        self.progress.log("=" * 70)
        self.progress.log(f"Databases to migrate: {len(databases)}")
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
                "Database": title,
                "Table Name": table_name,
                "Properties": property_count,
                "Relations": relation_count,
                "Select Option Tables": option_tables_count,
                "Option Table Names": ", ".join(option_table_names) if option_table_names else "-"
            })
        
        # Create pretty table
        headers = ["Database", "Table Name", "Properties", "Relations", "Select Option Tables", "Option Table Names"]
        table_data = []
        
        for row in data:
            table_data.append([
                row["Database"],
                row["Table Name"], 
                row["Properties"],
                row["Relations"],
                row["Select Option Tables"],
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
        
        self.progress.log("")
        self.progress.log("=" * 70)
    
    def _get_user_confirmation(self) -> bool:
        """Get user confirmation to proceed with migration."""
        try:
            response = input("\nProceed with migration? (y/N): ").strip().lower()
            return response in ['y', 'yes']
        except (EOFError, KeyboardInterrupt):
            return False
    
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
        
        # Add page_content column if feature is enabled
        if self.extract_page_content:
            columns.append(Column("page_content", Text, comment="Free-form content from the bottom of the Notion page"))
        
        for prop_name, prop_config in properties.items():
            prop_type = prop_config.get("type")
            
            # Track skipped properties for summary
            if prop_type in ["formula", "rollup"]:
                self.skipped_properties.append({
                    "table": table_name,
                    "property": prop_name,
                    "type": prop_type,
                    "reason": "Computed property - values are derived from other data"
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
                    "property": prop_name,
                    "type": prop_type,
                    "reason": "Unsupported property type"
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
                row_data["page_content"] = page_content
            
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
    
    def _validate_relations(self) -> None:
        """Check for relation references to tables not in migration scope."""
        # Get all migrated table IDs for quick lookup
        migrated_table_ids = set(self.created_tables.keys())
        
        issues = []
        total_relations = 0
        
        # Check each migrated table for relation fields
        for db_info in self._get_databases():
            db_id = db_info["id"]
            if db_id not in migrated_table_ids:
                continue
                
            table_name = self._clean_table_name(db_info["title"])
            properties = db_info["details"]["properties"]
            
            # Find relation properties
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") == "relation":
                    relation_config = prop_config.get("relation", {})
                    target_db_id = relation_config.get("database_id")
                    
                    if target_db_id:
                        total_relations += 1
                        if target_db_id not in migrated_table_ids:
                            # Find the target database name
                            target_db_name = self._get_database_name_by_id(target_db_id)
                            issues.append({
                                "source_table": table_name,
                                "source_field": self._clean_table_name(prop_name),
                                "target_db_name": target_db_name or f"Unknown ({target_db_id[:8]}...)",
                                "target_db_id": target_db_id
                            })
        
        # Report results
        if issues:
            self.progress.log("⚠️  Found relation references to tables not in migration scope:")
            self.progress.log("=" * 70)
            
            # Group by target database for cleaner output
            from collections import defaultdict
            by_target = defaultdict(list)
            for issue in issues:
                by_target[issue["target_db_name"]].append(issue)
            
            for target_name, target_issues in by_target.items():
                self.progress.log(f"📋 Target: {target_name}")
                for issue in target_issues:
                    self.progress.log(f"   ↳ {issue['source_table']}.{issue['source_field']}")
                self.progress.log("")
            
            self.progress.log(f"💡 To fix: Share these {len(by_target)} databases with your Notion integration")
            self.progress.log("   or remove the relation fields from the migration scope.")
            self.progress.log("=" * 70)
        else:
            self.progress.log("✅ All relation references are valid (within migration scope)")
        
        self.progress.log(f"📊 Summary: {total_relations} relation fields checked, {len(issues)} issues found")
    
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
        
        # For blocks we don't handle, return empty string
        return ""
    
    def _extract_rich_text_plain(self, rich_text_array: List[Dict]) -> str:
        """Extract plain text from Notion rich text array."""
        if not rich_text_array:
            return ""
        return "".join(item.get("plain_text", "") for item in rich_text_array)
    
    def _show_skipped_properties_summary(self) -> None:
        """Show summary of properties that were not migrated."""
        if not self.skipped_properties:
            self.progress.log("✅ All supported properties were migrated successfully")
            return
        
        self.progress.log(f"\n📋 Migration Summary - Skipped Properties:")
        self.progress.log("=" * 70)
        
        # Group by reason for cleaner output
        from collections import defaultdict
        by_reason = defaultdict(list)
        for prop in self.skipped_properties:
            by_reason[prop["reason"]].append(prop)
        
        for reason, props in by_reason.items():
            self.progress.log(f"🚫 {reason}:")
            
            # Group by table for even cleaner output
            by_table = defaultdict(list)
            for prop in props:
                by_table[prop["table"]].append(prop)
            
            for table, table_props in by_table.items():
                prop_list = [f"{p['property']} ({p['type']})" for p in table_props]
                self.progress.log(f"   📊 {table}: {', '.join(prop_list)}")
            self.progress.log("")
        
        total_skipped = len(self.skipped_properties)
        unique_tables = len(set(p["table"] for p in self.skipped_properties))
        
        self.progress.log(f"📊 Total: {total_skipped} properties skipped across {unique_tables} tables")
        
        if any(p["type"] in ["formula", "rollup"] for p in self.skipped_properties):
            self.progress.log("💡 Note: Formula and rollup values change dynamically in Notion")
            self.progress.log("   Consider recreating them as PostgreSQL views or computed columns")
        
        self.progress.log("=" * 70)
    
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
