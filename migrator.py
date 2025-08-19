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
    
    def __init__(self, notion_token: str, db_connection: Engine, verbose: bool = True):
        """
        Initialize the migrator.
        
        Args:
            notion_token: Notion integration token
            db_connection: SQLAlchemy engine for PostgreSQL connection
            verbose: Enable verbose output with progress bars
        """
        self.notion = Client(auth=notion_token)
        self.db_engine = db_connection
        self.verbose = verbose
        self.progress = ProgressTracker(verbose)
        self.rate_limiter = RateLimiter(requests_per_second=2.5)
        self.metadata = MetaData()
        self.property_mapper = NotionPropertyMapper()
        
        # Track created tables and lookup tables
        self.created_tables: Dict[str, Table] = {}
        self.lookup_tables: Dict[str, Table] = {}
    
    def run(self) -> None:
        """Run the complete migration process."""
        try:
            self.progress.log("🚀 Starting Notion workspace migration to PostgreSQL...")
            
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
            
            self.progress.log("✅ Migration completed successfully!")
            
        finally:
            self.progress.cleanup()
    
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
            
            # Create option table names preview (using double underscore for clarity)
            option_table_names = []
            for field_name in all_option_fields:
                clean_field_name = self._clean_table_name(field_name)
                option_table_names.append(f"{table_name}__{clean_field_name}")
            
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
        from sqlalchemy import Column, String
        columns.append(Column("notion_id", String(36), primary_key=True))
        
        for prop_name, prop_config in properties.items():
            column = self.property_mapper.get_postgres_column(prop_name, prop_config)
            if column is not None:
                columns.append(column)
            
            # Track multi-select properties for lookup tables
            if self.property_mapper.needs_lookup_table(prop_config):
                lookup_table_configs.append((prop_name, prop_config))
        
        # Create main table
        table = Table(table_name, self.metadata, *columns)
        
        # Add table comment
        if details.get("description"):
            description_text = "".join(item.get("plain_text", "") 
                                     for item in details["description"])
            table.comment = description_text
        
        # Create option tables for select and multi-select properties
        for prop_name, prop_config in lookup_table_configs:
            option_table_name = self.property_mapper.get_lookup_table_name(table_name, prop_name)
            option_table = Table(
                option_table_name,
                self.metadata,
                Column("id", String(36), primary_key=True),
                Column("value", String(255), nullable=False),
                Column("color", String(50))
            )
            self.lookup_tables[f"{table_name}_{prop_name}"] = option_table
        
        self.created_tables[db_id] = table
        
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
    
    def _insert_pages_batch(self, table: Table, pages: List[Dict], properties: Dict) -> None:
        """Insert a batch of pages into the PostgreSQL table."""
        if not pages:
            return
        
        rows = []
        for page in pages:
            row_data = {"notion_id": page["id"]}
            
            page_properties = page.get("properties", {})
            for prop_name, prop_config in properties.items():
                if prop_config.get("type") in ["formula", "rollup"]:
                    continue
                
                prop_data = page_properties.get(prop_name, {})
                value = self.property_mapper.extract_property_value(
                    prop_data, prop_config["type"]
                )
                row_data[prop_name] = value
            
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
