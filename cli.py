"""
Command line interface for the Notion to PostgreSQL migration tool.
"""

import argparse
import os
import sys
import sqlalchemy as sa
try:
    from .migrator import NotionMigrator
except ImportError:
    from migrator import NotionMigrator


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate Notion workspace to PostgreSQL database"
    )
    
    parser.add_argument(
        "--notion-token",
        help="Notion integration token (or set NOTION_TOKEN env var)"
    )
    
    parser.add_argument(
        "--database-url",
        help="PostgreSQL connection string (or set DATABASE_URL env var)"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Run in non-interactive mode (skip validation steps and progress bars)"
    )
    
    args = parser.parse_args()
    
    # Get credentials
    notion_token = args.notion_token or os.getenv("NOTION_TOKEN")
    if not notion_token:
        print("❌ Error: Notion token required. Set --notion-token or NOTION_TOKEN env var")
        return 1
    
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ Error: Database URL required. Set --database-url or DATABASE_URL env var")
        return 1
    
    # Create database connection
    engine = sa.create_engine(database_url)
    
    # Initialize and run migrator
    migrator = NotionMigrator(
        notion_token=notion_token,
        db_connection=engine,
        interactive_mode=not args.quiet
    )
    
    migrator.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
