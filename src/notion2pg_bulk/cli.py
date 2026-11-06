"""
Command line interface for the Notion to PostgreSQL migration tool.
"""

import argparse
import os
import sys
import sqlalchemy as sa
from .migrator import NotionMigrator


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
        help="Non-interactive mode: no confirmation prompt or progress bars (all safety checks still run)",
    )

    parser.add_argument(
        "--include-trashed",
        action="store_true",
        help=(
            "Add in_trash and archived columns to all tables and keep items that "
            "arrive flagged archived. Note: the Notion search and query APIs do not "
            "surface items that have been moved to Trash, so truly deleted items are "
            "not recoverable via this tool regardless of this flag."
        ),
    )

    parser.add_argument(
        "--fetch-page-content",
        action="store_true",
        help=(
            "Fetch and store the full block tree for every database row in a "
            "page_content JSONB column. Requires at least one extra API call per row "
            "(blocks.children.list, recursively), which significantly slows migration. "
            "Omitted by default; see README for what is and is not captured without it."
        ),
    )

    parser.add_argument(
        "--no-standalone-pages",
        action="store_false",
        dest="include_standalone_pages",
        help=(
            "Skip the workspace-wide standalone page search and block migration. "
            "Only database rows are migrated; the standalone_pages schema is not created."
        ),
    )

    parser.add_argument(
        "--ignore-unsupported-button",
        action="store_true",
        help="Skip Notion button properties silently instead of reporting them as skipped",
    )

    parser.add_argument(
        "--ignore-unsupported-place",
        action="store_true",
        help="Skip Notion place properties silently instead of reporting them as skipped",
    )

    parser.add_argument(
        "--ignore-unsupported-verification",
        action="store_true",
        help="Skip Notion verification properties silently instead of reporting them as skipped",
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

    # pool_pre_ping validates each connection before use, preventing silent
    # hangs if the PostgreSQL server closes idle connections during migration.
    engine = sa.create_engine(database_url, pool_pre_ping=True)

    ignored_unsupported_property_types = []
    if args.ignore_unsupported_button:
        ignored_unsupported_property_types.append("button")
    if args.ignore_unsupported_place:
        ignored_unsupported_property_types.append("place")
    if args.ignore_unsupported_verification:
        ignored_unsupported_property_types.append("verification")

    # Initialize and run migrator
    migrator = NotionMigrator(
        notion_token=notion_token,
        db_connection=engine,
        interactive_mode=not args.quiet,
        include_trashed=args.include_trashed,
        fetch_page_content=args.fetch_page_content,
        include_standalone_pages=args.include_standalone_pages,
        ignored_unsupported_property_types=ignored_unsupported_property_types,
    )
    
    migrator.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
