"""Command line interface for the Notion to PostgreSQL migration tool."""

import argparse
import logging
import os
import sys

import sqlalchemy as sa

from .migrator import NotionMigrator
from .notion_http import PLAN_BUDGETS
from .telemetry import LOGGER_NAME, JSONFormatter


def main() -> int:
    """Run the CLI.

    :returns: Process exit code.
    """
    parser = argparse.ArgumentParser(
        description="Migrate Notion workspace to PostgreSQL database"
    )
    parser.add_argument("--notion-token", help="Notion integration token (or set NOTION_TOKEN env var)")
    parser.add_argument("--database-url", help="PostgreSQL connection string (or set DATABASE_URL env var)")
    parser.add_argument(
        "--plan",
        choices=sorted(PLAN_BUDGETS),
        default="free",
        help="Notion workspace plan; sets the request budget (default: free)",
    )
    parser.add_argument(
        "--fetch-page-content",
        action="store_true",
        help="Fetch and store the full block tree for every database row.",
    )
    args = parser.parse_args()

    notion_token = args.notion_token or os.getenv("NOTION_TOKEN")
    if not notion_token:
        parser.error("Notion token required: set --notion-token or NOTION_TOKEN")
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("Database URL required: set --database-url or DATABASE_URL")

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JSONFormatter())
    package_logger = logging.getLogger(LOGGER_NAME)
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.INFO)

    NotionMigrator(
        notion_token=notion_token,
        db_connection=sa.create_engine(database_url),
        fetch_page_content=args.fetch_page_content,
        plan=args.plan,
    ).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
