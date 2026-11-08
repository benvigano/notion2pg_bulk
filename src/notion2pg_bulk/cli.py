"""Command line interface: ``notion2pg-bulk``."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional, Sequence

from .notion_http import PLAN_BUDGETS
from .orchestrate import snapshot
from .telemetry import LOGGER_NAME, JSONFormatter


def build_parser() -> argparse.ArgumentParser:
    """:returns: The CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="notion2pg-bulk",
        description="Snapshot a Notion workspace into an empty PostgreSQL database.",
    )
    parser.add_argument("--notion-token", help="Notion integration token (or set NOTION_TOKEN)")
    parser.add_argument("--database-url", help="PostgreSQL URL of an empty database (or set DATABASE_URL)")
    parser.add_argument(
        "--plan",
        choices=sorted(PLAN_BUDGETS),
        default="free",
        help="Notion workspace plan; sets the request budget (default: free)",
    )
    parser.add_argument(
        "--no-row-bodies",
        dest="row_bodies",
        action="store_false",
        help="Skip the bodies (blocks) of data source rows",
    )
    parser.add_argument("--propagation-id", help="Identifier carried by every telemetry event")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the CLI. Telemetry is written to stderr as one JSON object per line.

    :param argv: Arguments (defaults to ``sys.argv[1:]``).
    :returns: Process exit code (failures raise, exiting non-zero).
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    notion_token = args.notion_token or os.getenv("NOTION_TOKEN")
    if not notion_token:
        parser.error("a Notion token is required: --notion-token or NOTION_TOKEN")
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("a database URL is required: --database-url or DATABASE_URL")

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JSONFormatter())
    package_logger = logging.getLogger(LOGGER_NAME)
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.INFO)

    snapshot(
        notion_token,
        database_url,
        plan=args.plan,
        row_bodies=args.row_bodies,
        propagation_id=args.propagation_id,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
