"""
Notion Workspace to PostgreSQL Migration Tool

A Python package for migrating Notion workspaces to PostgreSQL databases.
"""

try:
    from .migrator import NotionMigrator
    from . import cli
except ImportError:
    from migrator import NotionMigrator
    import cli

__version__ = "1.0.0"
__all__ = ["NotionMigrator", "cli"]