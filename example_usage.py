"""
Example usage of the Notion to PostgreSQL migration tool.
"""

import os
import sqlalchemy as sa
from migrator import NotionMigrator


def main():
    # Get credentials from environment variables
    notion_token = os.getenv("NOTION_FULL_READ_ONLY")
    if not notion_token:
        print("Please set NOTION_FULL_READ_ONLY environment variable")
        return
    
    # Database connection string - adjust as needed
    db_url = os.getenv("LOCAL_POSTGRES_URL")
    
    # Create database connection
    engine = sa.create_engine(db_url)
    
    # Initialize migrator
    migrator = NotionMigrator(
        notion_token=notion_token,
        db_connection=engine,
        verbose=True,
        extract_page_content=False  # Set to True to extract free-form page content (slower)
    )
    
    # Run migration
    migrator.run()
    
    return 0


if __name__ == "__main__":
    exit(main())
