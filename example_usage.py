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
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(sa.text("SELECT 1"))
    
    print("✅ Connected to database successfully")
    
    # Initialize migrator
    migrator = NotionMigrator(
        notion_token=notion_token,
        db_connection=engine,
        verbose=True
    )
    
    # Run migration
    migrator.run()
    
    return 0


if __name__ == "__main__":
    exit(main())
