# Notion Workspace to PostgreSQL Migration Tool

A Python package that performs a full migration of a Notion workspace to a PostgreSQL database.

## Features

- **Complete workspace migration**: Migrates all databases and their content
- **Table and field descriptions**: Preserves database and property descriptions as PostgreSQL comments
- **Relation field support**: Handles relation properties using PostgreSQL arrays
- **Multi-select support**: Stores multi-select options in separate lookup tables
- **Progress tracking**: Displays detailed progress bars during migration
- **Rate limiting**: Automatically complies with Notion API rate limits
- **Automatic discovery**: Finds all databases already shared with your integration

## Prerequisites

### 1. Create a Notion Integration

1. Go to [Notion Developers](https://www.notion.so/my-integrations)
2. Click "New integration"
3. Give it a name and select your workspace
4. **Configure capabilities** (following least privilege principle):
   - ✅ **Read content** - Required to read database schemas and data
   - ✅ **Read comments** - Optional, for comment migration if needed
   - ❌ **No user information** - Not needed for database migration
   - ❌ **Insert content** - Not needed (we're only reading)
   - ❌ **Update content** - Not needed (we're only reading)
5. Copy the "Internal Integration Token"

### 2. Share Databases with Integration

**Important**: Each database you want to migrate must be manually shared with your integration:

1. Open each database in Notion
2. Click the "..." menu in the top right
3. Select "Add connections"
4. Find and select your integration
5. Click "Confirm"

The migration tool will automatically discover all databases that have been shared with your integration.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

No package installation needed! Just run the scripts directly.

## Usage

### Direct Script Usage (Recommended)

```bash
# Set environment variables (adjust variable names as needed)
export NOTION_FULL_READONLY="your_notion_integration_token"
export LOCAL_POSTGRES_URL="postgresql://user:password@localhost/dbname"

# Run the example script
python example_usage.py
```

### Command Line Interface

```bash
# Run CLI directly
python cli.py --notion-token "your_token" --database-url "postgresql://..."

# Or with environment variables
export NOTION_TOKEN="your_notion_integration_token"
export DATABASE_URL="postgresql://user:password@localhost/dbname"
python cli.py
```

### Python API (if imported as module)

```python
from migrator import NotionMigrator
import sqlalchemy as sa

# Create database connection
engine = sa.create_engine('postgresql://user:password@localhost/dbname')

# Initialize migrator
migrator = NotionMigrator(
    notion_token="your_notion_integration_token",
    db_connection=engine,
    verbose=True  # Default: shows progress bars and detailed output
)

# Run migration
migrator.run()
```

## What Gets Migrated

- **Database structure**: Each Notion database becomes a PostgreSQL table
- **Property types**: Mapped to appropriate PostgreSQL column types
- **Relations**: Stored as arrays of related record IDs
- **Multi-select**: Options stored in separate `TABLENAME_FIELDNAME` tables
- **Descriptions**: Database and property descriptions become PostgreSQL comments
- **Data**: All database entries (pages) are migrated as table rows

## Property Type Mapping

| Notion Property | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| Title | TEXT | Primary identifier |
| Rich Text | TEXT | Formatted as plain text |
| Number | NUMERIC | |
| Select | TEXT | Single option value |
| Multi-select | TEXT[] | Array of option values + lookup table |
| Date | TIMESTAMP | |
| Checkbox | BOOLEAN | |
| URL | TEXT | |
| Email | TEXT | |
| Phone | TEXT | |
| Relation | TEXT[] | Array of related page IDs |
| People | TEXT[] | Array of user IDs |
| Files | TEXT[] | Array of file URLs |
| Created time | TIMESTAMP | |
| Created by | TEXT | User ID |
| Last edited time | TIMESTAMP | |
| Last edited by | TEXT | User ID |

## Limitations

- **Formulas and Rollups**: Skipped (these are computed values)
- **Database views**: Not migrated (focus on raw data)
- **Page content**: Only database entries are migrated, not free-form pages

## Rate Limiting

The tool automatically respects Notion's API rate limits (3 requests per second average) and handles HTTP 429 responses appropriately.
