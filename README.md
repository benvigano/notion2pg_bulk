# Notion Workspace to PostgreSQL Migration Tool

A Python package that performs a full migration of a Notion workspace to a PostgreSQL database.

- Automatically discovers and migrates **all databases in a workspace** regardless of their views or position in the page structure
- Preserves **database and property descriptions**
- Supports **relation properties** using PostgreSQL arrays
- **Select field support**: Single and multi-select options stored in separate lookup tables with foreign key constraints
- Complies with Notion API limits (3 requests/second average)

## Setup and Usage

### 1. Create a Notion Integration

1. Go to [Notion Developers](https://www.notion.so/my-integrations)
2. Click "New integration"
3. Give it a name and select your workspace
4. **Configure permissions** (following least privilege principle):
   - **Read content** - Required to read database schemas and data
   - **Read comments** - Required for property descriptions

5. Copy the "Internal Integration Token"

### 2. Grant Database Access

**Option A: From Integration Page (faster for selecting all databases)**
1. In your integration settings, go to "Access" tab
2. Select databases singularly, or select all top-level pages to quickly grant access to all databases within them

**Option B: GUI**
1. Open each database in Notion
2. Click the "..." menu in the top right
3. Select "Add connections"
4. Find and select your integration

### 3. Install and Run
**CLI:**
```bash
python cli.py --notion-token "your_token" --database-url "postgresql://..."
```

**Python API:**
```python
from migrator import NotionMigrator
import sqlalchemy as sa

engine = sa.create_engine('postgresql://user:password@localhost/dbname')
migrator = NotionMigrator(
    notion_token="your_notion_integration_token",
    db_connection=engine,
    verbose=True
)
migrator.run()
```

## Property Type Mapping

| Notion Property | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| Title | `TEXT` | Primary identifier |
| Rich Text | `TEXT` | Formatted as plain text |
| Number | `NUMERIC` | |
| Select | `TEXT` | Single option value + lookup table with foreign key |
| Multi-select | `TEXT[]` | Array of option values + lookup table with check constraints |
| Date | `TIMESTAMP` | |
| Checkbox | `BOOLEAN` | |
| URL | `TEXT` | |
| Email | `TEXT` | |
| Phone | `TEXT` | |
| Relation | `TEXT[]` | Array of related page IDs |
| People | `TEXT[]` | Array of user IDs |
| Files | `TEXT[]` | Array of file URLs |
| Created time | `TIMESTAMP` | |
| Created by | `TEXT` | User ID |
| Last edited time | `TIMESTAMP` | |
| Last edited by | `TEXT` | User ID |
| Formula | - | ⚠️ Skipped (not supported by Notion API) |
| Rollup | - | ⚠️ Skipped (not supported by Notion API) |

## `additional_page_content`

The migrator can optionally extract free-form content from database pages. Extracted content is stored in the `additional_page_content` column as plain text.

```python
migrator = NotionMigrator(
    notion_token=token,
    db_connection=engine,
    extract_page_content=True  # Default is false
)
```

**Supported blocks:**
- Paragraph text
- Headings (all levels)
- Bulleted and numbered lists
- Code blocks
- To-do items
- Callouts
- Toggle blocks
- Dividers
- Embedded databases (ID reference only)

**Unsupported blocks:**
- Images and media files
- Complex block types (equations, embeds, etc.)
- Block formatting and styling
- Page hierarchies and relationships