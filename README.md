# notion2pg_bulk

- Automatically discovers and migrates **all databases in a workspace** regardless of their views or position in the page structure
- Preserves **database descriptions and property descriptions**
- Supports **relation properties** using PostgreSQL arrays
- **Select field support**: Single and multi-select options stored in separate lookup tables with foreign key constraints
- Complies with Notion API limits (3 requests/second average)

## Installation
```bash
pip install notion2pg-bulk
```

## Setup and Usage

### Requirements
- a **Notion API key** connected to all the databased that you wish to migrate (note: Notion now allows to connect databases ditectly from the integration configuration page, under the 'Access' tab. For quickly connecting **all databases** in a workspace, just tick all top level pages.
- PostgreSQL database connection URL

### Interactive Mode
**On (Default):**
- Dispays detected databases and fields and asks for confirmation before running the migration
- Progress bar
- Shows post-migration notes

**Off:**
(`--quiet` flag in CLI or set `interactive_mode=False` in Python API)
- `.run()` runs migration directly and silently

### CLI
**Options:**
- `--notion-token`: Notion integration token (or set `NOTION_TOKEN` env var)
- `--database-url`: PostgreSQL connection string (or set `DATABASE_URL` env var)
- `--quiet`: Run in non-interactive mode (skip validation steps and progress bars)
- `--extract-page-content`: Extract free-form content from page bodies (slower migration)

**Examples:**
```bash
# Interactive mode (default)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..."

# Non-interactive mode (skip validation steps)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --quiet

# Extract page content (slower but includes free-form content)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --extract-page-content

# Non-interactive with page content extraction
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --quiet --extract-page-content
```

### Python API:
```python
from notion2pg_bulk import NotionMigrator
import sqlalchemy as sa

engine = sa.create_engine('postgresql://user:password@localhost/dbname')
migrator = NotionMigrator(
    notion_token="your_notion_integration_token",
    db_connection=engine,
    interactive_mode=True,
    extract_page_content=True,
)
migrator.run()
```

## Property Type Mapping

| Notion Property | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| Title | `TEXT` | Primary identifier |
| Rich Text | `TEXT` | Preserved as markdown with formatting |
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

## Additional Page Content Mode

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

**Post-migration analysis:**
When page content extraction is enabled, the migrator provides a comprehensive summary of:
- Unsupported block types found and their locations (grouped by record)
- Embedded databases that were referenced in page content but not connected to the integration (with full database IDs)
