# notion2pg_bulk

Migrates an entire Notion workspace to PostgreSQL fully automatically: databases (schema + data + metadata), database views, formula/rollup definitions, and - optionally - standalone pages with their full block tree.

## Installation
```bash
pip install notion2pg-bulk
```

## Set up

### Requirements
- A **Notion API key** connected to all pages you wish to migrate (Notion integration settings → Access tab; tick all top-level pages to cover the full workspace)
- PostgreSQL connection URL

## Run

### Python API
```python
from notion2pg_bulk import NotionMigrator
import sqlalchemy as sa

engine = sa.create_engine('postgresql://user:password@localhost/dbname')
migrator = NotionMigrator(
    notion_token="your_notion_integration_token",
    db_connection=engine,
    interactive_mode=True,
    include_trashed=False,
    fetch_page_content=False,       # set True to also store the block tree per row
    include_standalone_pages=True,  # set False to skip standalone page discovery and migration
    ignored_unsupported_property_types=["button"],
)
migrator.run()
```

### CLI

**Options:**
- `--notion-token`: Notion integration token (or set `NOTION_TOKEN` env var)
- `--database-url`: PostgreSQL connection string (or set `DATABASE_URL` env var)
- `--quiet`: Non-interactive mode — no confirmation prompt or progress bars; all safety checks still run
- `--include-trashed`: Add `in_trash` and `archived` columns to all tables and retain items that arrive flagged archived. See API-caused limitations for why this does not recover truly deleted items.
- `--fetch-page-content`: Fetch and store the full block tree for every database row in a `page_content` JSONB column. Omitted by default — see [Database page content](#database-page-content) for the full trade-off.

  > ⚠️ **This flag can increase migration time by orders of magnitude.** Without it, an entire database is fetched in `ceil(rows / 100)` bulk API calls. With it, every single row requires at least one additional API call (`blocks.children.list`), recursively repeated for nested blocks. A 500-row database goes from ~5 requests to 500+, adding a minimum of several minutes at the enforced API rate — more for pages with deep block trees.
- `--no-standalone-pages`: Skip the workspace-wide standalone page search and all block migration for standalone pages. Only database rows are migrated; the `standalone_pages` schema is not created. Enabled by default.
- `--ignore-unsupported-button`: Skip Notion `button` properties silently instead of reporting them as skipped
- `--ignore-unsupported-place`: Skip Notion `place` properties silently instead of reporting them as skipped
- `--ignore-unsupported-verification`: Skip Notion `verification` properties silently instead of reporting them as skipped

**Examples:**
```bash
# Interactive mode (default)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..."

# Non-interactive mode
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --quiet

# Retain archived items and include trash/archived columns
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --include-trashed

# Suppress known unsupported property warnings
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --ignore-unsupported-button
```

### Interactive mode

**On (default):** displays the migration plan, asks for confirmation, shows a progress bar.

**Off** (`--quiet` / `interactive_mode=False`): no prompts or progress bars; pre-flight checks (empty database, Notion connection, table-name collisions) always run and fail immediately on error.

Unsupported property types not covered by an `--ignore-unsupported-*` flag still print a warning in non-interactive mode, then migration continues.

### Database requirements

The target PostgreSQL database must be **completely empty**: only the default `public` schema with no tables or views. Any other schema, table, or view causes the migrator to abort before making any changes. Partial re-runs are not supported.

## PostgreSQL schema layout

| Schema | Contents |
|--------|----------|
| `database_content` | One table per Notion database — all migrated rows (`notion_id` PK, built-in audit columns, one `property_*` column per supported property, and optionally `page_content` JSONB when `--fetch-page-content` is set) |
| `database_select_options` | One lookup table per `select` / `multi_select` / `status` field (`{table}__{field}`), with FK constraints back to `database_content` |
| `database_computed_fields` | One table per Notion database — `formula` and `rollup` property definitions as JSONB |
| `database_property_meta` | One table per Notion database — full Notion property specification for every non-computed property (one row per property). Columns: `property_id`, `property_name`, `notion_type`, `pg_column` (the resulting `database_content` column name, or NULL if the type is unsupported), `description`, `config` JSONB (complete Notion property config — crucially includes relation target `database_id`, select/multi-select/status option lists, and all other per-property metadata) |
| `database_views` | One table per Notion database — for each view of a database that appears any where in the workspace, stores view configuration (type, filters, sorts, layout...) as JSONB |
| `database_meta` | One single-row table per Notion data source — database container metadata (`notion_database_id`, `notion_data_source_id` PK), title, description, cover, icon, URL, inline flag, data sources list, audit timestamps |
| `standalone_pages` | Two fixed tables: `page_tree` (one row per standalone page: title, parent, depth, cover, icon, URL, audit timestamps) and `block_content` (one row per block: type, position, parent block, audit timestamps, content JSONB) |

### Column naming

Built-in row columns (`notion_id`, `created_time`, `last_edited_time`, `created_by`, `last_edited_by`, `cover`, `icon`, `url`, and the optional `page_content`) are unprefixed. User-defined property columns are prefixed with `property_` (e.g. a Notion property named `Due Date` becomes `property_due_date`). This prevents collisions between built-in column names and user-chosen property names.

## Property type mapping

| Notion property | PostgreSQL type | Notes |
|----------------|-----------------|-------|
| Title | `TEXT` | |
| Rich Text | `JSONB` | Full Notion `rich_text` array, preserving annotations, mentions, equations, links, and API object shape |
| Number | `NUMERIC` | |
| Select | `VARCHAR(255)` | Lookup table with FK |
| Multi-select | `VARCHAR(255)[]` | Lookup table; no DB-level FK (PostgreSQL arrays) |
| Status | `VARCHAR(255)` | Lookup table with FK; status groups not preserved |
| Date | `TSTZRANGE` | Full range (start → end); point dates stored as `[start, start]`. Date-only values are stored as midnight UTC instants; named IANA timezones from Notion are not stored separately. |
| Checkbox | `BOOLEAN` | |
| URL | `TEXT` | |
| Email | `VARCHAR(200)` | |
| Phone | `VARCHAR(200)` | |
| Unique ID | `TEXT` | Formatted as `PREFIX-N` or `N` |
| Relation | `TEXT[]` | Array of related page IDs; arrays >25 items are fully paginated |
| People | `TEXT[]` | Array of user IDs; arrays >25 items are fully paginated |
| Files | `JSONB` | Full Notion `files` property array as returned by the API — see API-caused limitations |
| Created time | `TIMESTAMP` | |
| Created by | `VARCHAR(36)` | User ID |
| Last edited time | `TIMESTAMP` | |
| Last edited by | `VARCHAR(36)` | User ID |
| Formula | — | Expression stored in `database_computed_fields`; per-row computed values are not stored |
| Rollup | — | Relation/property/function triple stored in `database_computed_fields`; per-row computed values are not stored |
| Button | — | ⚠️ Skipped — no data exposed by the API |
| Place | — | ⚠️ Skipped — API returns `null` for all place values |
| Verification | — | ⚠️ Skipped by this implementation; wiki-specific verification values are not stored |

## Database page content

Notion database rows carry two distinct layers of data:

1. **Structured properties** — the typed fields (title, text, number, date, relation, etc.) returned as part of the bulk database query. These are fully captured by default into individual `property_*` columns. No additional API calls are needed.

2. **Free-form page body** — the block tree a user types below the property table (paragraphs, headings, images, callouts, etc.). This is **not** included in the bulk query response. Fetching it requires a separate `blocks.children.list` call per row, recursively repeated for any nested blocks.

### Default behaviour (no `--fetch-page-content`)

The `page_content` column is **not created**. Only the structured property data is stored. For databases used purely as structured data stores (the common case), nothing meaningful is lost: the free-form page body in these rows is typically empty or carries notes that are not needed for data reconstruction.

**What is not captured:** any text, images, embeds, callouts, to-dos, code blocks, or other block-type content typed directly on the page below the property table.

### With `--fetch-page-content`

A `page_content JSONB` column is added to every `database_content` table. Each value is a recursive array of block objects, where each entry contains `id`, `type`, `content` (the type-specific payload), audit fields (`created_time`, `last_edited_time`, `created_by`, `last_edited_by`), and a `children` array (recursively populated). `child_database` blocks are not recursed into.

**Cost:** at least one extra API call per row, more for pages with nested blocks. On a database with 500 rows this adds a minimum of 500 extra requests; at the enforced rate of 2.5 req/s that is at least 3 minutes of additional API time before accounting for nested block depth or retry delays.

## API-caused limitations

These limits are inherent to the Notion API, and are not currently circumventable - or at least not at the time of writing.

### Directly affecting captured content

- **Discovery is not guaranteed to be exhaustive**: workspace discovery depends on the Notion Search endpoint. Notion documents Search as unsuitable for exhaustive enumeration of every accessible document, and indexing can change while a migration runs. Even with standalone page migration enabled and recursive block fetching, the API does not guarantee that every page in an accessible workspace is discovered. This especially applies to trashed pages (`--include-trashed`).
- **Some block types are unsupported on standalone pages** (applies only when `include_standalone_pages=True`, only applies to standalone pages, not to database pages): block trees stored in `standalone_pages.block_content` cannot faithfully reproduce every page layout. Two API gaps are responsible:
  - **Unsupported blocks (`unsupported`)** — some block types (ex: forms, buttons...) are identified only by a `block_type` string (ex: `form`, `button`...). The endpoint does not return any content for these blocks, and does not recurse into them, so anything built from these blocks is not in the export.
  - **Database views (`child_database`)** — while all databases and database views that appear anywhere in the workspace are fully exported (see [PostgreSQL schema layout](#postgresql-schema-layout) above), their placement in the global page tree can not. In stnadalone page fetching, if the page contains a database or database view, the API only returns the database title, but no reference to the specific view configuration (which instead would allow full page layout reconstruction).

### Other content and workspace features not captured

- **Page locks**: lock state is not part of the page object returned by the content APIs, so restored pages cannot preserve the original lock state from this snapshot.
- **Page edit history and version history**: Notion page history is not exposed through the public API, so a restore can only recreate the latest captured state.
- **Workspace permissions, sharing, and admin settings**: page-level ACLs, sharing configuration, groups, guests, teamspaces, and workspace administration settings are outside the content API used by this project.
- **Workspace-level billing, roles, and automations**: billing state, role configuration, workspace automations, notifications, and similar account-level behavior are not represented in the exported content objects.
- **External embed rendering details**: embed blocks preserve the API-visible URL/configuration, but Notion's rendered preview metadata and third-party rendering behavior are not fully exposed for exact recreation.
- **Custom emojis**: custom emoji references may appear in API payloads, but exact workspace custom emoji catalog restoration is outside the exported content model.
- **Notion-hosted file and image URLs**: uploaded file, image, icon, cover, comment attachment, and media block URLs are temporary signed links that expire. The migration stores the API response, not durable file binaries.

## Roadmap

Possible future enhancements:

- **Workspace restoration script**: reconstruct the full Notion workspace from `database_computed_fields`, `database_property_meta`, `database_views`, `database_content`, `database_select_options`, `database_meta`, and `standalone_pages` via the Notion API.
- **Comments**: store API-readable unresolved comments for pages and blocks. Resolved comments remain outside the public comments API.
- **Lossless date property storage**: store raw Notion date property objects in JSONB to preserve the exact API shape, including date-only values and `time_zone`. This improves one-to-one restore fidelity but makes direct SQL date querying looser unless paired with generated columns, views, or secondary typed columns.
- **Computed value snapshots**: store best-effort per-row formula and rollup values alongside their definitions. The Notion API can return read-only computed values, but formula and rollup values that depend on large or multi-layer relations can be truncated or incomplete.
- **Durable file storage**: add a background worker that downloads each temporary Notion file URL immediately after the API response and streams the binary to persistent storage such as S3 or a local file store, then saves the permanent URI in PostgreSQL.
- **User directory snapshots**: store full API-visible user objects instead of only user IDs, subject to the token's user capabilities.
- **Relation foreign keys**: add PostgreSQL foreign keys between `database_content` tables when relation targets are part of the same migration (today relations are stored as page ID arrays only).
