# notion2pg_bulk

_Python package to snapshot a Notion workspace to a PostgreSQL database, within its [coverage limits](#coverage-limits)._

## Snapshot scope

### Not captured

| Item | Reason |
|---|---|
| Media: `image`, `video`, `audio`, `file`, `pdf` blocks; `files` properties; page and database covers; file icons | Notion-hosted files: not retrievable; the API returns only URLs that expire after 1 h ([file object](https://developers.notion.com/reference/file-object)). External links: choice; keeping them while dropping hosted files means handling the two variants separately for each media type. |
| `embed` blocks | An embed is read back with only `url`, a temporary signed link when it is backed by an uploaded file; `file_upload` is a create/update parameter only ([embed](https://developers.notion.com/reference/block#embed)). Notion-hosted and external embeds can't be told apart, so neither is kept. |
| Database templates | Choice. Friction: +1 paginated request per data source ([list templates](https://developers.notion.com/reference/list-data-source-templates)) plus a body fetch per template. |
| Comments | Choice. Friction: ≥1 request per page and per block; only unresolved comments are returned ([list comments](https://developers.notion.com/reference/list-comments)). |
| Automations, AI autofill | Not exposed by the [API](https://developers.notion.com/reference/intro). |
| Trashed content | Choice. Friction: separate discovery ([search](https://developers.notion.com/reference/post-search) `filter.in_trash`) plus trash/restore state tracking. |
| Archived rows (`is_archived`) | Choice. Friction: +1 paginated query per data source per full pull ([query](https://developers.notion.com/reference/query-a-data-source#archived-pages)). Queries exclude archived rows by default, so archiving a row removes it from the snapshot. |
| `button` properties | Not returned: schema and values are `{}` ([properties](https://developers.notion.com/reference/property-object), [values](https://developers.notion.com/reference/page-property-values#unsupported-properties)). |
| Users | Choice. Friction: +1 request ([get users](https://developers.notion.com/reference/get-users)) and a users table. User IDs appear only as values of `people`, `created_by` and `last_edited_by` properties. |
| Built-in page metadata (`created_time`, `last_edited_time`, `created_by`, `last_edited_by` of every page and block) | Choice. Friction: none (present on every page and block object). Captured where a data source defines the matching property type. |
| View tab order | Not returned: the view object has no order field; `position` is write-only ([view](https://developers.notion.com/reference/view), [positioning](https://developers.notion.com/guides/data-apis/working-with-views#view-positioning)). |
| Content of `unsupported` blocks | Not returned: only `unsupported.block_type` is exposed ([block](https://developers.notion.com/reference/block#unsupported)). |
| `formula` and `rollup` property values (definitions are captured) | Unreliable: values depending on >25 relations or several relation layers may be wrong ([query → "Formula and rollup limitations"](https://developers.notion.com/reference/query-a-data-source)), and values can come back as `unsupported` ([values](https://developers.notion.com/reference/page-property-values#unsupported-formula)). They also change without any row edit. Single values can be read via [retrieve a page property](https://developers.notion.com/reference/retrieve-a-page-property). |

### Captured

- **Pages** (standalone, i.e. not database rows), including pages whose parent is `database_id`
  - fields: title, icon, parent, `is_locked`, `public_url`
  - content: blocks
- **Databases** (i.e. data source containers): full-page or inline; original, or linked (showing a data source owned by another database)
  - fields: title, description, icon, parent, `is_inline`, `is_locked`, `show_data_source_title`
    - linked databases: ID, title and location only (the API cannot retrieve them)
  - **Views**: each scoped to one data source (dashboards: several, via widgets); all types (table, board, calendar, timeline, gallery, list, form, chart, map, dashboard and its widgets), with filters, sorts, quick filters and layout configuration
- **Data sources** (i.e. tables): parented by one database, which may hold several; shown by any number of linked databases; including synced data sources (labelled by `sync_metadata`)
  - schema: every property type, with its full configuration as returned:
    - descriptions; select/multi-select/status options (colors, descriptions); number formats; unique-ID prefixes
    - relations as configured (target, `limit`, one-/two-way), including targets the integration cannot access
    - formula and rollup definitions; expressions stored as returned, including internal `{{notion:…}}` references
  - **Rows** (pages): every row the data source queries return, including data sources over 10k rows
    - fields: icon, `is_locked`, `public_url`
    - property values: every type not listed in the "Not captured" section, each losslessly in its own column, including status, unique ID, place, wiki verification, date ranges and time zones, and relations with more than 25 links
    - body: blocks (optional, on by default)
- **Blocks** (page and row content, any nesting depth)
  - all JSON-only types: paragraph, headings 1–4 (including toggle headings), bulleted/numbered list, to_do, toggle, quote, callout, divider, breadcrumb, table_of_contents, code (including mermaid), bookmark, link_preview, equation, table/table_row, column_list/column, tab, child_page, child_database, link_to_page
  - block colors
  - synced blocks: the original is stored as a normal block with its children; copies are stored as a pointer (`synced_from.block_id`), with no duplicated content
- **Rich text** (titles, text properties, blocks): formatting, colors, links, mentions (including link mentions), inline equations

## Discovery
Search is only the starting point. From there, discovery follows documented paths, and repeats until nothing new is found:
- **up**: the parent of every captured page, block, database and data source, up to the workspace;
- **down**: page bodies, `child_page` blocks, and the views of `child_database` blocks (listed by `database_id`);
- **sideways**: data sources named by relation settings, views and dashboard widgets; pages and databases named by `link_to_page` blocks and mentions; child data sources (wikis) returned by data source queries.

Every page found gets its body walked; every data source found gets its table, rows and views. A database reached by ID is original only if it contains a discovered data source; otherwise it is recorded as linked.

## Coverage limits
Even with full workspace access and every requirement below met, a snapshot may miss:
- **Content that no documented path connects to captured content**: search can miss items, and nothing else leads to them. Keeping all content under a single top-level page makes the workspace one connected tree.
- **Pages whose parent is `database_id`**: search is their only path.
- **Items reachable only through row bodies**, when row bodies are skipped.
- **A dashboard without widgets, inside a linked database the walk never reaches.**
- **Edits made while the snapshot runs.** Search also lags behind recent changes.

## Installation
### Pip
```bash
pip install notion2pg-bulk
```
### Uv
```bash
uv add notion2pg-bulk
```

## Setup and Usage

### Requirements
- **PostgreSQL database connection URL**
- **Notion API token** with read access to all content. Notion doesn't allow Connections' content access to apply the whole workspace directly. This is achieved by ticking **all _Top-level pages_** in the Content access section in the desktop or web UI (Developer tools -> Connections -> MyConnection -> Content access -> Edit access). **Important**: each new top-level page that is subsequently added to the workspace **must be also manually granted access to Connection**, as new pages default to `unticked`.
    > A workaround to ensure that the Connection will remain full-workspace in the future without needing to manually grant it access to all new top-level pages, is to place all content in a single static top-level page (ex: 'Workspace') which is granted once. This way all pages in the workspace will inherit its accessibility by the Connection regardless of structure changes.

### Connection capabilities
- **Read content** only. No user capabilities and no comment capabilities are needed.
- Shared with the whole workspace (see above).

### Workspace requirements
Row tables and columns are named verbatim, so the workspace must satisfy:
1. Data source titles are unique (exact match) and non-empty.
2. Every data source title and every name of a property that maps to a column is at most 63 bytes of UTF-8 (PostgreSQL's identifier limit).
3. No property that maps to a column is named `tableoid`, `xmin`, `xmax`, `cmin`, `cmax`, `ctid` or `_page_id`.

A violation aborts the snapshot with `SnapshotIntegrityError` before anything is written.

### Target database
The target must be empty: no schema besides `public` (and the system schemas), and nothing in `public` (tables, views, materialized views, sequences, foreign tables, functions, types). Default extensions (`plpgsql`) are ignored. Otherwise `TargetNotEmptyError` is raised before any Notion request. The package never drops or overwrites anything: to replace an old snapshot, create a fresh database, snapshot into it, then switch.

### CLI

Options:
- `--notion-token`: Notion integration token (or set `NOTION_TOKEN` env var)
- `--database-url`: PostgreSQL connection string of an empty database (or set `DATABASE_URL` env var)
- `--plan`: Notion workspace plan, which sets the request budget: `free` (default) or `plus` → 180 req/min; `business` or `enterprise` → 600 req/min
- `--no-row-bodies`: Skip the bodies (blocks) of data source rows (much faster: at least one request per row is saved)
- `--propagation-id`: Identifier carried by every telemetry event (generated if omitted)

Examples:

```bash
# Full snapshot (row bodies included)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://user:password@localhost/dbname"

# Business or Enterprise workspace
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --plan business

# Without row bodies (faster)
notion2pg-bulk --notion-token "your_token" --database-url "postgresql://..." --no-row-bodies
```

Telemetry is written to stderr, one JSON object per line. On failure the error is raised and the exit code is non-zero.

### Python API

```python
from notion2pg_bulk import snapshot

snapshot(
    notion_token="your_notion_integration_token",
    database_url="postgresql://user:password@localhost/dbname",
    plan="free",
    row_bodies=True,
)
```

A bare `postgresql://` URL uses psycopg 3. The package creates its own engine, with a 10 s connect timeout.

## PostgreSQL mapping

- **One table per data source**, holding its rows: primary key = row page ID, then exactly one column per property. Every column is lossless: with the shared tables it references (e.g. options), it reproduces everything the API returns for that value. Exceptions: the property types listed in the "Not captured" section have no column.
- **Shared tables, keyed by Notion IDs**:
  - catalog: data source → table name, property → column name. Names are verbatim: the table is named exactly after the data source title and each column exactly after its property name, as quoted identifiers (no sanitizing, lowercasing or truncating). The row ID column is `_page_id`.
  - databases (original and linked), data sources, properties (full configuration, including formula and rollup definitions), select/multi-select/status options, views
  - pages: every captured page, rows included (parent, title for standalone pages, icon, `is_locked`, `public_url`)
  - blocks: every block of every captured page body (`page_id`, parent block, position, type, content)
- References between entities (relations, parents, options, views → data sources) are stored as Notion IDs.
- **Two schemas**: `notion` holds the shared tables, `rows` holds the row tables, so row-table names never collide with them.
- **What Notion guarantees**: property names are unique within a data source (they are the keys of the API's `properties` object); IDs are unique and stable. Titles of data sources and databases are not guaranteed unique and can be empty (see [Workspace requirements](#workspace-requirements)).
- **The table name is the data source title** (`data_source.title`), not the database title. A data source can keep the name it got when it was created, so it may differ from the database's current title. Check it in Notion under **Manage data sources**.

### Tables

| Table | Key | Contents |
|---|---|---|
| `notion.catalog_tables` | `data_source_id` | `table_name` in `rows` |
| `notion.catalog_columns` | `data_source_id`, `property_id` | `column_name` in that table |
| `notion.databases` | `database_id` | `is_linked`, `title` (rich text), `block_title` (title of the `child_database` block), `description`, `icon`, `parent_type`, `parent_id`, `is_inline`, `is_locked`, `show_data_source_title` |
| `notion.data_sources` | `data_source_id` | `database_id`, `parent_type`, `parent_id`, `title`, `description`, `icon`, `sync_metadata` |
| `notion.properties` | `data_source_id`, `property_id` | `name`, `type`, `description`, `config` (the property object as returned) |
| `notion.options` | `data_source_id`, `property_id`, `option_id` | `position`, `name`, `color`, `description`, `status_group_id`, `status_group_name` |
| `notion.views` | `view_id` | `database_id`, `data_source_id` (null for dashboards), `dashboard_view_id` (widgets), `name`, `type`, `content` (filter, sorts, quick filters, configuration, …) |
| `notion.pages` | `page_id` | `parent_type`, `parent_id`, `title` (standalone pages), `icon`, `is_locked`, `public_url` |
| `notion.blocks` | `block_id` | `page_id`, `parent_block_id` (null at top level), `position` (index among the API's children), `type`, `content` (the type's payload) |
| `rows."<data source title>"` | `_page_id` | one column per property |

### Property type mapping

| Notion Property | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| Title | `JSONB` | Rich-text array as returned (formatting, colors, links, mentions, equations); complete beyond 25 mentions |
| Rich text | `JSONB` | As title |
| Number | `NUMERIC` | Number format in `notion.properties.config` |
| Select | `TEXT` | Option ID; name, color and description in `notion.options` |
| Status | `TEXT` | Option ID; group in `notion.options`, default option in `config` |
| Multi-select | `TEXT[]` | Option IDs |
| Date | `JSONB` | `{start, end, time_zone}` as returned: date-only values stay `YYYY-MM-DD`, datetimes keep their offset. Query with expressions, e.g. `("Due"->>'start')` |
| Checkbox | `BOOLEAN` | |
| URL | `TEXT` | |
| Email | `TEXT` | |
| Phone | `TEXT` | |
| Relation | `UUID[]` | Related page IDs, complete beyond 25; target and `limit` in `config` |
| People | `UUID[]` | User IDs, complete beyond 25 |
| Unique ID | `BIGINT` | Number; the prefix is in `config` |
| Place | `JSONB` | `{lat, lon, name, address, aws_place_id, google_place_id}` |
| Verification | `JSONB` | `{state, date, verified_by}` |
| Created time | `TIMESTAMPTZ` | |
| Created by | `UUID` | User ID |
| Last edited time | `TIMESTAMPTZ` | |
| Last edited by | `UUID` | User ID |
| Formula | - | Definition in `config`; values not captured |
| Rollup | - | Definition in `config`; values not captured |
| Button | - | Not returned by the API |
| Files | - | Not captured |
| Any other type | `JSONB` | Value as returned |

Empty values are `NULL` (the API returns `null` or `[]` for them); an unchecked checkbox is `false`.

## Consistency
Notion has no snapshot API, so **a snapshot is not point-in-time consistent**: entities are read one after another over minutes (an hour or more with row bodies), and edits made meanwhile may be partly reflected. If a row shows a property, type or option that the schema read earlier lacks, the data source is read again: new properties get a column (under the name first seen), while a property that changed type, or an option still unknown after the re-read, aborts the snapshot.

The snapshot itself is atomic: DDL and data are written in one PostgreSQL transaction, so a failed run leaves the target empty.

## Failures
Any fault that is not retried, or that persists after its retries, raises an exception from one hierarchy, rolls the transaction back and leaves the target empty. Nothing is skipped and continued, with one exception: a lookup made only to extend [discovery](#discovery) that returns 404 simply ends that path.

| Exception | Raised when |
|---|---|
| `Notion2PgError` | Base class |
| `TargetNotEmptyError` | The target database is not empty |
| `NotionAPIError` | Notion returned an error that is not retried (`status`, `code`, `request_id`) |
| `RetriesExhaustedError` | A retried fault persisted, or Notion asked to wait more than 300 s (`last_fault`) |
| `SnapshotIntegrityError` | A completeness guarantee can't be met (a pagination cursor repeats or is missing, a 10k-row window can't advance, a workspace requirement is violated, a property changes type, nesting deeper than 100 blocks) |

## Request limits and retries
- One limiter blocks before each request: at most 180 (`free`, `plus`) or 600 (`business`, `enterprise`) requests in any 60 s, and at least 1/3 s between data source queries on every plan.
- Timeouts on every request: connect 10 s, read 60 s, write 10 s, pool 10 s. PostgreSQL: 60 s per statement.
- Only these faults are retried, per request, up to 5 times: `rate_limited` (429, except `public_api_request_blocked`), `service_overload` (529), server errors (500, 502, 503, 504) and connection/timeout/protocol errors. The wait is Notion's `Retry-After` (or `additional_data.retry_after`), otherwise 1, 2, 4, 8, 16 s plus up to 25% jitter.
- Indicative duration on Free for a workspace of ~70 data sources and ~8,500 rows: about 5 min without row bodies, 50+ min with them.

## Telemetry
Every limiter wait and retry transition emits one JSON event on the `notion2pg_bulk` logger (the package itself only adds a `NullHandler`): `throttle_wait`, `retry_scheduled`, `retries_exhausted`, `request_failed` and `discovery_stopped` (a 404 on a lookup made only to extend discovery, which ends that path). Each carries `level` (`info` for `throttle_wait` and `discovery_stopped`, `warning` for `retry_scheduled`, `error` for the other two; the record is logged at that level), `ts`, `propagation_id`, the locus (`phase`, `entity_type`, `entity_id`, `method`, `endpoint`), the fault's scalars (`http_status`, `code`, `rate_limit_reason`, `request_id`, `exception_type`) and the Notion error `message`. Headers, the token and request or response bodies are never included; strings are cut at 512 characters. The JSON Schema ships with the package as `notion2pg_bulk/telemetry.schema.json`; the event record is also attached to each log record as `record.telemetry`.

## Notion version
**Last tested Notion-Version**: `2026-03-11`
