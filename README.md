# notion2pg_bulk

_Python package to snapshot a Notion workspace to a PostgreSQL database, within its [coverage limits](#coverage-limits)._

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
- **Notion API token** with read content access to all content (no user capabilities and no comment capabilities). Notion doesn't allow Connections' content access to apply the whole workspace directly. This is achieved by ticking **all _Top-level pages_** in the Content access section in the desktop or web UI (Developer tools -> Connections -> MyConnection -> Content access -> Edit access). **Important**: each new top-level page that is subsequently added to the workspace **must be also manually granted access to Connection**, as new pages default to `unticked`.
- **Optional but recommended: all content under a single top-level page** (e.g. `Workspace`), shared with the connection once. This:
  1. makes the workspace one connected tree, so [discovery](#discovery) reaches all content even where search misses it (see [Coverage limits](#coverage-limits));
  2. keeps the connection's access full-workspace: pages created anywhere under it inherit its access, so new top-level pages never need to be shared by hand.

### Workspace requirements
Row tables and columns are named verbatim, so the workspace must satisfy:
1. Data source titles are unique (exact match) and non-empty.
2. Every data source title and every name of a property that maps to a column is at most 63 bytes of UTF-8 (PostgreSQL's identifier limit).
3. No property that maps to a column is named `tableoid`, `xmin`, `xmax`, `cmin`, `cmax`, `ctid` or `_page_id`.

Notion guarantees only that property names are unique within a data source (they are the keys of the API's `properties` object) and that IDs are unique and stable; titles of data sources can repeat and can be empty.

A violation aborts the snapshot with `SnapshotIntegrityError` before anything is written.

### Target database
The target must be a newly created database. Otherwise `TargetNotEmptyError` is raised before any Notion request. The package never drops or overwrites anything: to replace an old snapshot, create a fresh database, snapshot into it, then switch.

### CLI

Options:
- `--notion-token`: Notion integration token (or set `NOTION_TOKEN` env var)
- `--database-url`: PostgreSQL connection string of a newly created database (or set `DATABASE_URL` env var)
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

## Snapshot scope

### Not captured

| Item | Reason |
|---|---|
| Media: `image`, `video`, `audio`, `file`, `pdf` blocks; `files` properties; page and database covers; file icons | Notion-hosted files: not retrievable; the API returns only URLs that expire after 1 h ([file object](https://developers.notion.com/reference/file-object)). External links: choice; keeping them while dropping hosted files means handling the two variants separately for each media type. |
| `embed` blocks | An embed is read back with only `url`, a temporary signed link when it is backed by an uploaded file; `file_upload` is a create/update parameter only ([embed](https://developers.notion.com/reference/block#embed)). Notion-hosted and external embeds can't be told apart, so neither is kept. |
| Favourites | Not exposed by the [API](https://developers.notion.com/reference/intro): no endpoint returns a user's favourites. |
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
    - property values: every type not listed in the "Not captured" section, each losslessly in its own column, including status, unique ID, place, wiki verification, date ranges, and relations with more than 25 links
    - body: blocks (optional, on by default)
- **Blocks** (page and row content, any nesting depth)
  - all JSON-only types: paragraph, headings 1–4 (including toggle headings), bulleted/numbered list, to_do, toggle, quote, callout, divider, breadcrumb, table_of_contents, code (including mermaid), bookmark, link_preview, equation, table/table_row, column_list/column, tab, child_page, child_database, link_to_page
  - block colors
  - synced blocks: the original is stored as a normal block with its children; copies are stored as a pointer (`synced_from.block_id`), with no duplicated content
- **Rich text** (titles, text properties, blocks): formatting, colors, links, mentions (including link mentions), inline equations

## Coverage limits
Search is the only endpoint that lists pages and data sources without knowing their IDs, and the Notion docs say it can't serve as an inventory: "Search can't provide a complete inventory of the content a connection can access", and its index "takes time to reflect changes" ([search limitations](https://developers.notion.com/reference/search-optimizations-and-limitations)). [Discovery](#discovery) follows every documented path from what search finds, but an item that search misses can still be missed when:
- **no documented path connects it to captured content**, including an original database without views that is reached only by ID (e.g. through a mention): `GET /databases/{id}` isn't used on databases reached by ID, so it is recorded as linked;
- **it is a page whose parent is `database_id`**: search is its only path. Such pages are most likely leftovers of Notion's move to data sources, and should be rare: the current parent rules don't list databases among page parents ([parent object](https://developers.notion.com/reference/parent-object)), while before `2025-09-03` rows were parented by `database_id`, as the [page object](https://developers.notion.com/reference/page) example still shows;
- **only row bodies lead to it, and the snapshot runs with `--no-row-bodies`** (`row_bodies=False`): discovery doesn't read row bodies then, so it can't reach anything inside them;
- **it is a dashboard without widgets**, in a linked database that the walk never reaches.

Keeping all content under a [single top-level page](#requirements) practically rules these out: discovery climbs from anything it finds to that page and walks everything under it. Only pages parented by `database_id` and, with `--no-row-bodies`, content inside rows stay out of its reach.

Independently of search, edits made while the snapshot runs may be partly reflected (see [Consistency](#consistency)).

## PostgreSQL mapping

The snapshot has two schemas: `notion`, with the shared tables below, and `rows`, with one table per data source.

**Identifiers.** Page, block, database, data source, view and user IDs are `UUID`. Property IDs and option IDs are `TEXT`, exactly as the API returns them: property IDs are short strings or UUIDs, URL-encoded (e.g. `M%3BBw`), the title property's ID is always `title`, and option IDs are UUIDs or short strings. Every reference between entities is one of these IDs; the only foreign key is `notion.options` → `notion.properties`.

**Empty values** are `NULL` (the API returns `null` or `[]` for them); an unchecked checkbox is `false`.

**Rich text** (`JSONB`) is the API's array of [rich text objects](https://developers.notion.com/reference/rich-text), in order. Each has `type` (`text`, `mention` or `equation`), the matching object (`text.content`, `text.link`; `mention.type` with the target in `mention.<type>`, e.g. `mention.page.id`; `equation.expression`), `annotations` (`bold`, `italic`, `strikethrough`, `underline`, `code`, `color`), `plain_text` and `href`. The plain text of a value is the concatenation of its items' `plain_text`.

**Parents** are a pair: `parent_type` and `parent_id`. `parent_type` is `workspace` (then `parent_id` is `NULL`), `page_id`, `block_id`, `database_id`, `data_source_id` or `agent_id`, and `parent_id` is the ID of that entity.

**Icons** (`JSONB`) are the icon object as returned: `{"type": "emoji", "emoji": …}`, `{"type": "icon", "icon": {"name", "color"}}`, `{"type": "custom_emoji", "custom_emoji": {"id", "name", "url"}}` or `{"type": "external", "external": {"url"}}`; `NULL` when there is none or it is a Notion-hosted file.

### Row tables
Each data source has the table `rows."<title>"`, where `<title>` is the plain text of the data source's title, used verbatim as a quoted identifier (no sanitizing, lowercasing or truncating). This is the data source title, not the database title; the two can differ (check it in Notion under **Manage data sources**). `notion.catalog_tables` maps each data source to its table name.

The table's primary key is `_page_id`, the row page's ID (the row's page is in `notion.pages`). Every other column holds one property, named after it as first seen by the snapshot; `notion.catalog_columns` maps each property ID to its column name. Its type and content follow the property type:

| Notion property | PostgreSQL type | Content |
|---|---|---|
| Title | `JSONB` | Rich text, complete beyond 25 mentions |
| Rich text | `JSONB` | Rich text, complete beyond 25 mentions |
| Number | `NUMERIC` | The exact number; the display format is in the property's `config` |
| Select | `TEXT` | Option ID, in `notion.options` |
| Status | `TEXT` | Option ID, in `notion.options` |
| Multi-select | `TEXT[]` | Option IDs, in `notion.options` |
| Date | `JSONB` | `{"start", "end", "time_zone"}` as returned. `start` and `end` (`NULL` unless a range) are `YYYY-MM-DD` for a date, or a datetime with UTC offset (e.g. `2026-09-15T09:30:00.000+02:00`). A named time zone may come back only as that offset, with `time_zone` `NULL` |
| Checkbox | `BOOLEAN` | |
| URL, Email, Phone | `TEXT` | |
| Relation | `UUID[]` | Page IDs of the related rows, complete beyond 25: the `_page_id` values of the target data source (named in the property's `config`) |
| People | `UUID[]` | User IDs, complete beyond 25 |
| Unique ID | `BIGINT` | The number; the prefix is in the property's `config` |
| Place | `JSONB` | `{"lat", "lon", "name", "address", "aws_place_id", "google_place_id"}` |
| Verification | `JSONB` | `{"state", "date", "verified_by"}`: `state` is `verified`, `unverified` or `expired`; `date` is a date value; `verified_by` a user `{"object", "id"}` |
| Created time, Last edited time | `TIMESTAMPTZ` | |
| Created by, Last edited by | `UUID` | User ID |
| Any other type | `JSONB` | The value as returned |

Formula, rollup, button and files properties have no column (see [Not captured](#not-captured)); formula and rollup definitions are in `notion.properties`.

### Shared tables

**`notion.catalog_tables`**: one row per data source.

| Column | Type | Content |
|---|---|---|
| `data_source_id` | `UUID`, key | |
| `table_name` | `TEXT`, unique | Its table in `rows` |

**`notion.catalog_columns`**: one row per property that has a column.

| Column | Type | Content |
|---|---|---|
| `data_source_id`, `property_id` | `UUID`, `TEXT`, key | |
| `column_name` | `TEXT` | Its column in the data source's table |

**`notion.databases`**: one row per database, original (owns data sources) or linked (shows a data source owned by another database).

| Column | Type | Content |
|---|---|---|
| `database_id` | `UUID`, key | Also the ID of the `child_database` block showing it in its parent page |
| `is_linked` | `BOOLEAN` | |
| `title`, `description` | `JSONB` | Rich text; `NULL` when empty, and for linked databases |
| `block_title` | `TEXT` | Title shown on its `child_database` block; set for linked databases found in a page body |
| `icon` | `JSONB` | `NULL` for linked databases |
| `parent_type`, `parent_id` | `TEXT`, `UUID` | Where it sits: `workspace`, `page_id` or `block_id`. For a linked database, the page or block holding its `child_database` block; `NULL` if it was only seen as the parent of a view |
| `is_inline` | `BOOLEAN` | Inline (`true`) or full page (`false`); `NULL` for linked databases |
| `is_locked`, `show_data_source_title` | `BOOLEAN` | `NULL` for linked databases |

**`notion.data_sources`**: one row per data source.

| Column | Type | Content |
|---|---|---|
| `data_source_id` | `UUID`, key | |
| `database_id` | `UUID` | The original database that owns it |
| `parent_type`, `parent_id` | `TEXT`, `UUID` | `database_id` (the owning database) or, for a synced data source, `data_source_id` |
| `title` | `JSONB` | Rich text (its plain text is the table name) |
| `description` | `JSONB` | Rich text; `NULL` when empty |
| `icon` | `JSONB` | |
| `sync_metadata` | `JSONB` | As returned for data sources synced from external tools (provider, sync ID, health); `NULL` otherwise |

**`notion.properties`**: one row per property of every data source (including formula, rollup, button and files properties).

| Column | Type | Content |
|---|---|---|
| `data_source_id`, `property_id` | `UUID`, `TEXT`, key | |
| `name` | `TEXT` | Current name |
| `type` | `TEXT` | API type: `title`, `rich_text`, `number`, `select`, `status`, `multi_select`, `date`, `checkbox`, `url`, `email`, `phone_number`, `relation`, `people`, `unique_id`, `place`, `verification`, `created_time`, `created_by`, `last_edited_time`, `last_edited_by`, `formula`, `rollup`, `button`, `files`, or any newer type |
| `description` | `TEXT` | |
| `config` | `JSONB` | The [property object](https://developers.notion.com/reference/property-object) as returned: `id`, `name`, `description`, `type`, and the type's settings under the key named by `type` |

Settings in `config`, by type:
- `relation`: `relation.data_source_id` and `relation.database_id` (the target), `relation.limit`, `relation.type` (`single_property` or `dual_property`), and for two-way relations `relation.dual_property.synced_property_id` and `.synced_property_name` (the property on the other side);
- `formula`: `formula.expression`, in `prop("Name")` syntax (expressions that can't be shown that way contain `{{notion:block_property:…}}` references);
- `rollup`: `rollup.relation_property_id`, `.relation_property_name`, `.rollup_property_id`, `.rollup_property_name`, `.function`;
- `number`: `number.format`;
- `unique_id`: `unique_id.prefix`;
- `status`: `status.default_option` (option name) and `status.groups` (`id`, `name`, `color`, `option_ids`);
- `select`, `multi_select`, `status`: `<type>.options` (as also listed in `notion.options`).

**`notion.options`**: one row per option of every select, multi-select and status property.

| Column | Type | Content |
|---|---|---|
| `data_source_id`, `property_id`, `option_id` | `UUID`, `TEXT`, `TEXT`, key | |
| `position` | `INTEGER` | Order in the property's schema, from 0 |
| `name`, `color`, `description` | `TEXT` | |
| `status_group_id`, `status_group_name` | `TEXT` | The status group holding the option; `NULL` for select and multi-select |

**`notion.views`**: one row per view: the tabs of every database, and dashboard widgets.

| Column | Type | Content |
|---|---|---|
| `view_id` | `UUID`, key | |
| `database_id` | `UUID` | The database (original or linked) whose tab it is |
| `data_source_id` | `UUID` | The data source it shows; `NULL` for dashboards |
| `dashboard_view_id` | `UUID` | For a dashboard widget, its dashboard; `NULL` otherwise |
| `name`, `type` | `TEXT` | `type`: `table`, `board`, `calendar`, `timeline`, `gallery`, `list`, `form`, `chart`, `map` or `dashboard` |
| `content` | `JSONB` | The [view object](https://developers.notion.com/reference/view) as returned, without `object`, `id`, `parent`, `url`, `created_*` and `last_edited_*`: `name`, `type`, `data_source_id`, `dashboard_view_id`, `filter`, `sorts`, `quick_filters`, `configuration`, and any other key the API returns |

In `content`:
- `filter` and `sorts` are a [filter](https://developers.notion.com/reference/filter-data-source-entries) and a list of [sorts](https://developers.notion.com/reference/sort-data-source-entries) (`NULL` when unset); a property is named by `property` (its name or ID), and select and status values by option name;
- `configuration` depends on `type` ([view configuration](https://developers.notion.com/guides/data-apis/working-with-views#view-configuration)); in view types that have it (e.g. `table`), `configuration.properties` lists the view's properties as `{"property_id", "visible", "width"}`, `visible` saying whether the column is shown. For a dashboard, `configuration.rows` lists its rows, each with `widgets`, each with `view_id` (the widget view) and `row_index`.

The order of a database's tabs is not available.

**`notion.pages`**: one row per page, rows included.

| Column | Type | Content |
|---|---|---|
| `page_id` | `UUID`, key | Also the ID of the `child_page` block showing it in its parent page |
| `parent_type`, `parent_id` | `TEXT`, `UUID` | A row's parent is its data source (`data_source_id`) |
| `title` | `JSONB` | Rich text; `NULL` for rows (their title is in their table) |
| `icon` | `JSONB` | |
| `is_locked` | `BOOLEAN` | |
| `public_url` | `TEXT` | `NULL` unless published to the web |

**`notion.blocks`**: one row per block of every page body.

| Column | Type | Content |
|---|---|---|
| `block_id` | `UUID`, key | |
| `page_id` | `UUID` | The page whose body it is in (a row's page for row bodies), at any depth |
| `parent_block_id` | `UUID` | The block it is nested in; `NULL` at the top level of the body |
| `position` | `INTEGER` | Order among its siblings, from 0 (gaps where a block is [not captured](#not-captured)) |
| `type` | `TEXT` | Block type, e.g. `paragraph`, `heading_1`, `toggle`, `child_page`, `child_database`, `link_to_page`, `synced_block`, `table`, `table_row`, `tab`, `column_list`, `column`, `unsupported` |
| `content` | `JSONB` | The block's [type object](https://developers.notion.com/reference/block#block-type-objects) as returned (e.g. `rich_text`, `color`, `checked`), without children |

`content` by type, where it links to other entities:
- `child_page` and `child_database`: `{"title"}`; the block ID is the page's or database's ID, and its body is that page's own, not this one's;
- `link_to_page`: `{"type": "page_id", "page_id"}` or `{"type": "database_id", "database_id"}`;
- `synced_block`: an original is `{"synced_from": null}`, with its blocks nested under it; a copy is `{"synced_from": {"type": "block_id", "block_id"}}` and has no blocks of its own;
- `tab`: `{}`; its children are `paragraph` blocks (the tab label in `rich_text`, its `icon`) holding the tab's blocks;
- `table`: `{"table_width", "has_column_header", "has_row_header"}`; its children are `table_row` blocks with `cells`, one rich text per column.

### Page tree
A page or database sits where its `parent_type` and `parent_id` point. A parent of type `block_id` is a block in `notion.blocks` (for example a toggle or a column): its `page_id` is the page containing it. Among the siblings in a page body, a sub-page or database is ordered by the `position` of the `child_page` or `child_database` block with its ID; pages and databases at the top level of the workspace have no recorded order. A row's database is the `database_id` of its data source; a linked database's data source is the `data_source_id` of its views.

## Consistency
- DDL and data are written in one PostgreSQL transaction, so a failed run leaves the target as it was.
- Notion has no snapshot API, so **a snapshot is not point-in-time consistent**: entities are read one after another over minutes (an hour or more with row bodies), and edits made meanwhile may be partly reflected. If a row shows a property, type or option that the schema read earlier lacks, the data source is read again: new properties get a column (under the name first seen), while a property that changed type, or an option still unknown after the re-read, aborts the snapshot with `SnapshotIntegrityError`.

## Failures
Any fault that is not retried, or that persists after its retries, raises an exception from one hierarchy, rolls the transaction back and leaves the target as it was. Nothing is skipped and continued, except where a [discovery](#discovery) path ends: an object not found on a lookup made only to extend discovery, or returned trashed or archived.

| Exception | Raised when |
|---|---|
| `Notion2PgError` | Base class |
| `TargetNotEmptyError` | The target database is not newly created |
| `NotionAPIError` | Notion returned an error that is not retried (`status`, `code`, `request_id`) |
| `RetriesExhaustedError` | A retried fault persisted, or Notion asked to wait more than 300 s (`last_fault`) |
| `SnapshotIntegrityError` | A completeness guarantee can't be met (a pagination cursor repeats or is missing, a 10k-row window can't advance, a workspace requirement is violated, a property changes type, nesting deeper than 100 blocks) |

## Request limits and retries
- One limiter blocks before each request: at most 180 (`free`, `plus`) or 600 (`business`, `enterprise`) requests in any 60 s, and at least 1/3 s between data source queries on every plan.
- Timeouts on every request: connect 10 s, read 60 s, write 10 s, pool 10 s. PostgreSQL: 60 s per statement.
- Only these faults are retried, per request, up to 5 times: `rate_limited` (429, except `public_api_request_blocked`), `service_overload` (529), server errors (500, 502, 503, 504) and connection/timeout/protocol errors. The wait is Notion's `Retry-After` (or `additional_data.retry_after`), otherwise 1, 2, 4, 8, 16 s plus up to 25% jitter.
- Indicative duration on Free for a workspace of ~70 data sources and ~8,500 rows: about 5 min without row bodies, 50+ min with them.

## Telemetry
One JSON event per limiter wait, retry transition or ended discovery path, on the `notion2pg_bulk` logger (the package only adds a `NullHandler`; the CLI writes `info` and above to stderr).
- **Events**
  - `throttle_wait` (`debug`): the limiter blocked a request.
  - `retry_scheduled` (`warning`): a retried fault, and the wait before the next attempt.
  - `retries_exhausted` (`error`): a retried fault that persisted.
  - `request_failed` (`error`): a fault that is not retried.
  - `discovery_stopped` (`info`): a discovery path ended (`reason`: `not_found`, `in_trash` or `archived`).
- **Fields**: `event`, `level`, `ts`, `propagation_id`; locus (`phase`, `entity_type`, `entity_id`, `method`, `endpoint`); fault (`http_status`, `code`, `rate_limit_reason`, `request_id`, `exception_type`); the Notion error `message`.
- **Never included**: headers, the token, request or response bodies. Strings are cut at 512 characters.
- **Schema**: `notion2pg_bulk/telemetry.schema.json`. Each log record also carries the event as `record.telemetry`.

## Tests
There are two suites, kept apart:

| Suite | Directory | Notion | PostgreSQL | Runs by default |
|---|---|---|---|---|
| Unit and integration | `tests/` | Faked (in-memory API behind `httpx.MockTransport`), no network | Real | Yes (`uv run pytest`) |
| Live integration | `tests_live/` | A real, disposable Notion workspace | Real | No (`uv run pytest tests_live`) |

### Unit and integration tests
Requires a PostgreSQL server: set `NOTION2PG_TEST_ADMIN_URL` to a superuser URL (e.g. `postgresql://postgres:postgres@localhost/postgres`); each test creates and drops its own database. Without it, a throwaway cluster is started with the local `initdb` (not possible as root).

```bash
NOTION2PG_TEST_ADMIN_URL="postgresql://postgres:postgres@localhost/postgres" uv run pytest
```

### Live integration tests
They check, against a real Notion workspace, the assumptions the snapshot depends on that the Notion docs don't state exactly. Each run builds its own fixtures under a root page, takes three snapshots, checks them, and moves the root page with everything under it to the trash (about 4 minutes on Free).

1. Use a dedicated, disposable workspace: the snapshot captures everything the connection can access.
2. Create an internal connection there (Developer tools → Connections → New connection) with **Read**, **Update** and **Insert content**.
3. Create a page in the sidebar (**+ New page**), then **•••** → **Connections** → select the connection → **Confirm**.
4. Optional (their tests are skipped otherwise), on that page:
   - type `/full page database`, pick **Database – Full page**, title it exactly `UI Full Page`, return to the page;
   - type `/linked`, pick **Linked view of data source**, choose `UI Full Page`.
5. Run, with the page ID (the last 32 hexadecimal characters of the page link) and a PostgreSQL superuser URL:

```bash
NOTION_TOKEN="..." NOTION2PG_LIVE_ROOT_PAGE_ID="..." NOTION2PG_TEST_ADMIN_URL="postgresql://postgres:postgres@localhost/postgres" uv run pytest tests_live -v
```

Repeat steps 3–4 before each run. `NOTION2PG_LIVE_PLAN` (default `free`) sets the request budget. The API can only trash content: empty Notion's Trash to delete it.

## Notion version
**Last tested Notion-Version**: `2026-03-11`

## Internal logic

### Discovery
Search is only the starting point. From there, discovery follows documented paths, and repeats until nothing new is found:
- **up**: the parent of every captured page, block, database and data source, up to the workspace;
- **down**: page bodies, `child_page` blocks, and the views of `child_database` blocks (listed by `database_id`);
- **sideways**: data sources named by relation settings, views and dashboard widgets; pages and databases named by `link_to_page` blocks and mentions; the original block of each synced-block copy (`synced_from.block_id`), followed up to its page; child data sources (wikis) returned by data source queries.

Every page found gets its body walked; every data source found gets its table, rows and views.
