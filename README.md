# notion2pg_bulk

_Python package to snapshot an entire Notion workspace to a PostgreSQL database._

## Snapshot scope

### Not captured

| Item | Reason |
|---|---|
| Media: `image`, `video`, `audio`, `file`, `pdf` blocks; `files` properties; page and database covers; file icons | Notion-hosted files: not retrievable; the API returns only URLs that expire after 1 h ([file object](https://developers.notion.com/reference/file-object)). External links: choice; keeping them while dropping hosted files means handling the two variants separately for each media type. |
| Database templates | Choice. Friction: +1 paginated request per data source ([list templates](https://developers.notion.com/reference/list-data-source-templates)) plus a body fetch per template; |
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
  - **Rows** (pages): all of them, including data sources over 10k rows
    - fields: icon, `is_locked`, `public_url`
    - property values: every type not listed in the "Not captured" section, each losslessly in its own column, including status, unique ID, place, wiki verification, date ranges and time zones, and relations with more than 25 links
    - body: blocks (optional, on by default)
- **Blocks** (page and row content, any nesting depth)
  - all JSON-only types: paragraph, headings 1–4 (including toggle headings), bulleted/numbered list, to_do, toggle, quote, callout, divider, breadcrumb, table_of_contents, code (including mermaid), bookmark, embed, link_preview, equation, table/table_row, column_list/column, tab, child_page, child_database, link_to_page
  - block colors
  - synced blocks: the original is stored as a normal block with its children; copies are stored as a pointer (`synced_from.block_id`), with no duplicated content
- **Rich text** (titles, text properties, blocks): formatting, colors, links, mentions (including link mentions), inline equations

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

## PostgreSQL mapping

- **One table per data source**, holding its rows: primary key = row page ID, then exactly one column per property. Every column is lossless: with the shared tables it references (e.g. options), it reproduces everything the API returns for that value. Exceptions: the property types listed in the "Not captured" section have no column.
- **Shared tables, keyed by Notion IDs**:
  - catalog: data source → table name, property → column name. Names are derived once and kept across Notion renames; they never encode identity.
  - databases (original and linked), data sources, properties (full configuration, including formula and rollup definitions), select/multi-select/status options, views
  - pages: every page, rows included (parent, title for standalone pages, icon, `is_locked`, `public_url`)
  - blocks: every block of every page body (`page_id`, parent block, position, type, content)
- References between entities (relations, parents, options, views → data sources) are stored as Notion IDs.

## Notion version
**Last tested Notion-Version**: `2026-03-11` (note: `notion-client` 3.x defaults to `2025-09-03`).
