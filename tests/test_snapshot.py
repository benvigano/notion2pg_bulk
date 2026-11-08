"""End-to-end snapshot on a real PostgreSQL database with a fake Notion."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from fake_notion import error_body
from helpers import dump_database, run_snapshot
from notion2pg_bulk.errors import NotionAPIError, TargetNotEmptyError
from notion2pg_bulk.orchestrate import create_target_engine
from notion2pg_bulk.target import assert_target_empty
from workspace import OPTION_HIGH, OPTION_TAG, STATUS_DONE, USER_ID, build_workspace


def _plain(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


class Row(tuple):
    """Result row with UUIDs as strings; ``_mapping`` keyed by column name."""

    def __new__(cls, row):
        instance = super().__new__(cls, (_plain(value) for value in row))
        instance._mapping = {key: _plain(value) for key, value in row._mapping.items()}
        return instance


def query(database_url, sql, **params):
    engine = create_target_engine(database_url)
    try:
        with engine.connect() as conn:
            return [Row(row) for row in conn.execute(text(sql), params)]
    finally:
        engine.dispose()


def test_snapshot_into_empty_database(database_url, clock):
    ws = build_workspace()
    run_snapshot(ws.fake, database_url, clock)

    tables = {r[0] for r in query(database_url, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'rows'")}
    assert tables == {"Projects", "Tasks"}

    [row] = query(database_url, 'SELECT * FROM rows."Projects"')
    values = dict(row._mapping)
    assert values["_page_id"] == ws.project_row
    assert values["Name"][0]["plain_text"] == "Launch"
    assert values["Priority"] == OPTION_HIGH
    assert values["Tags"] == [OPTION_TAG]
    assert values["Status"] == STATUS_DONE
    assert float(values["Estimate"]) == 8.5
    assert values["Done"] is True
    assert values["Due date"] == {"start": "2026-09-15", "end": None, "time_zone": None}
    assert values["Owner"] == [USER_ID]
    assert sorted(values["Tasks"]) == sorted(ws.task_rows)
    assert values["Created time"].isoformat() == "2026-02-01T10:00:00+00:00"
    assert values["Created by"] == USER_ID
    assert values["ID"] == 42
    assert values["Office"]["name"] == "San Francisco"
    assert values["Summary"][0]["annotations"]["bold"] is True
    assert values["Hologram"] == {"beam": 3}
    for absent in ("Double estimate", "Total", "Send update", "Attachments"):
        assert absent not in values

    catalog = dict(query(database_url, "SELECT data_source_id::text, table_name FROM notion.catalog_tables"))
    assert catalog == {ws.projects_ds: "Projects", ws.tasks_ds: "Tasks"}
    columns = dict(query(database_url,
                         "SELECT property_id, column_name FROM notion.catalog_columns WHERE data_source_id = :ds",
                         ds=ws.projects_ds))
    assert columns["M%3BBw"] == "Due date" and "calc" not in columns

    options = query(database_url, "SELECT option_id, name, color, description, status_group_name, position "
                                  "FROM notion.options WHERE property_id IN ('prio', 'stat') ORDER BY property_id, position")
    assert [tuple(o) for o in options] == [
        (OPTION_HIGH, "High", "red", None, None, 0),
        ("a1b2c3d4-0000-4000-8000-000000000001", "Low", "gray", "Later", None, 1),
        (STATUS_DONE, "Done", "green", None, "Complete", 0),
    ]
    [formula] = query(database_url, "SELECT config FROM notion.properties WHERE property_id = 'calc'")
    assert formula[0]["formula"]["expression"] == 'prop("Estimate") * 2'

    pages = dict(query(database_url, "SELECT page_id::text, parent_type FROM notion.pages"))
    assert pages[ws.standalone_page] == "workspace"
    assert pages[ws.project_row] == "data_source_id"

    blocks = query(database_url, "SELECT page_id::text, parent_block_id::text, position, type, content "
                                 "FROM notion.blocks WHERE page_id = :p ORDER BY parent_block_id NULLS FIRST, position",
                   p=ws.standalone_page)
    assert [(b[2], b[3]) for b in blocks] == [(0, "heading_1"), (1, "paragraph"), (0, "paragraph")]
    assert blocks[0][4]["color"] == "blue_background"
    assert blocks[2][1] is not None

    [row_block] = query(database_url, "SELECT type FROM notion.blocks WHERE page_id = :p", p=ws.project_row)
    assert row_block[0] == "paragraph"

    [v] = query(database_url, "SELECT database_id::text, data_source_id::text, content FROM notion.views")
    assert (v[0], v[1]) == (ws.projects_db, ws.projects_ds)
    assert v[2]["filter"] == {"property": "Done", "checkbox": {"equals": True}}
    assert "url" not in v[2]

    dbs = dict(query(database_url, "SELECT database_id::text, is_linked FROM notion.databases"))
    assert dbs == {ws.projects_db: False, ws.tasks_db: False}


def test_snapshot_without_row_bodies(database_url, clock):
    ws = build_workspace()
    run_snapshot(ws.fake, database_url, clock, row_bodies=False)
    assert query(database_url, "SELECT count(*) FROM notion.blocks WHERE page_id = :p", p=ws.project_row)[0][0] == 0
    assert query(database_url, "SELECT count(*) FROM notion.blocks WHERE page_id = :p", p=ws.standalone_page)[0][0] == 3


@pytest.mark.parametrize("ddl", [
    "CREATE TABLE public.leftover (id int)",
    "CREATE SCHEMA old_snapshot",
    "CREATE VIEW public.v AS SELECT 1 AS x",
    "CREATE MATERIALIZED VIEW public.mv AS SELECT 1 AS x",
    "CREATE SEQUENCE public.s",
    "CREATE FUNCTION public.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    "CREATE TYPE public.mood AS ENUM ('ok')",
    "CREATE TYPE public.pair AS (a int, b int)",
    "CREATE DOMAIN public.posint AS int CHECK (VALUE > 0)",
])
def test_non_empty_target_raises_before_any_notion_request(database_url, clock, ddl):
    engine = create_target_engine(database_url)
    with engine.begin() as conn:
        conn.execute(text(ddl))
    engine.dispose()
    ws = build_workspace()
    with pytest.raises(TargetNotEmptyError):
        run_snapshot(ws.fake, database_url, clock)
    assert ws.fake.requests == []


def test_default_database_counts_as_empty(database_url):
    engine = create_target_engine(database_url)
    with engine.connect() as conn:
        assert_target_empty(conn)
    engine.dispose()


def test_failure_mid_run_rolls_back_everything(database_url, clock):
    ws = build_workspace()
    ws.fake.faults.append(
        lambda request: error_body(400, "validation_error") if request.url.path.startswith("/v1/views/") else None
    )
    with pytest.raises(NotionAPIError):
        run_snapshot(ws.fake, database_url, clock)
    assert ws.fake.count("POST", "data_sources") > 0
    engine = create_target_engine(database_url)
    with engine.connect() as conn:
        assert_target_empty(conn)
    engine.dispose()


def test_snapshot_sets_transaction_timeouts(database_url, clock):
    from notion2pg_bulk import orchestrate as module

    seen = {}
    original = module._Snapshot.run

    def spy(self):
        seen["statement"] = self.conn.execute(text("SHOW statement_timeout")).scalar()
        seen["idle"] = self.conn.execute(text("SHOW idle_in_transaction_session_timeout")).scalar()
        original(self)

    module._Snapshot.run = spy
    try:
        run_snapshot(build_workspace().fake, database_url, clock)
    finally:
        module._Snapshot.run = original
    assert seen == {"statement": "1min", "idle": "40min"}


def test_loading_same_records_twice_is_idempotent(database_url, clock):
    from notion2pg_bulk import fetch, load
    from notion2pg_bulk.notion_api import NotionAPI
    from notion2pg_bulk.notion_http import NotionHTTP
    from notion2pg_bulk.telemetry import Telemetry

    ws = build_workspace()
    run_snapshot(ws.fake, database_url, clock)
    before = dump_database(database_url)

    engine = create_target_engine(database_url)
    with NotionHTTP("t", "free", Telemetry("again"), transport=ws.fake.transport(), clock=clock, sleep=clock.sleep) as http:
        api = NotionAPI(http)
        with engine.begin() as conn:
            load.create_shared_schema(conn)
            sources = [fetch.fetch_data_source(api, ds) for ds in (ws.projects_ds, ws.tasks_ds)]
            load.load_databases(conn, [fetch.fetch_database(api, db) for db in (ws.projects_db, ws.tasks_db)])
            for source in sources:
                table = load.load_data_source(conn, source)
                for objects in fetch.query_rows(api, source.data_source_id):
                    load.load_rows(conn, source, table, [fetch.parse_row(api, o, source) for o in objects])
            load.load_pages(conn, fetch.discover_standalone_pages(api))
            for page_id in (ws.standalone_page, ws.project_row):
                load.load_page_body(conn, page_id, fetch.fetch_page_body(api, page_id))
            load.load_views(conn, fetch.fetch_views(api, sources))
    engine.dispose()
    assert dump_database(database_url) == before


def test_pages_parented_by_database_id_are_kept(database_url, clock):
    from fake_notion import new_id, page, paragraph, title_value

    ws = build_workspace()
    legacy = new_id()
    ws.fake.add_page(page(legacy, {"type": "database_id", "database_id": ws.projects_db}, {"Name": title_value("Legacy")}))
    ws.fake.set_children(legacy, [paragraph("legacy body")])
    template = new_id()
    ws.fake.add_page(page(template, {"type": "data_source_id", "data_source_id": ws.projects_ds,
                                     "database_id": ws.projects_db}, {"Name": title_value("Template")}))
    run_snapshot(ws.fake, database_url, clock)
    [found] = query(database_url, "SELECT parent_type, parent_id::text, title FROM notion.pages WHERE page_id = :p", p=legacy)
    assert (found[0], found[1], found[2][0]["plain_text"]) == ("database_id", ws.projects_db, "Legacy")
    assert query(database_url, "SELECT count(*) FROM notion.blocks WHERE page_id = :p", p=legacy)[0][0] == 1
    assert query(database_url, "SELECT count(*) FROM notion.pages WHERE page_id = :p", p=template)[0][0] == 0
