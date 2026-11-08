"""Rows: values over 25 items, 10k windowing, wiki results and schema drift."""

from __future__ import annotations

import pytest

from fake_notion import FakeNotion, data_source, new_id, page, page_mention, rich_text, title_value, user
from helpers import run_snapshot
from notion2pg_bulk import fetch
from notion2pg_bulk.errors import SnapshotIntegrityError
from notion2pg_bulk.notion_api import NotionAPI
from notion2pg_bulk.notion_http import NotionHTTP
from notion2pg_bulk.records import parse_data_source
from notion2pg_bulk.telemetry import Telemetry
from test_snapshot import query
from workspace import build_workspace

TITLE = {"id": "title", "name": "Name", "description": None, "type": "title", "title": {}}


def api_for(fake: FakeNotion, clock) -> NotionAPI:
    return NotionAPI(NotionHTTP("t", "free", Telemetry("p"), transport=fake.transport(), clock=clock, sleep=clock.sleep))


def simple_source(fake: FakeNotion, properties=None):
    ds_id, db_id = new_id(), new_id()
    obj = data_source(ds_id, db_id, "Items", {"Name": TITLE, **(properties or {})})
    fake.add_data_source(obj)
    return ds_id, db_id, parse_data_source(obj)


def row(ds_id, db_id, created="2026-01-01T00:00:00.000Z", **values):
    return page(new_id(), {"type": "data_source_id", "data_source_id": ds_id, "database_id": db_id},
                {"Name": title_value("r"), **values}, created_time=created)


# ---------------------------------------------------------------- values over 25

RELATED = [new_id() for _ in range(130)]


@pytest.mark.parametrize("prop_type,config,truncated,items,expected", [
    ("relation", {"relation": {"data_source_id": new_id(), "database_id": new_id(), "type": "single_property",
                               "single_property": {}}},
     [{"id": r} for r in RELATED[:25]],
     [{"object": "property_item", "id": "M%3BBw", "type": "relation", "relation": {"id": r}} for r in RELATED],
     RELATED),
    ("people", {"people": {}},
     [user(u) for u in RELATED[:25]],
     [{"object": "property_item", "id": "M%3BBw", "type": "people", "people": user(u)} for u in RELATED[:40]],
     RELATED[:40]),
])
def test_values_over_25_items_are_complete(clock, prop_type, config, truncated, items, expected):
    fake = FakeNotion()
    ds_id, db_id, source = simple_source(fake, {"Many": {"id": "M%3BBw", "name": "Many", "description": None,
                                                         "type": prop_type, **config}})
    obj = row(ds_id, db_id, Many={"id": "M%3BBw", "type": prop_type, prop_type: truncated, "has_more": True})
    fake.add_row(ds_id, obj)
    fake.property_items[(obj["id"], "M%3BBw")] = items
    record = fetch.parse_row(api_for(fake, clock), obj, source)
    assert record.values["M%3BBw"] == expected
    paths = [r.url.raw_path.decode() for r in fake.requests]
    assert all("M%3BBw" in p for p in paths) and len(paths) == -(-len(items) // 100)
    assert all("start_cursor" not in r.url.params for r in fake.requests[:1])


def test_title_with_more_than_25_mentions_is_complete(clock):
    fake = FakeNotion()
    ds_id, db_id, source = simple_source(fake)
    mentions = [page_mention(p) for p in RELATED[:30]]
    obj = row(ds_id, db_id)
    obj["properties"]["Name"] = {"id": "title", "type": "title", "title": mentions[:25], "has_more": True}
    fake.property_items[(obj["id"], "title")] = [
        {"object": "property_item", "id": "title", "type": "title", "title": m} for m in mentions
    ]
    record = fetch.parse_row(api_for(fake, clock), obj, source)
    assert record.values["title"] == mentions


def test_has_more_on_non_paginated_type_raises(clock):
    fake = FakeNotion()
    ds_id, db_id, source = simple_source(fake, {"N": {"id": "num", "name": "N", "description": None,
                                                      "type": "number", "number": {}}})
    obj = row(ds_id, db_id, N={"id": "num", "type": "number", "number": 1, "has_more": True})
    with pytest.raises(SnapshotIntegrityError):
        fetch.parse_row(api_for(fake, clock), obj, source)


def test_values_without_has_more_cost_no_request(clock):
    fake = FakeNotion()
    ds_id, db_id, source = simple_source(fake)
    fetch.parse_row(api_for(fake, clock), row(ds_id, db_id), source)
    assert fake.requests == []


# ---------------------------------------------------------------- 10k windowing

def minute(index: int) -> str:
    return f"2026-01-01T{index // 60:02d}:{index % 60:02d}:00.000Z"


def test_windowing_reads_past_the_result_limit(clock):
    fake = FakeNotion(query_cap=5, page_size_cap=2)
    ds_id, db_id, _ = simple_source(fake)
    created = [minute(i // 2) for i in range(23)]
    expected = []
    for stamp in created:
        obj = row(ds_id, db_id, created=stamp)
        fake.add_row(ds_id, obj)
        expected.append(obj["id"])
    seen = [o["id"] for batch in fetch.query_rows(api_for(fake, clock), ds_id) for o in batch]
    assert sorted(seen) == sorted(expected) and len(seen) == len(set(seen))
    bodies = [r for r in fake.requests if r.url.path.endswith("/query")]
    assert all(b'"created_time","direction":"ascending"' in r.content for r in bodies)
    assert sum(b"on_or_after" in r.content for r in bodies) >= 4


def test_more_than_limit_in_one_minute_raises(clock):
    fake = FakeNotion(query_cap=5)
    ds_id, db_id, _ = simple_source(fake)
    for _ in range(6):
        fake.add_row(ds_id, row(ds_id, db_id, created="2026-01-01T00:00:00.000Z"))
    with pytest.raises(SnapshotIntegrityError):
        list(fetch.query_rows(api_for(fake, clock), ds_id))


def test_wiki_data_source_results_are_not_rows(clock):
    fake = FakeNotion()
    ds_id, db_id, _ = simple_source(fake)
    real = row(ds_id, db_id)
    fake.add_row(ds_id, real)
    fake.query_extras[ds_id] = [data_source(new_id(), db_id, "Child", {"Name": TITLE},
                                            created_time="2026-01-02T00:00:00.000Z")]
    seen = [o["id"] for batch in fetch.query_rows(api_for(fake, clock), ds_id) for o in batch]
    assert seen == [real["id"]]


def test_wiki_data_source_results_advance_the_window(clock):
    fake = FakeNotion(query_cap=2)
    ds_id, db_id, _ = simple_source(fake)
    rows = [row(ds_id, db_id, created=minute(i)) for i in (0, 3)]
    for obj in rows:
        fake.add_row(ds_id, obj)
    fake.query_extras[ds_id] = [data_source(new_id(), db_id, "Child", {"Name": TITLE}, created_time=minute(1)),
                                data_source(new_id(), db_id, "Child2", {"Name": TITLE}, created_time=minute(2))]
    seen = [o["id"] for batch in fetch.query_rows(api_for(fake, clock), ds_id) for o in batch]
    assert sorted(seen) == sorted(o["id"] for o in rows)


# ---------------------------------------------------------------- schema drift (end to end)

def test_property_added_during_the_run_gets_a_column(database_url, clock):
    ws = build_workspace()
    new_row = row(ws.tasks_ds, ws.tasks_db, created="2026-04-01T00:00:00.000Z",
                  Points={"id": "pts", "type": "number", "number": 5})
    new_row["properties"]["Project"] = {"id": "back", "type": "relation", "relation": [], "has_more": False}
    ws.fake.add_row(ws.tasks_ds, new_row)
    original = ws.fake.data_sources[ws.tasks_ds]
    stale = {**original, "properties": dict(original["properties"])}
    original["properties"]["Points"] = {"id": "pts", "name": "Points", "description": None, "type": "number", "number": {}}
    served = {"n": 0}

    def stale_first_read(request):
        if request.method == "GET" and request.url.path == f"/v1/data_sources/{ws.tasks_ds}" and served["n"] == 0:
            served["n"] += 1
            import httpx
            return httpx.Response(200, json=stale)
        return None

    ws.fake.faults.append(stale_first_read)
    run_snapshot(ws.fake, database_url, clock)
    values = dict(query(database_url, 'SELECT _page_id::text, "Points" FROM rows."Tasks"'))
    assert values[new_row["id"]] == 5
    assert query(database_url, "SELECT column_name FROM notion.catalog_columns WHERE property_id = 'pts'")[0][0] == "Points"


def test_property_type_change_during_the_run_raises(database_url, clock):
    ws = build_workspace()
    ws.fake.pages[ws.task_rows[0]]["properties"]["Project"] = {"id": "back", "type": "number", "number": 1}
    original = ws.fake.data_sources[ws.tasks_ds]
    first = {**original, "properties": dict(original["properties"])}
    original["properties"]["Project"] = {"id": "back", "name": "Project", "description": None, "type": "number", "number": {}}
    served = {"n": 0}

    def stale_first_read(request):
        if request.method == "GET" and request.url.path == f"/v1/data_sources/{ws.tasks_ds}" and served["n"] == 0:
            served["n"] += 1
            import httpx
            return httpx.Response(200, json=first)
        return None

    ws.fake.faults.append(stale_first_read)
    with pytest.raises(SnapshotIntegrityError):
        run_snapshot(ws.fake, database_url, clock)


def test_unknown_option_without_schema_change_raises(clock):
    fake = FakeNotion()
    ds_id, db_id, source = simple_source(fake, {"S": {"id": "sel", "name": "S", "description": None, "type": "select",
                                                      "select": {"options": []}}})
    obj = row(ds_id, db_id, S={"id": "sel", "type": "select", "select": {"id": "ghost", "name": "G", "color": "red"}})
    assert fetch.schema_mismatches(obj, source) == ["unknown option ghost of property sel"]
    with pytest.raises(SnapshotIntegrityError):
        fetch.parse_row(api_for(fake, clock), obj, source)
