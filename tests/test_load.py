"""Load layer limits."""

from __future__ import annotations

from fake_notion import data_source, new_id, page, title_value
from notion2pg_bulk import load
from notion2pg_bulk.orchestrate import create_target_engine
from notion2pg_bulk.records import RowRecord, parse_data_source, parse_page
from test_snapshot import query

COLUMNS = 200
ROWS = 400


def test_wide_data_source_loads_within_the_parameter_limit(database_url):
    properties = {"Name": {"id": "title", "name": "Name", "description": None, "type": "title", "title": {}}}
    properties.update({f"N{i}": {"id": f"n{i}", "name": f"N{i}", "description": None, "type": "number", "number": {}}
                       for i in range(COLUMNS - 1)})
    ds_id, db_id = new_id(), new_id()
    source = parse_data_source(data_source(ds_id, db_id, "Wide", properties))
    rows = []
    for index in range(ROWS):
        obj = page(new_id(), {"type": "data_source_id", "data_source_id": ds_id, "database_id": db_id},
                   {"Name": title_value(f"r{index}")})
        values = {"title": [{"plain_text": f"r{index}"}], **{f"n{i}": index for i in range(COLUMNS - 1)}}
        rows.append(RowRecord(page=parse_page(obj, is_row=True), values=values))
    assert (COLUMNS + 1) * min(ROWS, load.BATCH_SIZE) > 65_535

    engine = create_target_engine(database_url)
    with engine.begin() as conn:
        load.create_shared_schema(conn)
        table = load.load_data_source(conn, source)
        load.load_rows(conn, source, table, rows)
    engine.dispose()
    assert query(database_url, 'SELECT count(*), sum("N198") FROM rows."Wide"')[0] == (ROWS, sum(range(ROWS)))
