"""Property type → column type and value mapping, including empty values."""

from __future__ import annotations

import pytest
from sqlalchemy import BigInteger, Boolean, Numeric, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP, UUID

from notion2pg_bulk.mapping import column_type, column_value
from workspace import USER_ID

PAGE = "dd456007-6c66-4bba-957e-ea501dcda3a6"
TEXT = [{"type": "text", "text": {"content": "x", "link": None}, "plain_text": "x", "href": None}]
DATE = {"start": "2026-09-15T09:00:00", "end": "2026-09-18T09:00:00", "time_zone": "America/Los_Angeles"}
PLACE = {"lat": 1.5, "lon": 2.5, "name": "n", "address": "a", "aws_place_id": "aws", "google_place_id": "g"}
VERIFICATION = {"state": "verified", "date": {"start": "2026-09-01T09:00:00.000Z", "end": None, "time_zone": None},
                "verified_by": {"object": "user", "id": USER_ID}}

# (type, raw value, stored value, empty raw value, column type class)
CASES = [
    ("title", TEXT, TEXT, [], JSONB),
    ("rich_text", TEXT, TEXT, [], JSONB),
    ("number", 3.25, 3.25, None, Numeric),
    ("checkbox", True, True, None, Boolean),
    ("url", "https://e.x", "https://e.x", None, Text),
    ("email", "a@e.x", "a@e.x", None, Text),
    ("phone_number", "+1", "+1", None, Text),
    ("select", {"id": "opt1", "name": "A", "color": "red"}, "opt1", None, Text),
    ("status", {"id": "st1", "name": "Done", "color": "green"}, "st1", None, Text),
    ("multi_select", [{"id": "o1", "name": "A"}, {"id": "o2", "name": "B"}], ["o1", "o2"], [], ARRAY),
    ("date", DATE, DATE, None, JSONB),
    ("place", PLACE, PLACE, None, JSONB),
    ("verification", VERIFICATION, VERIFICATION, None, JSONB),
    ("relation", [{"id": PAGE}], [PAGE], [], ARRAY),
    ("people", [{"object": "user", "id": USER_ID}], [USER_ID], [], ARRAY),
    ("unique_id", {"number": 7, "prefix": "T"}, 7, None, BigInteger),
    ("created_time", "2026-09-01T09:00:00.000Z", "2026-09-01T09:00:00.000Z", None, TIMESTAMP),
    ("last_edited_time", "2026-09-01T09:00:00.000Z", "2026-09-01T09:00:00.000Z", None, TIMESTAMP),
    ("created_by", {"object": "user", "id": USER_ID}, USER_ID, None, UUID),
    ("last_edited_by", {"object": "user", "id": USER_ID}, USER_ID, None, UUID),
    ("future_type", {"anything": [1, 2]}, {"anything": [1, 2]}, None, JSONB),
]


@pytest.mark.parametrize("prop_type,raw,stored,empty,sql_type", CASES, ids=[c[0] for c in CASES])
def test_value_and_empty_value(prop_type, raw, stored, empty, sql_type):
    assert isinstance(column_type(prop_type), sql_type)
    assert column_value({"id": "p", "type": prop_type, prop_type: raw}) == stored
    assert column_value({"id": "p", "type": prop_type, prop_type: empty}) is None


def test_timestamps_keep_time_zone():
    assert column_type("created_time").timezone is True
    assert column_type("last_edited_time").timezone is True


def test_checkbox_false_is_not_empty():
    assert column_value({"id": "d", "type": "checkbox", "checkbox": False}) is False


def test_date_only_value_is_kept_verbatim():
    date = {"start": "2026-09-15", "end": None, "time_zone": None}
    assert column_value({"id": "d", "type": "date", "date": date}) == date


@pytest.mark.parametrize("prop_type", ["button", "files", "formula", "rollup"])
def test_types_without_column(prop_type):
    assert column_type(prop_type) is None
