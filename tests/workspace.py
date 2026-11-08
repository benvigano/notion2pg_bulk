"""A representative fake workspace covering every property type."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from fake_notion import (
    FakeNotion, block, data_source, database, new_id, page, paragraph, rich_text, title_value, user, view,
)

OPTION_HIGH = "ff8e9269-9579-47f7-8f6e-83a84716863c"
OPTION_LOW = "a1b2c3d4-0000-4000-8000-000000000001"
OPTION_TAG = "tag1"
STATUS_DONE = "330aeafb-598c-4e1c-bc13-1148aa5963d3"
GROUP_COMPLETE = "4fa7348e-ae74-46d9-9585-e773caca6f40"
USER_ID = "c2f20311-9e54-4d11-8c79-7398424ae41e"


def project_properties(tasks_ds: str, tasks_db: str) -> Dict[str, dict]:
    return {
        "Name": {"id": "title", "name": "Name", "description": None, "type": "title", "title": {}},
        "Priority": {"id": "prio", "name": "Priority", "description": "How urgent", "type": "select",
                     "select": {"options": [
                         {"id": OPTION_HIGH, "name": "High", "color": "red", "description": None},
                         {"id": OPTION_LOW, "name": "Low", "color": "gray", "description": "Later"}]}},
        "Tags": {"id": "tags", "name": "Tags", "description": None, "type": "multi_select",
                 "multi_select": {"options": [{"id": OPTION_TAG, "name": "Infra", "color": "blue", "description": None}]}},
        "Status": {"id": "stat", "name": "Status", "description": None, "type": "status",
                   "status": {"default_option": "Done",
                              "options": [{"id": STATUS_DONE, "name": "Done", "color": "green", "description": None}],
                              "groups": [{"id": GROUP_COMPLETE, "name": "Complete", "color": "green",
                                          "option_ids": [STATUS_DONE]}]}},
        "Estimate": {"id": "estM", "name": "Estimate", "description": None, "type": "number", "number": {"format": "number"}},
        "Done": {"id": "done", "name": "Done", "description": None, "type": "checkbox", "checkbox": {}},
        "Due date": {"id": "M%3BBw", "name": "Due date", "description": None, "type": "date", "date": {}},
        "Website": {"id": "web", "name": "Website", "description": None, "type": "url", "url": {}},
        "Contact email": {"id": "emlA", "name": "Contact email", "description": None, "type": "email", "email": {}},
        "Contact phone": {"id": "phon", "name": "Contact phone", "description": None, "type": "phone_number", "phone_number": {}},
        "Owner": {"id": "ownR", "name": "Owner", "description": None, "type": "people", "people": {}},
        "Tasks": {"id": "proj", "name": "Tasks", "description": None, "type": "relation",
                  "relation": {"database_id": tasks_db, "data_source_id": tasks_ds, "limit": None,
                               "type": "single_property", "single_property": {}}},
        "Created time": {"id": "crtT", "name": "Created time", "description": None, "type": "created_time", "created_time": {}},
        "Created by": {"id": "crtB", "name": "Created by", "description": None, "type": "created_by", "created_by": {}},
        "Last edited time": {"id": "edtT", "name": "Last edited time", "description": None, "type": "last_edited_time", "last_edited_time": {}},
        "Last edited by": {"id": "edtB", "name": "Last edited by", "description": None, "type": "last_edited_by", "last_edited_by": {}},
        "ID": {"id": "uniq", "name": "ID", "description": None, "type": "unique_id", "unique_id": {"prefix": "PRJ"}},
        "Office": {"id": "plce", "name": "Office", "description": None, "type": "place", "place": {}},
        "Double estimate": {"id": "calc", "name": "Double estimate", "description": None, "type": "formula",
                            "formula": {"expression": "prop(\"Estimate\") * 2"}},
        "Total": {"id": "roll", "name": "Total", "description": None, "type": "rollup",
                  "rollup": {"relation_property_id": "proj", "relation_property_name": "Tasks",
                             "rollup_property_id": "title", "rollup_property_name": "Name", "function": "count"}},
        "Send update": {"id": "btnA", "name": "Send update", "description": None, "type": "button", "button": {}},
        "Attachments": {"id": "file", "name": "Attachments", "description": None, "type": "files", "files": {}},
        "Summary": {"id": "text", "name": "Summary", "description": None, "type": "rich_text", "rich_text": {}},
        "Hologram": {"id": "holo", "name": "Hologram", "description": None, "type": "hologram", "hologram": {}},
    }


def project_values(task_ids, created_time="2026-02-01T10:00:00.000Z") -> Dict[str, dict]:
    return {
        "Name": title_value("Launch"),
        "Priority": {"id": "prio", "type": "select", "select": {"id": OPTION_HIGH, "name": "High", "color": "red"}},
        "Tags": {"id": "tags", "type": "multi_select", "multi_select": [{"id": OPTION_TAG, "name": "Infra", "color": "blue"}]},
        "Status": {"id": "stat", "type": "status", "status": {"id": STATUS_DONE, "name": "Done", "color": "green"}},
        "Estimate": {"id": "estM", "type": "number", "number": 8.5},
        "Done": {"id": "done", "type": "checkbox", "checkbox": True},
        "Due date": {"id": "M%3BBw", "type": "date", "date": {"start": "2026-09-15", "end": None, "time_zone": None}},
        "Website": {"id": "web", "type": "url", "url": "https://example.com"},
        "Contact email": {"id": "emlA", "type": "email", "email": "alex@example.com"},
        "Contact phone": {"id": "phon", "type": "phone_number", "phone_number": "+1 415 555 0123"},
        "Owner": {"id": "ownR", "type": "people", "people": [user(USER_ID)]},
        "Tasks": {"id": "proj", "type": "relation", "relation": [{"id": t} for t in task_ids], "has_more": False},
        "Created time": {"id": "crtT", "type": "created_time", "created_time": created_time},
        "Created by": {"id": "crtB", "type": "created_by", "created_by": user(USER_ID)},
        "Last edited time": {"id": "edtT", "type": "last_edited_time", "last_edited_time": "2026-09-04T14:30:00.000Z"},
        "Last edited by": {"id": "edtB", "type": "last_edited_by", "last_edited_by": user(USER_ID)},
        "ID": {"id": "uniq", "type": "unique_id", "unique_id": {"number": 42, "prefix": "PRJ"}},
        "Office": {"id": "plce", "type": "place", "place": {"lat": 37.7749, "lon": -122.4194, "name": "San Francisco",
                                                           "address": None, "aws_place_id": None, "google_place_id": None}},
        "Double estimate": {"id": "calc", "type": "formula", "formula": {"type": "number", "number": 17}},
        "Total": {"id": "roll", "type": "rollup", "rollup": {"type": "number", "number": 1, "function": "count"}},
        "Send update": {"id": "btnA", "type": "button", "button": {}},
        "Attachments": {"id": "file", "type": "files", "files": []},
        "Summary": {"id": "text", "type": "rich_text", "rich_text": [rich_text("Bold", bold=True)]},
        "Hologram": {"id": "holo", "type": "hologram", "hologram": {"beam": 3}},
    }


@dataclass
class Workspace:
    fake: FakeNotion
    projects_db: str
    projects_ds: str
    tasks_db: str
    tasks_ds: str
    project_row: str
    task_rows: list
    standalone_page: str
    table_view: str


def build_workspace() -> Workspace:
    fake = FakeNotion()
    projects_db, projects_ds, tasks_db, tasks_ds = new_id(), new_id(), new_id(), new_id()
    fake.add_database(database(projects_db, "Projects DB", [projects_ds]))
    fake.add_database(database(tasks_db, "Tasks DB", [tasks_ds]))
    fake.add_data_source(data_source(projects_ds, projects_db, "Projects", project_properties(tasks_ds, tasks_db)))
    fake.add_data_source(data_source(tasks_ds, tasks_db, "Tasks", {
        "Name": {"id": "title", "name": "Name", "description": None, "type": "title", "title": {}},
        "Project": {"id": "back", "name": "Project", "description": None, "type": "relation",
                    "relation": {"database_id": projects_db, "data_source_id": projects_ds, "limit": 1,
                                 "type": "dual_property",
                                 "dual_property": {"synced_property_id": "proj", "synced_property_name": "Tasks"}}},
    }))
    task_rows = []
    for index in range(3):
        task_id = new_id()
        task_rows.append(task_id)
        fake.add_row(tasks_ds, page(task_id, {"type": "data_source_id", "data_source_id": tasks_ds, "database_id": tasks_db},
                                    {"Name": title_value(f"Task {index}"),
                                     "Project": {"id": "back", "type": "relation", "relation": [], "has_more": False}},
                                    created_time=f"2026-03-0{index + 1}T08:00:00.000Z"))
    project_row = new_id()
    fake.add_row(projects_ds, page(project_row, {"type": "data_source_id", "data_source_id": projects_ds, "database_id": projects_db},
                                   project_values(task_rows)))
    fake.set_children(project_row, [paragraph("Row body")])

    standalone = new_id()
    fake.add_page(page(standalone, {"type": "workspace", "workspace": True}, {"title": title_value("Handbook")}))
    toggle_id = new_id()
    fake.set_children(standalone, [
        block("heading_1", {"rich_text": [rich_text("Intro")], "color": "blue_background", "is_toggleable": True},
              has_children=True, block_id=toggle_id),
        paragraph("Second"),
    ])
    fake.set_children(toggle_id, [paragraph("Nested")])

    table_view = new_id()
    fake.add_view(view(table_view, projects_db, projects_ds, filter={"property": "Done", "checkbox": {"equals": True}}))
    return Workspace(fake, projects_db, projects_ds, tasks_db, tasks_ds, project_row, task_rows, standalone, table_view)
