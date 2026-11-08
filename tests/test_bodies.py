"""Page bodies: walk exclusions, structure of tabs and tables, depth limit."""

from __future__ import annotations

import pytest

from fake_notion import FakeNotion, block, new_id, paragraph, rich_text
from notion2pg_bulk import fetch
from notion2pg_bulk.errors import SnapshotIntegrityError
from test_rows import api_for


def walk(fake, clock, page_id):
    return fetch.fetch_page_body(api_for(fake, clock), page_id)


def test_walk_excludes_child_pages_databases_synced_copies_and_media(clock):
    fake = FakeNotion()
    page_id = new_id()
    child_page = block("child_page", {"title": "Sub"}, has_children=True)
    child_db = block("child_database", {"title": "Inline DB"}, has_children=True)
    original = block("synced_block", {"synced_from": None}, has_children=True)
    copy = block("synced_block", {"synced_from": {"type": "block_id", "block_id": original["id"]}}, has_children=True)
    media = [block(t, {"type": "external", "external": {"url": "https://e.x/a"}, "caption": []})
             for t in ("image", "video", "audio", "file", "pdf")]
    hosted_embed = block("embed", {"type": "file_upload", "file_upload": {"id": new_id()}, "url": "https://s3/x"})
    plain_embed = block("embed", {"url": "https://companywebsite.com"})
    fake.set_children(page_id, [child_page, child_db, original, copy, *media, hosted_embed, plain_embed])
    fake.set_children(child_page["id"], [paragraph("must not be read")])
    fake.set_children(child_db["id"], [paragraph("must not be read")])
    fake.set_children(original["id"], [paragraph("synced content")])
    fake.set_children(copy["id"], [paragraph("duplicate")])

    blocks = walk(fake, clock, page_id)
    by_id = {b.block_id: b for b in blocks}
    assert set(by_id) == {child_page["id"], child_db["id"], original["id"], copy["id"], plain_embed["id"],
                          next(b.block_id for b in blocks if b.parent_block_id == original["id"])}
    assert by_id[copy["id"]].content == {"synced_from": {"type": "block_id", "block_id": original["id"]}}
    walked = {r.url.path for r in fake.requests}
    assert f"/v1/blocks/{child_page['id']}/children" not in walked
    assert f"/v1/blocks/{child_db['id']}/children" not in walked
    assert f"/v1/blocks/{copy['id']}/children" not in walked
    assert all(b.page_id == page_id for b in blocks)


def test_tab_and_table_structure(clock):
    fake = FakeNotion()
    page_id = new_id()
    tab = block("tab", {}, has_children=True)
    first = paragraph("Overview", has_children=True, icon={"type": "emoji", "emoji": "📋"})
    second = paragraph("Details", has_children=True)
    table = block("table", {"table_width": 2, "has_column_header": True, "has_row_header": False}, has_children=True)
    rows = [block("table_row", {"cells": [[rich_text("a")], [rich_text("b")]]}),
            block("table_row", {"cells": [[rich_text("c")], []]})]
    fake.set_children(page_id, [tab])
    fake.set_children(tab["id"], [first, second])
    fake.set_children(first["id"], [paragraph("Tab 1 content")])
    fake.set_children(second["id"], [table])
    fake.set_children(table["id"], rows)

    blocks = {b.block_id: b for b in walk(fake, clock, page_id)}
    assert blocks[first["id"]].parent_block_id == tab["id"] and blocks[first["id"]].position == 0
    assert blocks[second["id"]].position == 1
    assert blocks[first["id"]].content["icon"] == {"type": "emoji", "emoji": "📋"}
    assert blocks[table["id"]].parent_block_id == second["id"]
    assert [blocks[r["id"]].position for r in rows] == [0, 1]
    assert blocks[rows[1]["id"]].content["cells"][1] == []
    assert len(blocks) == 7


def test_positions_keep_api_order_around_excluded_blocks(clock):
    fake = FakeNotion()
    page_id = new_id()
    kept = [paragraph("a"), paragraph("b")]
    fake.set_children(page_id, [kept[0], block("image", {"type": "external", "external": {"url": "u"}}), kept[1]])
    positions = {b.block_id: b.position for b in walk(fake, clock, page_id)}
    assert positions == {kept[0]["id"]: 0, kept[1]["id"]: 2}


def test_depth_limit_raises(clock):
    fake = FakeNotion()
    page_id = new_id()
    parent = page_id
    for level in range(101):
        child = paragraph("deeper", has_children=level < 100)
        fake.set_children(parent, [child])
        parent = child["id"]
    with pytest.raises(SnapshotIntegrityError):
        walk(fake, clock, page_id)


def test_depth_within_limit_is_walked(clock):
    fake = FakeNotion()
    page_id = new_id()
    parent = page_id
    for level in range(100):
        child = paragraph("deeper", has_children=level < 99)
        fake.set_children(parent, [child])
        parent = child["id"]
    assert len(walk(fake, clock, page_id)) == 100


def test_unsupported_blocks_are_stored_but_not_walked(clock):
    fake = FakeNotion()
    page_id = new_id()
    form = block("unsupported", {"block_type": "form"}, has_children=True)
    fake.set_children(page_id, [form])
    fake.set_children(form["id"], [paragraph("must not be read")])
    blocks = walk(fake, clock, page_id)
    assert [(b.type, b.content) for b in blocks] == [("unsupported", {"block_type": "form"})]
    assert f"/v1/blocks/{form['id']}/children" not in {r.url.path for r in fake.requests}
