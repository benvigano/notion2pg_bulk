"""One-off workspace stats: same Notion access as migrator (Client + RateLimiter + search/query)."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from notion_client import Client

from notion2pg_bulk.rate_limiter import RateLimiter


def _percentile_nearest_rank(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 100:
        return float(sorted_vals[-1])
    k = math.ceil((p / 100.0) * len(sorted_vals)) - 1
    return float(sorted_vals[max(0, k)])


def _database_title(db: dict) -> str:
    title_prop = db.get("title", [])
    if title_prop:
        return "".join(item.get("plain_text", "") for item in title_prop)
    return f"Untitled_{db.get('id', '')[:8]}"


def _title_property_id(details: dict) -> str | None:
    for prop in details.get("properties", {}).values():
        if prop.get("type") == "title":
            pid = prop.get("id")
            if pid:
                return pid
    return None


def _discover_databases(notion: Client, limiter: RateLimiter) -> list[dict]:
    out: list[dict] = []
    start_cursor = None
    while True:
        resp = limiter.rate_limited_call(
            notion.search,
            filter={"property": "object", "value": "database"},
            start_cursor=start_cursor,
        )
        out.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return out


def _count_rows_title_only(
    notion: Client, limiter: RateLimiter, database_id: str, title_prop_id: str | None
) -> tuple[int, int]:
    """Return (row_count, utf8_bytes_of_query_response_bodies_for_this_db)."""
    total_rows = 0
    total_bytes = 0
    q_cursor = None
    while True:
        kwargs: dict = {
            "database_id": database_id,
            "start_cursor": q_cursor,
            "page_size": 100,
        }
        if title_prop_id:
            kwargs["filter_properties"] = [title_prop_id]
        q = limiter.rate_limited_call(notion.databases.query, **kwargs)
        pages = q.get("results", [])
        total_rows += len(pages)
        total_bytes += len(
            json.dumps(q, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
        if not q.get("has_more"):
            break
        q_cursor = q.get("next_cursor")
    return total_rows, total_bytes


def main() -> int:
    p = argparse.ArgumentParser(
        description=(
            "Workspace stats via Notion API. Exact row counts require paginating "
            "databases.query (no total field in the API). Use --count-rows to do that "
            "with title-only properties per page; otherwise only database discovery runs."
        )
    )
    p.add_argument(
        "--count-rows",
        action="store_true",
        help=(
            "Paginate each database with filter_properties=title only; counts rows and "
            "sums UTF-8 JSON size of each query response (not full row payloads)."
        ),
    )
    args = p.parse_args()

    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("NOTION_TOKEN is not set", file=sys.stderr)
        return 1

    notion = Client(auth=token)
    limiter = RateLimiter(requests_per_second=2.5)

    databases = _discover_databases(notion, limiter)
    n_db = len(databases)

    print("Notion workspace snapshot")
    print("-" * 60)
    print(f"Total databases: {n_db}")

    if not args.count_rows:
        print(
            "\nRow counts / averages / percentiles / payload MiB were skipped "
            "(each needs one query page per ≤100 rows per database). "
            "Re-run with --count-rows to scan with title-only responses."
        )
        if databases:
            print("\nDatabases (from search):")
            for db in databases:
                print(f"  - {_database_title(db)!r}  ({db['id']})")
        return 0

    counts_per_db: list[int] = []
    bytes_per_db: list[int] = []
    total_bytes = 0

    for db in databases:
        db_id = db["id"]
        details = limiter.rate_limited_call(notion.databases.retrieve, database_id=db_id)
        title_pid = _title_property_id(details)
        n_rows, n_bytes = _count_rows_title_only(notion, limiter, db_id, title_pid)
        counts_per_db.append(n_rows)
        bytes_per_db.append(n_bytes)
        total_bytes += n_bytes

    total_records = sum(counts_per_db)
    avg = (total_records / n_db) if n_db else 0.0
    sorted_counts = sorted(counts_per_db)
    pcts = [50, 75, 90, 95, 99]
    mib = total_bytes / (1024 * 1024)

    print(f"\nTotal records (pages, title-only query projection): {total_records}")
    print(f"Avg records per db: {avg:.4f}")
    print("Records per db (percentiles, nearest-rank):")
    for pct in pcts:
        print(f"  p{pct}: {_percentile_nearest_rank(sorted_counts, float(pct))}")
    print(
        f"\nUTF-8 size of each full query JSON response (title-only pages), summed: "
        f"{total_bytes} bytes ({mib:.6f} MiB, {total_bytes / 1_000_000:.6f} decimal MB)"
    )
    print(
        "\nNote: Notion does not expose row totals without pagination; "
        "this mode still issues ~ceil(rows/100) requests per database but avoids "
        "returning non-title properties on each page."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
