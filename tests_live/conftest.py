"""Live integration tests against a real, disposable Notion workspace.

Required environment (see README → Live integration tests):
- ``NOTION_TOKEN``: internal connection token of the disposable workspace;
- ``NOTION2PG_LIVE_ROOT_PAGE_ID``: the page created for the tests and shared with the connection;
- ``NOTION2PG_TEST_ADMIN_URL``: superuser URL of a PostgreSQL server (databases are created and dropped).
Optional: ``NOTION2PG_LIVE_PLAN`` (default ``free``).
"""

from __future__ import annotations

import json
import logging
import os
import ssl
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List

import httpx
import pytest

from live_db import create_database, drop_database
from live_rest import NotionREST
from live_workspace import Workspace, build, teardown, wait_for_search
from notion2pg_bulk import snapshot
from notion2pg_bulk.notion_api import NotionAPI
from notion2pg_bulk.notion_http import NotionHTTP
from notion2pg_bulk.orchestrate import create_target_engine, run
from notion2pg_bulk.telemetry import LOGGER_NAME, Telemetry

REQUIRED = ("NOTION_TOKEN", "NOTION2PG_LIVE_ROOT_PAGE_ID", "NOTION2PG_TEST_ADMIN_URL")


def pytest_collection_modifyitems(config, items):
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        skip = pytest.mark.skip(reason="live tests need " + ", ".join(missing) + " (README → Live integration tests)")
        for item in items:
            item.add_marker(skip)


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.events: List[Dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        payload = getattr(record, "telemetry", None)
        if payload is not None:
            self.events.append(payload)


class SearchFilter(httpx.BaseTransport):
    """Live transport that drops chosen IDs from search results (search can miss items; this makes it happen)."""

    def __init__(self, hidden: List[str]):
        self.hidden = set(hidden)
        cafile = os.environ.get("SSL_CERT_FILE")
        self.inner = httpx.HTTPTransport(
            proxy=os.environ.get("HTTPS_PROXY") or None,
            verify=ssl.create_default_context(cafile=cafile) if cafile else True,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self.inner.handle_request(request)
        if request.method != "POST" or request.url.path != "/v1/search":
            return response
        response.read()
        payload = json.loads(response.content)
        payload["results"] = [result for result in payload.get("results", []) if result["id"] not in self.hidden]
        headers = {k: v for k, v in response.headers.items() if k.lower() not in ("content-length", "content-encoding")}
        return httpx.Response(response.status_code, headers=headers, content=json.dumps(payload).encode())


@dataclass
class Runs:
    """Snapshot runs of the live workspace and their telemetry."""

    workspace: Workspace
    urls: Dict[str, str] = field(default_factory=dict)
    events: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)


@pytest.fixture(scope="session")
def rest() -> Iterator[NotionREST]:
    client = NotionREST(os.environ["NOTION_TOKEN"])
    yield client
    client.close()


@pytest.fixture(scope="session")
def runs(rest: NotionREST) -> Iterator[Runs]:
    """Build the workspace, run three snapshots, then tear everything down (always).

    - ``full_1``, ``full_2``: the public ``snapshot()`` with row bodies, into two empty databases;
    - ``filtered``: row bodies off and chosen items removed from search results, so the other
      discovery paths have to reach them.
    """
    root = os.environ["NOTION2PG_LIVE_ROOT_PAGE_ID"]
    admin_url = os.environ["NOTION2PG_TEST_ADMIN_URL"]
    plan = os.environ.get("NOTION2PG_LIVE_PLAN", "free")
    result = None
    try:
        workspace = build(rest, root)
        wait_for_search(rest, workspace)
        result = Runs(workspace)
        logger = logging.getLogger(LOGGER_NAME)
        logger.setLevel(logging.DEBUG)
        for label in ("full_1", "full_2", "filtered"):
            result.urls[label] = create_database(admin_url)
            capture = _Capture()
            logger.addHandler(capture)
            try:
                if label == "filtered":
                    hidden = [workspace[key] for key in ("synced_page", "hidden_home", "bymention_ds", "bylink_ds", "rowonly_ds")]
                    hidden += [workspace.ui["data_source"]] if "data_source" in workspace.ui else []
                    engine = create_target_engine(result.urls[label])
                    with NotionHTTP(os.environ["NOTION_TOKEN"], plan, Telemetry(f"live-{label}"),
                                    transport=SearchFilter(hidden)) as http:
                        run(engine, NotionAPI(http), row_bodies=False)
                    engine.dispose()
                else:
                    snapshot(os.environ["NOTION_TOKEN"], result.urls[label], plan=plan, row_bodies=True,
                             propagation_id=f"live-{label}")
            finally:
                logger.removeHandler(capture)
            result.events[label] = capture.events
        yield result
    finally:
        teardown(rest, root)
        if result is not None:
            for url in result.urls.values():
                drop_database(admin_url, url)
