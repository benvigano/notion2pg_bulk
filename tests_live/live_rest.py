"""Minimal Notion REST client used to build, inspect and tear down the live test workspace.

The token is read from ``NOTION_TOKEN`` and never printed or stored.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Mapping, Optional

import httpx

API = "https://api.notion.com/v1/"
VERSION = "2026-03-11"
MAX_ATTEMPTS = 6
RETRIED_STATUSES = frozenset({429, 500, 502, 503, 504, 529})


class NotionRESTError(Exception):
    """Notion answered with an error status."""

    def __init__(self, status: int, body: Mapping[str, Any]):
        self.status = status
        self.code = body.get("code")
        self.message = body.get("message")
        super().__init__(f"{status} {self.code}: {self.message}")


class NotionREST:
    """Blocking client with bounded retries on rate limits and server errors (test tooling only)."""

    def __init__(self, token: str):
        self._client = httpx.Client(
            base_url=API,
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0),
            headers={"Authorization": f"Bearer {token}", "Notion-Version": VERSION},
        )

    def close(self) -> None:
        self._client.close()

    def call(self, method: str, path: str, body: Optional[Mapping[str, Any]] = None,
             query: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        for attempt in range(MAX_ATTEMPTS):
            response = self._client.request(method, path, json=body, params=query)
            time.sleep(0.35)
            if response.is_success:
                return response.json()
            if response.status_code in RETRIED_STATUSES and attempt < MAX_ATTEMPTS - 1:
                time.sleep(min(float(response.headers.get("retry-after") or 2 ** attempt), 60.0))
                continue
            raise NotionRESTError(response.status_code, response.json() if response.content else {})
        raise AssertionError("unreachable")

    def status(self, method: str, path: str, body: Optional[Mapping[str, Any]] = None,
               query: Optional[Mapping[str, Any]] = None):
        """:returns: ``(status, body)`` without raising on error statuses."""
        try:
            return 200, self.call(method, path, body, query)
        except NotionRESTError as error:
            return error.status, {"code": error.code}

    def children(self, block_id: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            query = {"page_size": 100, **({"start_cursor": cursor} if cursor else {})}
            response = self.call("GET", f"blocks/{block_id}/children", query=query)
            out += response["results"]
            if not response["has_more"]:
                return out
            cursor = response["next_cursor"]

    def search(self, object_type: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cursor = None
        while True:
            body = {"filter": {"property": "object", "value": object_type}, "page_size": 100,
                    **({"start_cursor": cursor} if cursor else {})}
            response = self.call("POST", "search", body)
            out += response["results"]
            if not response["has_more"]:
                return out
            cursor = response["next_cursor"]


def token() -> str:
    return os.environ["NOTION_TOKEN"]


def text(content: str) -> Dict[str, Any]:
    return {"type": "text", "text": {"content": content}}


def plain(rich: Any) -> str:
    return "".join(item.get("plain_text", "") for item in rich or [])


def mention(kind: str, target: str) -> Dict[str, Any]:
    return {"type": "mention", "mention": {"type": kind, kind: {"id": target}}}


def paragraph(rich: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"type": "paragraph", "paragraph": {"rich_text": rich}}
