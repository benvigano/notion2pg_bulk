"""Typed access to the Notion endpoints used by the snapshot, with checked pagination."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, List, Literal, Mapping, Optional

from .errors import SnapshotIntegrityError
from .notion_http import NotionHTTP

PAGE_SIZE = 100
Response = Dict[str, Any]


def paginate(fetch: Callable[[Optional[str]], Response]) -> Iterator[Response]:
    """Yield every response page of a cursor-paginated list.

    :param fetch: Sends one request given the cursor (``None`` for the first).
    :raises SnapshotIntegrityError: ``has_more`` without ``next_cursor``, or a
        repeated cursor.
    """
    cursor: Optional[str] = None
    seen = set()
    while True:
        response = fetch(cursor)
        yield response
        if not response.get("has_more"):
            return
        cursor = response.get("next_cursor")
        if not cursor:
            raise SnapshotIntegrityError("Paginated response has has_more=true but no next_cursor")
        if cursor in seen:
            raise SnapshotIntegrityError(f"Pagination cursor repeated: {cursor}")
        seen.add(cursor)


def _cursor_query(cursor: Optional[str], **extra: Any) -> Dict[str, Any]:
    query: Dict[str, Any] = {"page_size": PAGE_SIZE, **extra}
    if cursor is not None:
        query["start_cursor"] = cursor
    return query


class NotionAPI:
    """Endpoint wrappers over :class:`NotionHTTP`.

    :param http: The HTTP layer.
    """

    def __init__(self, http: NotionHTTP):
        self._http = http

    def search(self, object_type: Literal["page", "data_source"]) -> Iterator[Dict[str, Any]]:
        """Yield every search result of one object type."""
        def fetch(cursor: Optional[str]) -> Response:
            body = _cursor_query(cursor, filter={"property": "object", "value": object_type})
            return self._http.request("POST", "search", "/v1/search", body=body)

        for response in paginate(fetch):
            yield from response.get("results", [])

    def retrieve_data_source(self, data_source_id: str, missing_ok: bool = False) -> Optional[Response]:
        """:returns: The data source object (``None`` on an ignored 404, see ``missing_ok``)."""
        return self._http.request(
            "GET", f"data_sources/{data_source_id}", "/v1/data_sources/{data_source_id}", missing_ok=missing_ok
        )

    def retrieve_database(self, database_id: str, missing_ok: bool = False) -> Optional[Response]:
        """:returns: The database object (``None`` on an ignored 404)."""
        return self._http.request("GET", f"databases/{database_id}", "/v1/databases/{database_id}", missing_ok=missing_ok)

    def retrieve_page(self, page_id: str, missing_ok: bool = False) -> Optional[Response]:
        """:returns: The page object (``None`` on an ignored 404)."""
        return self._http.request("GET", f"pages/{page_id}", "/v1/pages/{page_id}", missing_ok=missing_ok)

    def retrieve_block(self, block_id: str, missing_ok: bool = False) -> Optional[Response]:
        """:returns: The block object (``None`` on an ignored 404)."""
        return self._http.request("GET", f"blocks/{block_id}", "/v1/blocks/{block_id}", missing_ok=missing_ok)

    def retrieve_view(self, view_id: str, missing_ok: bool = False) -> Optional[Response]:
        """:returns: The view object (``None`` on an ignored 404)."""
        return self._http.request("GET", f"views/{view_id}", "/v1/views/{view_id}", missing_ok=missing_ok)

    def query_data_source(self, data_source_id: str, body: Mapping[str, Any]) -> Iterator[Response]:
        """Yield every response page of one data source query.

        :param data_source_id: Data source to query.
        :param body: Query body (filter, sorts) without pagination fields.
        """
        def fetch(cursor: Optional[str]) -> Response:
            return self._http.request(
                "POST",
                f"data_sources/{data_source_id}/query",
                "/v1/data_sources/{data_source_id}/query",
                body=_cursor_query(cursor, **body),
                is_data_source_query=True,
            )

        yield from paginate(fetch)

    def list_views(
        self,
        *,
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        missing_ok: bool = False,
    ) -> Optional[List[str]]:
        """:returns: IDs of the views listed for a database or a data source
        (``None`` when ``missing_ok`` and the first page is a 404).
        """
        scope = {"database_id": database_id} if database_id is not None else {"data_source_id": data_source_id}
        first = self._http.request("GET", "views", "/v1/views", query=_cursor_query(None, **scope), missing_ok=missing_ok)
        if first is None:
            return None

        def fetch(cursor: Optional[str]) -> Response:
            if cursor is None:
                return first
            return self._http.request("GET", "views", "/v1/views", query=_cursor_query(cursor, **scope))

        return [ref["id"] for response in paginate(fetch) for ref in response.get("results", [])]

    def property_items(self, page_id: str, property_id: str) -> Iterator[Response]:
        """Yield every response page of one page property, from the first item.

        :param page_id: Page holding the property.
        :param property_id: Property ID exactly as returned by the API (already
            URL-encoded; not encoded again).
        """
        def fetch(cursor: Optional[str]) -> Response:
            return self._http.request(
                "GET",
                f"pages/{page_id}/properties/{property_id}",
                "/v1/pages/{page_id}/properties/{property_id}",
                query=_cursor_query(cursor),
            )

        yield from paginate(fetch)

    def block_children(self, block_id: str) -> Iterator[Dict[str, Any]]:
        """Yield every child block of a page or block, in order."""
        def fetch(cursor: Optional[str]) -> Response:
            return self._http.request(
                "GET", f"blocks/{block_id}/children", "/v1/blocks/{block_id}/children", query=_cursor_query(cursor)
            )

        for response in paginate(fetch):
            yield from response.get("results", [])
