"""Lossless mapping of Notion property types onto PostgreSQL columns and values."""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional

from sqlalchemy import BigInteger, Boolean, Numeric, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP, UUID
from sqlalchemy.types import TypeEngine

NO_COLUMN_TYPES = frozenset({"button", "files", "formula", "rollup"})
"""Property types that get no column (see README → Not captured)."""

PAGINATED_TYPES = frozenset({"title", "rich_text", "people", "relation"})
"""Property types whose page-response values can be truncated at 25 items."""

_UUID = UUID(as_uuid=False)


def json_type() -> JSONB:
    """:returns: A JSONB type that stores Python ``None`` as SQL ``NULL``."""
    return JSONB(none_as_null=True)


COLUMN_TYPES: Mapping[str, TypeEngine] = {
    "title": json_type(),
    "rich_text": json_type(),
    "number": Numeric(),
    "checkbox": Boolean(),
    "url": Text(),
    "email": Text(),
    "phone_number": Text(),
    "select": Text(),
    "status": Text(),
    "multi_select": ARRAY(Text()),
    "date": json_type(),
    "place": json_type(),
    "verification": json_type(),
    "relation": ARRAY(_UUID),
    "people": ARRAY(_UUID),
    "unique_id": BigInteger(),
    "created_time": TIMESTAMP(timezone=True),
    "last_edited_time": TIMESTAMP(timezone=True),
    "created_by": _UUID,
    "last_edited_by": _UUID,
}

FALLBACK_COLUMN_TYPE: TypeEngine = json_type()


def column_type(property_type: str) -> Optional[TypeEngine]:
    """Column type for a property type.

    :param property_type: Notion property ``type``.
    :returns: The column type (JSONB for unknown types), or ``None`` for types
        that get no column.
    """
    if property_type in NO_COLUMN_TYPES:
        return None
    return COLUMN_TYPES.get(property_type, FALLBACK_COLUMN_TYPE)


def _non_empty(value: Any) -> Any:
    if value is None or value == [] or value == "":
        return None
    return value


def _ids(items: Any) -> Any:
    return [item["id"] for item in items] if items else None


def _id(item: Any) -> Any:
    return item["id"] if item else None


_EXTRACTORS: Dict[str, Callable[[Any], Any]] = {
    "title": _non_empty,
    "rich_text": _non_empty,
    "number": lambda value: value,
    "checkbox": lambda value: value,
    "url": _non_empty,
    "email": _non_empty,
    "phone_number": _non_empty,
    "select": _id,
    "status": _id,
    "multi_select": _ids,
    "date": lambda value: value,
    "place": lambda value: value,
    "verification": lambda value: value,
    "relation": _ids,
    "people": _ids,
    "unique_id": lambda value: value["number"] if value else None,
    "created_time": lambda value: value,
    "last_edited_time": lambda value: value,
    "created_by": _id,
    "last_edited_by": _id,
}


def column_value(property_value: Mapping[str, Any]) -> Any:
    """Column value of one page property value; ``None`` means empty.

    :param property_value: Entry of a page's ``properties`` object
        (``{"id", "type", <type>: value}``), with any paginated value complete.
    :returns: The value to store.
    """
    property_type = property_value["type"]
    raw = property_value.get(property_type)
    extractor = _EXTRACTORS.get(property_type, _non_empty)
    return extractor(raw)
