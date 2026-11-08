"""JSON codec that keeps Notion numbers exact (``decimal.Decimal``, never ``float``)."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Union


def loads(data: Union[str, bytes]) -> Any:
    """Decode JSON, reading non-integer numbers as :class:`~decimal.Decimal`."""
    return json.loads(data, parse_float=Decimal)


def dumps(value: Any) -> str:
    """Encode JSON, writing :class:`~decimal.Decimal` values as JSON numbers."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return "{" + ",".join(f"{json.dumps(str(key))}:{dumps(item)}" for key, item in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(dumps(item) for item in value) + "]"
    return json.dumps(value)
