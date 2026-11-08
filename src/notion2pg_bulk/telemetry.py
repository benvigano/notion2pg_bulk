"""Structured telemetry for the resiliency mechanisms.

Every event is a frozen dataclass whose serialized form validates against
``telemetry.schema.json`` (shipped in the package). Events are emitted
synchronously, one JSON object per event, through the ``notion2pg_bulk``
logger.
"""

from __future__ import annotations

import contextvars
import dataclasses
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import resources
from typing import Any, Dict, Iterator, Literal, Optional, Union

LOGGER_NAME = "notion2pg_bulk"
MAX_STRING_LENGTH = 512

Phase = Literal["discovery", "schema", "rows", "views", "bodies"]
Fault = Literal["rate_limited", "service_overload", "server_error", "transport"]

logger = logging.getLogger(LOGGER_NAME)


def truncate(value: str, limit: int = MAX_STRING_LENGTH) -> str:
    """Cut ``value`` deterministically to at most ``limit`` characters.

    The kept prefix is followed by ``…[+N]``, where ``N`` is the number of
    characters removed.

    :param value: String to bound.
    :param limit: Maximum length of the result.
    :returns: ``value`` itself, or its truncated form.
    """
    if len(value) <= limit:
        return value
    removed = len(value) - limit
    while True:
        suffix = f"…[+{removed}]"
        keep = limit - len(suffix)
        if len(value) - keep == removed:
            return value[:keep] + suffix
        removed = len(value) - keep


@dataclass(frozen=True)
class Scope:
    """Where the snapshot currently is: phase and entity being processed."""

    phase: Optional[Phase] = None
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None


_scope: contextvars.ContextVar[Scope] = contextvars.ContextVar("notion2pg_bulk_scope", default=Scope())


@contextmanager
def scope(
    phase: Optional[Phase] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> Iterator[None]:
    """Set the active scope; unset fields are inherited from the enclosing scope.

    :param phase: Snapshot phase.
    :param entity_type: Kind of entity being processed (e.g. ``data_source``).
    :param entity_id: Notion ID of that entity.
    """
    outer = _scope.get()
    inner = Scope(
        phase=phase or outer.phase,
        entity_type=entity_type if entity_type is not None else outer.entity_type,
        entity_id=entity_id if entity_type is not None else outer.entity_id,
    )
    token = _scope.set(inner)
    try:
        yield
    finally:
        _scope.reset(token)


def current_scope() -> Scope:
    """:returns: The active :class:`Scope`."""
    return _scope.get()


@dataclass(frozen=True)
class Locus:
    """Active locus state of a telemetry event."""

    phase: Optional[Phase]
    entity_type: Optional[str]
    entity_id: Optional[str]
    method: Optional[str]
    endpoint: Optional[str]


@dataclass(frozen=True)
class Anomaly:
    """Scalar properties of the fault."""

    http_status: Optional[int] = None
    code: Optional[str] = None
    rate_limit_reason: Optional[str] = None
    request_id: Optional[str] = None
    exception_type: Optional[str] = None


@dataclass(frozen=True)
class Context:
    """Upstream contextual payload: the Notion error message only."""

    message: Optional[str] = None


@dataclass(frozen=True)
class _Event:
    ts: str
    propagation_id: str
    locus: Locus

    def to_dict(self) -> Dict[str, Any]:
        """:returns: The event as a JSON-compatible dict, strings truncated."""
        return _bounded(dataclasses.asdict(self))


@dataclass(frozen=True)
class ThrottleWait(_Event):
    """The limiter blocked a request before sending it."""

    limiter: Literal["connection", "data_source_query"]
    wait_s: float
    event: Literal["throttle_wait"] = field(default="throttle_wait")


@dataclass(frozen=True)
class RetryScheduled(_Event):
    """A mapped fault occurred and the request will be retried."""

    fault: Fault
    attempt: int
    max_attempts: int
    delay_s: float
    delay_source: Literal["retry_after", "backoff"]
    anomaly: Anomaly
    context: Context
    event: Literal["retry_scheduled"] = field(default="retry_scheduled")


@dataclass(frozen=True)
class RetriesExhausted(_Event):
    """A mapped fault will not be retried any further and is about to propagate."""

    fault: Fault
    attempts: int
    max_attempts: int
    reason: Literal["max_attempts", "wait_exceeds_limit"]
    anomaly: Anomaly
    context: Context
    event: Literal["retries_exhausted"] = field(default="retries_exhausted")


@dataclass(frozen=True)
class RequestFailed(_Event):
    """A fault outside the retry set occurred and is about to propagate."""

    attempt: int
    anomaly: Anomaly
    context: Context
    event: Literal["request_failed"] = field(default="request_failed")


TelemetryEvent = Union[ThrottleWait, RetryScheduled, RetriesExhausted, RequestFailed]

_LEVELS = {
    "throttle_wait": logging.INFO,
    "retry_scheduled": logging.WARNING,
    "retries_exhausted": logging.ERROR,
    "request_failed": logging.ERROR,
}


def _bounded(value: Any) -> Any:
    if isinstance(value, str):
        return truncate(value)
    if isinstance(value, dict):
        return {key: _bounded(item) for key, item in value.items()}
    return value


def utc_now() -> str:
    """:returns: The current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Telemetry:
    """Builds and emits events carrying one propagation identifier.

    :param propagation_id: Identifier carried by every event.
    """

    def __init__(self, propagation_id: str):
        self.propagation_id = propagation_id

    def locus(self, method: Optional[str], endpoint: Optional[str]) -> Locus:
        """:returns: The :class:`Locus` for a request issued in the active scope."""
        active = current_scope()
        return Locus(
            phase=active.phase,
            entity_type=active.entity_type,
            entity_id=active.entity_id,
            method=method,
            endpoint=endpoint,
        )

    def emit(self, event: TelemetryEvent) -> None:
        """Serialize ``event`` and log it synchronously.

        :param event: Event to emit.
        """
        payload = event.to_dict()
        logger.log(_LEVELS[payload["event"]], json.dumps(payload), extra={"telemetry": payload})


def load_schema() -> Dict[str, Any]:
    """:returns: The published JSON Schema of telemetry events."""
    text = resources.files(__package__).joinpath("telemetry.schema.json").read_text(encoding="utf-8")
    return json.loads(text)


class JSONFormatter(logging.Formatter):
    """Formats telemetry records as their JSON payload, other records as JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "telemetry", None)
        if payload is not None:
            return json.dumps(payload)
        return json.dumps(
            {
                "event": "log",
                "ts": utc_now(),
                "level": record.levelname,
                "message": truncate(record.getMessage()),
            }
        )
