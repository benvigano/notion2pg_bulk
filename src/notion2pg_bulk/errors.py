"""Exception hierarchy raised by :mod:`notion2pg_bulk`."""

from __future__ import annotations

from typing import Optional, Sequence


class Notion2PgError(Exception):
    """Base class of every error raised by the package."""


class TargetNotEmptyError(Notion2PgError):
    """The target PostgreSQL database already contains user objects.

    :param objects: Human-readable descriptions of the objects found.
    """

    def __init__(self, objects: Sequence[str]):
        self.objects = tuple(objects)
        super().__init__("Target database is not empty: " + ", ".join(self.objects))


class NotionAPIError(Notion2PgError):
    """Notion answered with an error status.

    :param status: HTTP status code.
    :param code: Notion error ``code``, if the body carried one.
    :param request_id: Notion ``request_id``, if the body carried one.
    :param message: Notion error ``message``, if the body carried one.
    :param rate_limit_reason: ``additional_data.rate_limit_reason`` of a 429.
    :param retry_after: ``additional_data.retry_after`` (seconds, as returned).
    """

    def __init__(
        self,
        status: int,
        code: Optional[str],
        request_id: Optional[str],
        message: Optional[str],
        rate_limit_reason: Optional[str] = None,
        retry_after: Optional[str] = None,
    ):
        self.status = status
        self.code = code
        self.request_id = request_id
        self.message = message
        self.rate_limit_reason = rate_limit_reason
        self.retry_after = retry_after
        super().__init__(f"Notion API error {status} {code}: {message} (request_id={request_id})")


class RetriesExhaustedError(Notion2PgError):
    """A retryable fault persisted beyond the retry policy.

    :param last_fault: The last fault observed (a :class:`NotionAPIError` or a
        transport exception).
    :param attempts: Number of attempts made.
    :param reason: ``max_attempts`` or ``wait_exceeds_limit``.
    """

    def __init__(self, last_fault: BaseException, attempts: int, reason: str):
        self.last_fault = last_fault
        self.attempts = attempts
        self.reason = reason
        super().__init__(f"Retries exhausted after {attempts} attempt(s) ({reason}): {last_fault!r}")


class SnapshotIntegrityError(Notion2PgError):
    """A completeness or consistency guarantee of the snapshot cannot be met."""
