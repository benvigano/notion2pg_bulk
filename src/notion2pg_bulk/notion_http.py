"""Notion HTTP layer: timeouts, request limiter and the per-request retry."""

from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Literal, Mapping, Optional, Tuple, Union

import httpx

from . import json_codec
from .errors import NotionAPIError, RetriesExhaustedError
from .telemetry import (
    Anomaly,
    Context,
    Fault,
    Locus,
    RequestFailed,
    RetriesExhausted,
    RetryScheduled,
    Telemetry,
    ThrottleWait,
    utc_now,
)

NOTION_VERSION = "2026-03-11"
BASE_URL = "https://api.notion.com/v1/"
USER_AGENT = "notion2pg-bulk"

Plan = Literal["free", "plus", "business", "enterprise"]
PLAN_BUDGETS: Mapping[str, int] = {"free": 180, "plus": 180, "business": 600, "enterprise": 600}

WINDOW_S = 60.0
DATA_SOURCE_QUERY_INTERVAL_S = 1.0 / 3.0
MAX_RETRIES = 5
MAX_ATTEMPTS = MAX_RETRIES + 1
BACKOFF_BASE_S = 1.0
MAX_JITTER = 0.25
MAX_REQUESTED_WAIT_S = 300.0

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

TRANSPORT_FAULTS: Tuple[type, ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
)
SERVER_ERROR_STATUSES = frozenset({500, 502, 503, 504})
BLOCKED_RATE_LIMIT_REASON = "public_api_request_blocked"

Clock = Callable[[], float]
Sleep = Callable[[float], None]


class RateLimiter:
    """Blocks before each request so that the request budget is never exceeded.

    :param budget: Requests allowed per 60 s window.
    :param telemetry: Emitter for ``throttle_wait`` events.
    :param clock: Monotonic clock, in seconds.
    :param sleep: Blocking sleep, in seconds.
    """

    def __init__(self, budget: int, telemetry: Telemetry, clock: Clock = time.monotonic, sleep: Sleep = time.sleep):
        self._budget = budget
        self._telemetry = telemetry
        self._clock = clock
        self._sleep = sleep
        self._sent: Deque[float] = deque(maxlen=budget)
        self._last_query: Optional[float] = None

    def acquire(self, is_data_source_query: bool, locus: Locus) -> None:
        """Wait until a request may be sent, then record it as sent.

        :param is_data_source_query: Whether the request is a data source query.
        :param locus: Locus reported in ``throttle_wait`` events.
        """
        while True:
            now = self._clock()
            if is_data_source_query and self._last_query is not None:
                wait = self._last_query + DATA_SOURCE_QUERY_INTERVAL_S - now
                if wait > 0:
                    self._wait("data_source_query", wait, locus)
                    continue
            # Notion's 60 s windows have an unknown phase: a sliding window keeps every
            # 60 s interval, whatever its alignment, within the budget.
            if len(self._sent) == self._budget:
                wait = self._sent[0] + WINDOW_S - now
                if wait > 0:
                    self._wait("connection", wait, locus)
                    continue
            self._sent.append(now)
            if is_data_source_query:
                self._last_query = now
            return

    def _wait(self, limiter: Literal["connection", "data_source_query"], wait_s: float, locus: Locus) -> None:
        self._telemetry.emit(
            ThrottleWait(
                ts=utc_now(),
                propagation_id=self._telemetry.propagation_id,
                locus=locus,
                limiter=limiter,
                wait_s=wait_s,
            )
        )
        self._sleep(wait_s)


def _api_error(response: httpx.Response) -> NotionAPIError:
    body: Dict[str, Any] = {}
    if response.headers.get("content-type", "").startswith("application/json"):
        parsed = json_codec.loads(response.content)
        if isinstance(parsed, dict):
            body = parsed
    additional = body.get("additional_data") if isinstance(body.get("additional_data"), dict) else {}
    return NotionAPIError(
        status=response.status_code,
        code=body.get("code"),
        request_id=body.get("request_id"),
        message=body.get("message"),
        rate_limit_reason=additional.get("rate_limit_reason"),
        retry_after=additional.get("retry_after"),
    )


def classify(error: NotionAPIError) -> Optional[Fault]:
    """Map a Notion error onto the closed set of retried faults.

    :param error: Error returned by Notion.
    :returns: The fault name, or ``None`` when the error must propagate.
    """
    if error.status == 429:
        if error.code == "rate_limited" and error.rate_limit_reason != BLOCKED_RATE_LIMIT_REASON:
            return "rate_limited"
        return None
    if error.status == 529:
        return "service_overload"
    if error.status in SERVER_ERROR_STATUSES:
        return "server_error"
    return None


def _anomaly(fault: BaseException) -> Anomaly:
    if isinstance(fault, NotionAPIError):
        return Anomaly(
            http_status=fault.status,
            code=fault.code,
            rate_limit_reason=fault.rate_limit_reason,
            request_id=fault.request_id,
            exception_type=type(fault).__name__,
        )
    return Anomaly(exception_type=type(fault).__name__)


def _context(fault: BaseException) -> Context:
    return Context(message=fault.message if isinstance(fault, NotionAPIError) else None)


@dataclass(frozen=True)
class _RetryableFault:
    fault: Fault
    error: BaseException
    retry_after_header: Optional[float]


class NotionHTTP:
    """Sends Notion API requests through the limiter and the retry policy.

    :param token: Notion integration token.
    :param plan: Workspace plan; selects the request budget.
    :param telemetry: Telemetry emitter.
    :param transport: Optional ``httpx`` transport (used by tests).
    :param clock: Monotonic clock, in seconds.
    :param sleep: Blocking sleep, in seconds.
    :param rng: Random source for backoff jitter.
    """

    def __init__(
        self,
        token: str,
        plan: Plan,
        telemetry: Telemetry,
        transport: Optional[httpx.BaseTransport] = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
        rng: Optional[random.Random] = None,
    ):
        if plan not in PLAN_BUDGETS:
            raise ValueError(f"Unknown plan {plan!r}; expected one of {sorted(PLAN_BUDGETS)}")
        self._telemetry = telemetry
        self._sleep = sleep
        self._rng = rng or random.Random()
        self.limiter = RateLimiter(PLAN_BUDGETS[plan], telemetry, clock=clock, sleep=sleep)
        self._client = httpx.Client(
            base_url=BASE_URL,
            timeout=TIMEOUT,
            transport=transport,
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": NOTION_VERSION,
                "User-Agent": USER_AGENT,
            },
        )

    def close(self) -> None:
        """Close the underlying connection pool."""
        self._client.close()

    def __enter__(self) -> "NotionHTTP":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        endpoint: str,
        query: Optional[Mapping[str, Any]] = None,
        body: Optional[Mapping[str, Any]] = None,
        is_data_source_query: bool = False,
    ) -> Dict[str, Any]:
        """Send one request, retrying only the mapped faults.

        :param method: HTTP method.
        :param path: Path relative to ``/v1/``, with IDs substituted.
        :param endpoint: Path template reported in telemetry (no IDs).
        :param query: Query-string parameters.
        :param body: JSON body.
        :param is_data_source_query: Whether the data source query limit applies.
        :returns: The decoded JSON response.
        :raises NotionAPIError: Notion answered with an unmapped error.
        :raises RetriesExhaustedError: A mapped fault outlasted the retry policy.
        """
        locus = self._telemetry.locus(method, endpoint)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.limiter.acquire(is_data_source_query, locus)
            try:
                outcome = self._attempt(method, path, query, body)
            except Exception as exc:
                self._emit_failed(exc, attempt, locus)
                raise
            if isinstance(outcome, dict):
                return outcome
            fault, error = outcome.fault, outcome.error
            delay, source = self._delay(outcome, attempt)
            if delay > MAX_REQUESTED_WAIT_S or attempt == MAX_ATTEMPTS:
                reason: Literal["max_attempts", "wait_exceeds_limit"] = (
                    "wait_exceeds_limit" if delay > MAX_REQUESTED_WAIT_S else "max_attempts"
                )
                self._telemetry.emit(
                    RetriesExhausted(
                        ts=utc_now(),
                        propagation_id=self._telemetry.propagation_id,
                        locus=locus,
                        fault=fault,
                        attempts=attempt,
                        max_attempts=MAX_ATTEMPTS,
                        reason=reason,
                        anomaly=_anomaly(error),
                        context=_context(error),
                    )
                )
                raise RetriesExhaustedError(error, attempt, reason) from error
            self._telemetry.emit(
                RetryScheduled(
                    ts=utc_now(),
                    propagation_id=self._telemetry.propagation_id,
                    locus=locus,
                    fault=fault,
                    attempt=attempt,
                    max_attempts=MAX_ATTEMPTS,
                    delay_s=delay,
                    delay_source=source,
                    anomaly=_anomaly(error),
                    context=_context(error),
                )
            )
            self._sleep(delay)
        raise AssertionError("unreachable")

    def _attempt(
        self,
        method: str,
        path: str,
        query: Optional[Mapping[str, Any]],
        body: Optional[Mapping[str, Any]],
    ) -> Union[Dict[str, Any], "_RetryableFault"]:
        """Send once.

        :returns: The decoded body on success, or the mapped fault.
        :raises NotionAPIError: Notion answered with an unmapped error.
        """
        try:
            response = self._client.request(method, path, params=query, json=body)
        except TRANSPORT_FAULTS as exc:
            return _RetryableFault("transport", exc, None)
        if response.is_success:
            return json_codec.loads(response.content)
        error = _api_error(response)
        fault = classify(error)
        if fault is None:
            raise error
        header = response.headers.get("retry-after")
        return _RetryableFault(fault, error, float(header) if header is not None else None)

    def _delay(self, outcome: "_RetryableFault", attempt: int) -> Tuple[float, Literal["retry_after", "backoff"]]:
        if outcome.retry_after_header is not None:
            return outcome.retry_after_header, "retry_after"
        error = outcome.error
        if isinstance(error, NotionAPIError) and error.retry_after is not None:
            return float(error.retry_after), "retry_after"
        jitter = 1.0 + self._rng.uniform(0.0, MAX_JITTER)
        return BACKOFF_BASE_S * (2 ** (attempt - 1)) * jitter, "backoff"

    def _emit_failed(self, exc: BaseException, attempt: int, locus: Locus) -> None:
        self._telemetry.emit(
            RequestFailed(
                ts=utc_now(),
                propagation_id=self._telemetry.propagation_id,
                locus=locus,
                attempt=attempt,
                anomaly=_anomaly(exc),
                context=_context(exc),
            )
        )
