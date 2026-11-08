"""HTTP layer: limiter, retry policy, timeouts and telemetry."""

from __future__ import annotations

import json
import random
from typing import Callable, Iterator, List, Optional

import httpx
import pytest

from notion2pg_bulk.errors import NotionAPIError, RetriesExhaustedError, SnapshotIntegrityError
from notion2pg_bulk.notion_api import paginate
from notion2pg_bulk.notion_http import TIMEOUT, NotionHTTP
from notion2pg_bulk.telemetry import Telemetry, scope, truncate

TOKEN = "secret_token_value"


def error_response(status: int, code: Optional[str], *, reason: Optional[str] = None,
                   retry_after_body: Optional[str] = None, retry_after_header: Optional[str] = None,
                   message: str = "boom") -> httpx.Response:
    body = {"object": "error", "status": status, "code": code, "message": message, "request_id": "req-1"}
    additional = {}
    if reason is not None:
        additional["rate_limit_reason"] = reason
    if retry_after_body is not None:
        additional["retry_after"] = retry_after_body
    if additional:
        body["additional_data"] = additional
    headers = {"retry-after": retry_after_header} if retry_after_header is not None else {}
    return httpx.Response(status, json=body, headers=headers)


def scripted(responses: List[Callable[[httpx.Request], httpx.Response]]):
    calls: List[httpx.Request] = []
    iterator: Iterator = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return next(iterator)(request)

    return handler, calls


def ok(body=None):
    return lambda request: httpx.Response(200, json=body if body is not None else {"object": "page", "id": "x"})


def respond(response: httpx.Response):
    return lambda request: response


def raise_(exc_type):
    def handler(request):
        raise exc_type("transport failure", request=request)
    return handler


def make_http(handler, clock, plan="free", seed=7) -> NotionHTTP:
    return NotionHTTP(
        TOKEN, plan, Telemetry("prop-1"), transport=httpx.MockTransport(handler),
        clock=clock, sleep=clock.sleep, rng=random.Random(seed),
    )


def test_timeouts_are_explicit():
    assert (TIMEOUT.connect, TIMEOUT.read, TIMEOUT.write, TIMEOUT.pool) == (10.0, 60.0, 10.0, 10.0)


def test_unknown_plan_is_rejected(clock):
    with pytest.raises(ValueError):
        make_http(lambda r: httpx.Response(200, json={}), clock, plan="team")


def test_request_sends_version_and_auth(clock):
    handler, calls = scripted([ok()])
    make_http(handler, clock).request("GET", "pages/p1", "/v1/pages/{page_id}")
    assert calls[0].headers["Notion-Version"] == "2026-03-11"
    assert calls[0].headers["Authorization"] == f"Bearer {TOKEN}"


# ---------------------------------------------------------------- limiter

@pytest.mark.parametrize("plan,budget", [("free", 180), ("plus", 180), ("business", 600), ("enterprise", 600)])
def test_connection_budget_never_exceeded(clock, telemetry_events, plan, budget):
    sent: List[float] = []

    def handler(request):
        sent.append(clock.now)
        return httpx.Response(200, json={})

    http = make_http(handler, clock, plan=plan)
    for _ in range(budget * 2 + 5):
        http.request("GET", "pages/p", "/v1/pages/{page_id}")
    for index in range(len(sent)):
        window = [t for t in sent if sent[index] <= t < sent[index] + 60.0]
        assert len(window) <= budget
    waits = [e for e in telemetry_events.events if e["event"] == "throttle_wait"]
    assert waits and all(e["limiter"] == "connection" and e["wait_s"] > 0 for e in waits)


def test_data_source_queries_are_spaced(clock, telemetry_events):
    sent: List[float] = []

    def handler(request):
        sent.append(clock.now)
        return httpx.Response(200, json={})

    http = make_http(handler, clock, plan="enterprise")
    for _ in range(4):
        http.request("POST", "data_sources/d/query", "/v1/data_sources/{data_source_id}/query",
                     body={}, is_data_source_query=True)
    gaps = [b - a for a, b in zip(sent, sent[1:])]
    assert all(gap >= 1 / 3 - 1e-9 for gap in gaps)
    assert {e["limiter"] for e in telemetry_events.events} == {"data_source_query"}


def test_no_throttle_event_without_wait(clock, telemetry_events):
    http = make_http(lambda r: httpx.Response(200, json={}), clock)
    http.request("GET", "pages/p", "/v1/pages/{page_id}")
    http.request("POST", "data_sources/d/query", "/v1/data_sources/{data_source_id}/query",
                 body={}, is_data_source_query=True)
    assert telemetry_events.events == []


# ---------------------------------------------------------------- retry

MAPPED = [
    ("rate_limited", lambda: error_response(429, "rate_limited", reason="public_api_request_rate_limit", retry_after_header="7")),
    ("rate_limited", lambda: error_response(429, "rate_limited", reason="public_api_space_request_rate_limit", retry_after_body="12")),
    ("rate_limited", lambda: error_response(429, "rate_limited", reason="some_new_reason")),
    ("service_overload", lambda: error_response(529, "service_overload")),
    ("server_error", lambda: error_response(500, "internal_server_error")),
    ("server_error", lambda: httpx.Response(502, text="<html>bad gateway</html>", headers={"content-type": "text/html"})),
    ("server_error", lambda: error_response(503, "service_unavailable")),
    ("server_error", lambda: error_response(504, "gateway_timeout")),
]


@pytest.mark.parametrize("fault,make", MAPPED)
def test_mapped_status_faults_are_retried(clock, telemetry_events, fault, make):
    handler, calls = scripted([respond(make()), ok({"object": "page", "id": "done"})])
    with scope(phase="rows", entity_type="data_source", entity_id="ds-1"):
        result = make_http(handler, clock).request("POST", "search", "/v1/search", body={})
    assert result == {"object": "page", "id": "done"}
    assert len(calls) == 2
    [event] = telemetry_events.events
    assert event["event"] == "retry_scheduled"
    assert event["fault"] == fault
    assert event["attempt"] == 1 and event["max_attempts"] == 6
    assert event["locus"] == {"phase": "rows", "entity_type": "data_source", "entity_id": "ds-1",
                              "method": "POST", "endpoint": "/v1/search"}
    assert event["propagation_id"] == "prop-1"


def test_retry_after_header_wins_over_body(clock, telemetry_events):
    response = error_response(429, "rate_limited", retry_after_header="3", retry_after_body="9")
    handler, _ = scripted([respond(response), ok()])
    make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    event = telemetry_events.events[0]
    assert (event["delay_s"], event["delay_source"]) == (3.0, "retry_after")
    assert clock.sleeps == [3.0]


def test_retry_after_body_used_without_header(clock, telemetry_events):
    handler, _ = scripted([respond(error_response(429, "rate_limited", retry_after_body="90")), ok()])
    make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert telemetry_events.events[0]["delay_s"] == 90.0
    assert telemetry_events.events[0]["delay_source"] == "retry_after"


@pytest.mark.parametrize("exc_type", [
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout,
    httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ReadError,
])
def test_transport_faults_are_retried(clock, telemetry_events, exc_type):
    handler, calls = scripted([raise_(exc_type), ok()])
    make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    [event] = telemetry_events.events
    assert event["fault"] == "transport"
    assert event["anomaly"]["exception_type"] == exc_type.__name__
    assert event["anomaly"]["http_status"] is None
    assert event["delay_source"] == "backoff"


def test_backoff_schedule_and_exhaustion(clock, telemetry_events):
    handler, calls = scripted([respond(error_response(500, "internal_server_error"))] * 6)
    with pytest.raises(RetriesExhaustedError) as caught:
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert len(calls) == 6
    assert caught.value.attempts == 6 and caught.value.reason == "max_attempts"
    assert isinstance(caught.value.last_fault, NotionAPIError)
    assert len(clock.sleeps) == 5
    for base, slept in zip([1, 2, 4, 8, 16], clock.sleeps):
        assert base <= slept <= base * 1.25
    assert telemetry_events.names() == ["retry_scheduled"] * 5 + ["retries_exhausted"]
    final = telemetry_events.events[-1]
    assert final["attempts"] == 6 and final["reason"] == "max_attempts"
    assert final["anomaly"]["http_status"] == 500 and final["anomaly"]["request_id"] == "req-1"


def test_requested_wait_over_limit_raises_immediately(clock, telemetry_events):
    handler, calls = scripted([respond(error_response(429, "rate_limited", retry_after_header="301"))])
    with pytest.raises(RetriesExhaustedError) as caught:
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert caught.value.reason == "wait_exceeds_limit"
    assert len(calls) == 1 and clock.sleeps == []
    assert telemetry_events.names() == ["retries_exhausted"]


@pytest.mark.parametrize("response", [
    error_response(400, "validation_error"),
    error_response(401, "unauthorized"),
    error_response(403, "restricted_resource"),
    error_response(404, "object_not_found"),
    error_response(409, "conflict_error"),
    error_response(429, "rate_limited", reason="public_api_request_blocked", retry_after_header="1"),
])
def test_unmapped_status_propagates(clock, telemetry_events, response):
    handler, calls = scripted([respond(response)])
    with pytest.raises(NotionAPIError) as caught:
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert len(calls) == 1
    assert caught.value.status == response.status_code and caught.value.request_id == "req-1"
    [event] = telemetry_events.events
    assert event["event"] == "request_failed"
    assert event["anomaly"]["http_status"] == response.status_code
    assert event["context"]["message"] == "boom"


def test_json_decode_error_propagates(clock, telemetry_events):
    handler, _ = scripted([respond(httpx.Response(200, text="not json"))])
    with pytest.raises(json.JSONDecodeError):
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert telemetry_events.events[0]["anomaly"]["exception_type"] == "JSONDecodeError"


def test_unlisted_transport_exception_propagates(clock, telemetry_events):
    handler, calls = scripted([raise_(httpx.LocalProtocolError)])
    with pytest.raises(httpx.LocalProtocolError):
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert len(calls) == 1
    assert telemetry_events.names() == ["request_failed"]


def test_events_never_contain_token_or_bodies(clock, telemetry_events):
    response = error_response(500, "internal_server_error", message="x" * 2000)
    handler, _ = scripted([respond(response), ok()])
    make_http(handler, clock).request("POST", "search", "/v1/search", body={"query": "private"})
    serialized = json.dumps(telemetry_events.events)
    assert TOKEN not in serialized and "private" not in serialized
    message = telemetry_events.events[0]["context"]["message"]
    assert len(message) <= 512 and message.endswith(f"…[+{2000 - message.index('…')}]")


# ---------------------------------------------------------------- truncation and pagination

@pytest.mark.parametrize("length", [0, 511, 512, 513, 600, 10_000, 1_000_000])
def test_truncation_is_bounded_and_deterministic(length):
    value = "a" * length
    result = truncate(value)
    assert len(result) <= 512
    assert result == truncate(value)
    if length <= 512:
        assert result == value
    else:
        kept = result.index("…")
        assert result == value[:kept] + f"…[+{length - kept}]"


def test_pagination_rejects_missing_cursor():
    with pytest.raises(SnapshotIntegrityError):
        list(paginate(lambda cursor: {"results": [], "has_more": True, "next_cursor": None}))


def test_pagination_rejects_repeated_cursor():
    with pytest.raises(SnapshotIntegrityError):
        list(paginate(lambda cursor: {"results": [], "has_more": True, "next_cursor": "same"}))


def test_every_event_carries_its_level(clock, telemetry_events):
    import logging

    records = []

    class Spy(logging.Handler):
        def emit(self, record):
            records.append((record.levelno, record.telemetry["level"], record.telemetry["event"]))

    spy = Spy()
    logging.getLogger("notion2pg_bulk").addHandler(spy)
    try:
        handler, _ = scripted([respond(error_response(500, "internal_server_error"))] * 6
                              + [respond(error_response(400, "validation_error"))])
        http = make_http(handler, clock)
        with pytest.raises(RetriesExhaustedError):
            http.request("POST", "data_sources/d/query", "/v1/data_sources/{data_source_id}/query", body={},
                         is_data_source_query=True)
        with pytest.raises(NotionAPIError):
            http.request("POST", "data_sources/d/query", "/v1/data_sources/{data_source_id}/query", body={},
                         is_data_source_query=True)
    finally:
        logging.getLogger("notion2pg_bulk").removeHandler(spy)
    expected = {"throttle_wait": (logging.DEBUG, "debug"), "retry_scheduled": (logging.WARNING, "warning"),
                "retries_exhausted": (logging.ERROR, "error"), "request_failed": (logging.ERROR, "error")}
    assert {event for _, _, event in records} == set(expected)
    assert all((levelno, level) == expected[event] for levelno, level, event in records)


def test_schema_rejects_a_wrong_level():
    import jsonschema
    from notion2pg_bulk.telemetry import load_schema

    event = {"event": "throttle_wait", "level": "error", "ts": "t", "propagation_id": "p",
             "locus": {"phase": None, "entity_type": None, "entity_id": None, "method": "GET", "endpoint": "/v1/x"},
             "limiter": "connection", "wait_s": 1.0}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(event, load_schema())
    jsonschema.validate({**event, "level": "debug"}, load_schema())


def test_missing_ok_turns_a_404_into_none(clock, telemetry_events):
    handler, calls = scripted([respond(error_response(404, "object_not_found"))])
    assert make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}", missing_ok=True) is None
    assert len(calls) == 1
    [event] = telemetry_events.events
    assert (event["event"], event["level"], event["anomaly"]["http_status"]) == ("discovery_stopped", "info", 404)


@pytest.mark.parametrize("status,code", [(400, "validation_error"), (403, "restricted_resource"), (401, "unauthorized")])
def test_missing_ok_does_not_hide_other_errors(clock, telemetry_events, status, code):
    handler, _ = scripted([respond(error_response(status, code))])
    with pytest.raises(NotionAPIError):
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}", missing_ok=True)
    assert telemetry_events.names() == ["request_failed"]


def test_404_raises_without_missing_ok(clock, telemetry_events):
    handler, _ = scripted([respond(error_response(404, "object_not_found"))])
    with pytest.raises(NotionAPIError):
        make_http(handler, clock).request("GET", "pages/p", "/v1/pages/{page_id}")
    assert telemetry_events.names() == ["request_failed"]
