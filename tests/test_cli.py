"""CLI: argument handling and JSON telemetry on stderr."""

from __future__ import annotations

import json
import logging

import pytest

from notion2pg_bulk import cli
from notion2pg_bulk.telemetry import JSONFormatter


def test_missing_token_exits_non_zero(monkeypatch):
    monkeypatch.delenv("NOTION_TOKEN", raising=False)
    with pytest.raises(SystemExit) as caught:
        cli.main(["--database-url", "postgresql://x/y"])
    assert caught.value.code != 0


def test_arguments_reach_snapshot(monkeypatch):
    seen = {}
    monkeypatch.setattr(cli, "snapshot", lambda token, url, **kw: seen.update(token=token, url=url, **kw))
    monkeypatch.setenv("NOTION_TOKEN", "tok")
    assert cli.main(["--database-url", "postgresql://x/y", "--plan", "business", "--no-row-bodies",
                     "--propagation-id", "abc"]) == 0
    assert seen == {"token": "tok", "url": "postgresql://x/y", "plan": "business", "row_bodies": False,
                    "propagation_id": "abc"}


def test_failure_propagates_from_cli(monkeypatch):
    from notion2pg_bulk.errors import TargetNotEmptyError

    def fail(*args, **kwargs):
        raise TargetNotEmptyError(["relation public.t"])

    monkeypatch.setattr(cli, "snapshot", fail)
    with pytest.raises(TargetNotEmptyError):
        cli.main(["--notion-token", "t", "--database-url", "postgresql://x/y"])


def test_cli_leaves_no_handler_behind(monkeypatch):
    before = list(logging.getLogger("notion2pg_bulk").handlers)
    monkeypatch.setattr(cli, "snapshot", lambda *a, **k: None)
    cli.main(["--notion-token", "t", "--database-url", "postgresql://x/y"])
    assert logging.getLogger("notion2pg_bulk").handlers == before


def test_cli_output_leaves_out_debug_events(monkeypatch):
    seen = {}

    def fake_snapshot(*args, **kwargs):
        logger = logging.getLogger("notion2pg_bulk")
        seen["debug"], seen["info"] = logger.isEnabledFor(logging.DEBUG), logger.isEnabledFor(logging.INFO)

    monkeypatch.setattr(cli, "snapshot", fake_snapshot)
    cli.main(["--notion-token", "t", "--database-url", "postgresql://x/y"])
    assert seen == {"debug": False, "info": True}


def test_json_formatter_outputs_one_object_per_record():
    record = logging.LogRecord("notion2pg_bulk", logging.WARNING, __file__, 1, "{}", None, None)
    record.telemetry = {"event": "throttle_wait", "wait_s": 1.0}
    assert json.loads(JSONFormatter().format(record)) == {"event": "throttle_wait", "wait_s": 1.0}
    plain = logging.LogRecord("notion2pg_bulk", logging.INFO, __file__, 1, "hello", None, None)
    assert json.loads(JSONFormatter().format(plain))["message"] == "hello"
