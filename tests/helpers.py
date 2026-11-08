"""Shared test utilities: fake clock and telemetry capture."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import jsonschema

from notion2pg_bulk.telemetry import LOGGER_NAME, load_schema


class FakeClock:
    """Monotonic clock advanced only by :meth:`sleep`."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: List[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class TelemetryCapture(logging.Handler):
    """Collects telemetry payloads emitted on the package logger."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.events: List[Dict[str, Any]] = []
        self._validator = jsonschema.Draft202012Validator(load_schema())

    def emit(self, record: logging.LogRecord) -> None:
        payload = getattr(record, "telemetry", None)
        if payload is not None:
            self._validator.validate(payload)
            self.events.append(payload)

    def names(self) -> List[str]:
        return [event["event"] for event in self.events]

    def __enter__(self) -> "TelemetryCapture":
        logger = logging.getLogger(LOGGER_NAME)
        self._previous_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self)
        return self

    def __exit__(self, *exc_info: object) -> None:
        logger = logging.getLogger(LOGGER_NAME)
        logger.removeHandler(self)
        logger.setLevel(self._previous_level)


def run_snapshot(fake, database_url: str, clock: FakeClock, *, row_bodies: bool = True, plan: str = "free") -> None:
    """Run the snapshot against a fake Notion workspace and a real database."""
    from notion2pg_bulk.notion_api import NotionAPI
    from notion2pg_bulk.notion_http import NotionHTTP
    from notion2pg_bulk.orchestrate import create_target_engine, run
    from notion2pg_bulk.telemetry import Telemetry

    engine = create_target_engine(database_url)
    try:
        with NotionHTTP("test-token", plan, Telemetry("test-run"), transport=fake.transport(),
                        clock=clock, sleep=clock.sleep) as http:
            run(engine, NotionAPI(http), row_bodies=row_bodies)
    finally:
        engine.dispose()


def dump_database(database_url: str) -> Dict[str, List[tuple]]:
    """Every row of every table in the ``notion`` and ``rows`` schemas, sorted."""
    from sqlalchemy import text
    from notion2pg_bulk.orchestrate import create_target_engine

    engine = create_target_engine(database_url)
    try:
        with engine.connect() as conn:
            names = conn.execute(text(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema IN ('notion', 'rows') ORDER BY 1, 2"
            )).fetchall()
            preparer = conn.dialect.identifier_preparer
            result: Dict[str, List[tuple]] = {}
            for schema_name, table_name in names:
                qualified = f"{preparer.quote(schema_name)}.{preparer.quote(table_name)}"
                rows = conn.execute(text(f"SELECT * FROM {qualified}")).fetchall()
                result[f"{schema_name}.{table_name}"] = sorted((tuple(map(repr, row)) for row in rows))
            return result
    finally:
        engine.dispose()
