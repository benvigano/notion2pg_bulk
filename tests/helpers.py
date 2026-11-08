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
