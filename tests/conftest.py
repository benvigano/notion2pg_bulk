"""Pytest fixtures shared by the suite."""

from __future__ import annotations

import pytest

from helpers import FakeClock, TelemetryCapture


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def telemetry_events():
    with TelemetryCapture() as capture:
        yield capture
