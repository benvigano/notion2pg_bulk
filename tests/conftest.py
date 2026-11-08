"""Pytest fixtures: fake clock, telemetry capture and a fresh PostgreSQL database per test.

PostgreSQL is real. Set ``NOTION2PG_TEST_ADMIN_URL`` to a superuser URL of a
running server (e.g. ``postgresql://postgres:postgres@localhost/postgres``);
otherwise a throwaway cluster is started with the local ``initdb``.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Iterator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from helpers import FakeClock, TelemetryCapture


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def telemetry_events():
    with TelemetryCapture() as capture:
        yield capture


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _bindir() -> Path:
    pg_config = shutil.which("pg_config")
    if pg_config is None:
        raise RuntimeError("Set NOTION2PG_TEST_ADMIN_URL or install PostgreSQL server binaries")
    return Path(subprocess.check_output([pg_config, "--bindir"], text=True).strip())


@pytest.fixture(scope="session")
def admin_url() -> Iterator[str]:
    configured = os.environ.get("NOTION2PG_TEST_ADMIN_URL")
    if configured:
        yield configured
        return
    bindir = _bindir()
    data = Path(tempfile.mkdtemp(prefix="n2pg-pg-"))
    port = _free_port()
    subprocess.run([bindir / "initdb", "-D", data, "-U", "postgres", "--auth=trust"], check=True, capture_output=True)
    subprocess.run(
        [bindir / "pg_ctl", "-D", data, "-w", "-o", f"-p {port} -k {data} -c listen_addresses=127.0.0.1", "start"],
        check=True, capture_output=True,
    )
    try:
        yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run([bindir / "pg_ctl", "-D", data, "-m", "immediate", "stop"], check=False, capture_output=True)
        shutil.rmtree(data, ignore_errors=True)


@pytest.fixture
def database_url(admin_url: str) -> Iterator[str]:
    """URL of a freshly created, empty database, dropped after the test."""
    yield from _fresh_database(admin_url)


@pytest.fixture
def second_database_url(admin_url: str) -> Iterator[str]:
    """URL of another freshly created, empty database, dropped after the test."""
    yield from _fresh_database(admin_url)


def _fresh_database(admin_url: str) -> Iterator[str]:
    name = f"n2pg_test_{uuid.uuid4().hex[:12]}"
    admin = create_engine(make_url(admin_url).set(drivername="postgresql+psycopg"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield make_url(admin_url).set(database=name).render_as_string(hide_password=False)
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()
