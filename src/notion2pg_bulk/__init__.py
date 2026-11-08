"""Snapshot an entire Notion workspace into a PostgreSQL database."""

import logging

from .errors import (
    Notion2PgError,
    NotionAPIError,
    RetriesExhaustedError,
    SnapshotIntegrityError,
    TargetNotEmptyError,
)
from .orchestrate import snapshot
from .telemetry import LOGGER_NAME

logging.getLogger(LOGGER_NAME).addHandler(logging.NullHandler())

__all__ = [
    "snapshot",
    "Notion2PgError",
    "NotionAPIError",
    "RetriesExhaustedError",
    "SnapshotIntegrityError",
    "TargetNotEmptyError",
]
