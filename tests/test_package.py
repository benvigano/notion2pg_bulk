"""Package import smoke test."""

import notion2pg_bulk


def test_package_exports_public_api():
    assert "snapshot" in notion2pg_bulk.__all__
