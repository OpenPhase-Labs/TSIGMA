"""Smoke test for the DuckDB httpfs install script."""

import importlib
import sys

import pytest


def test_required_extensions_listed():
    """The install script must list httpfs in REQUIRED_EXTENSIONS."""
    spec = importlib.util.spec_from_file_location(
        "install_duckdb_extensions",
        "scripts/install_duckdb_extensions.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert "httpfs" in mod.REQUIRED_EXTENSIONS


def test_install_extension_via_real_duckdb():
    """install_extension actually installs and loads httpfs via the Python API."""
    spec = importlib.util.spec_from_file_location(
        "install_duckdb_extensions",
        "scripts/install_duckdb_extensions.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Should not raise — if duckdb is installed and httpfs is available
    mod.install_extension("httpfs")


def test_main_exits_zero():
    """main() exits with code 0 on success."""
    spec = importlib.util.spec_from_file_location(
        "install_duckdb_extensions",
        "scripts/install_duckdb_extensions.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()

    assert exc_info.value.code == 0
