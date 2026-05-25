#!/usr/bin/env python3
"""Pre-install DuckDB httpfs extension at Docker build time.

Uses the Python duckdb API — no CLI binary required.
This script is idempotent: running it multiple times is safe.
It exits non-zero if the httpfs extension cannot be loaded, which
will cause the Docker build to fail.

Usage:
    python scripts/install_duckdb_extensions.py

Exit codes:
    0  — httpfs extension installed and verified
    1  — installation or verification failed
"""

import sys

REQUIRED_EXTENSIONS = ["httpfs"]


def install_extension(name: str) -> None:
    """Install and verify a single DuckDB extension via the Python API."""
    import duckdb

    with duckdb.connect(":memory:") as con:
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")


def main() -> None:
    """Install all required DuckDB extensions."""
    for ext in REQUIRED_EXTENSIONS:
        print(f"Installing DuckDB extension: {ext}...")
        install_extension(ext)
        print(f"  OK: {ext} installed and verified.")

    print(f"All {len(REQUIRED_EXTENSIONS)} extension(s) installed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
