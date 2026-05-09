# =============================================================================
# api/database.py
# DuckDB read-only connection dependency for the FastAPI query layer.
#
# Design choices:
#   - A new read-only connection is opened per request and closed on teardown.
#     DuckDB supports multiple simultaneous read_only connections to the same
#     file, so this is safe under concurrent FastAPI requests.
#   - The database path is derived from PROJECT_ROOT so it resolves correctly
#     whether the app is started locally, via Docker, or in CI.
#   - A 503 is raised before yielding if the database file does not exist,
#     rather than letting DuckDB raise a cryptic FileNotFoundError.
# =============================================================================

from pathlib import Path  # Cross-platform path resolution
from typing import Generator  # Type hint for a generator-based dependency

import duckdb  # Embedded columnar OLAP database
from fastapi import HTTPException  # HTTP error raised when the DB is unavailable

# ── Path constants ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # api → project root
DB_PATH = PROJECT_ROOT / "database" / "superstore.duckdb"  # Absolute path to the DuckDB file


# =============================================================================
# get_db  –  FastAPI dependency
# =============================================================================


def get_db() -> Generator:
    """
    FastAPI dependency that yields a read-only DuckDB connection for each
    request and closes it cleanly on teardown.

    Raises
    ------
    HTTPException (503)
        When the DuckDB file does not exist — the pipeline has not been run yet.

    Yields
    ------
    duckdb.DuckDBPyConnection
        An open, read-only connection to the superstore analytical database.
    """
    if not DB_PATH.exists():  # Guard: reject requests when no database is present
        raise HTTPException(  # Return 503 Service Unavailable with an informative message
            status_code=503,
            detail="Database unavailable — run the ETL pipeline to generate the DuckDB store",
        )

    con = duckdb.connect(str(DB_PATH), read_only=True)  # Open read-only connection (concurrency-safe)

    try:
        yield con  # Hand the connection to the route handler
    finally:
        con.close()  # Always close the connection after the request completes
