# =============================================================================
# src/utils/pipeline_state.py
# Watermark tracking for the pipeline's incremental load feature (v2.0).
#
# Stores a single-row `pipeline_state` table inside the pipeline's DuckDB
# database, so extract() knows where it left off on the next run. This
# module owns all reads/writes to that table — no other module opens it
# directly.
# =============================================================================

from pathlib import Path  # Cross-platform path resolution
from typing import Optional  # Type hint for "no watermark yet"

import duckdb  # Embedded OLAP engine — same store the watermark lives in

from src.utils.logger import get_logger  # Centralised JSON logger

logger = get_logger(__name__)  # Create logger instance for this module


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create the pipeline_state table if it doesn't exist yet (idempotent)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            last_watermark DATE,
            last_run_id VARCHAR,
            updated_at TIMESTAMP
        )
        """)


def get_watermark(db_path: Path) -> Optional[str]:
    """
    Return the last-processed Order Date watermark as an ISO date string
    (YYYY-MM-DD), or None if no watermark has been recorded yet — either
    because the database file doesn't exist, or the pipeline_state table
    doesn't exist, or the table is empty. All three cases mean "first run,
    do a full load".
    """
    if not db_path.exists():  # Check if database file exists yet
        return None

    con = duckdb.connect(str(db_path), read_only=True)  # Open connection in read-only mode
    try:
        # Check if pipeline_state table exists in the database
        table_exists = (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'pipeline_state'"
            ).fetchone()[0]
            > 0
        )
        if not table_exists:  # Table doesn't exist yet — first run
            return None

        # Query the watermark from the pipeline_state table
        row = con.execute("SELECT last_watermark FROM pipeline_state LIMIT 1").fetchone()
        if row is None or row[0] is None:  # No rows or null watermark — first run
            return None
        return str(row[0])  # DuckDB DATE -> Python date -> "YYYY-MM-DD"
    finally:
        con.close()  # Always close connection


def set_watermark(db_path: Path, watermark: str, run_id: str) -> None:
    """
    Overwrite the single pipeline_state row with a new watermark.

    Only call this after a pipeline run has fully succeeded — see
    orchestration/pipeline.py's run() and dags/sales_pipeline_dag.py's
    load_task, which both call this exactly once, at the very end.

    Parameters
    ----------
    db_path   : Path  Path to the pipeline's DuckDB database file.
    watermark : str   ISO date string (YYYY-MM-DD) — the new high-water mark.
    run_id    : str   Identifier of the run that produced this watermark,
                       stored purely for audit/debugging.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)  # Create database/ if needed
    con = duckdb.connect(str(db_path))  # Open connection (creates file if needed)
    try:
        _ensure_table(con)  # Create table if it doesn't exist (idempotent)
        con.execute("DELETE FROM pipeline_state")  # Single-row table: clear before inserting
        con.execute(
            "INSERT INTO pipeline_state (last_watermark, last_run_id, updated_at) " "VALUES (?, ?, CURRENT_TIMESTAMP)",
            [watermark, run_id],  # Parameterised query for safety
        )
        # Log the watermark update with context
        logger.info("Watermark updated", extra={"last_watermark": watermark, "run_id": run_id})
    finally:
        con.close()  # Always close connection
