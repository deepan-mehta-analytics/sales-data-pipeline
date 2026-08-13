# =============================================================================
# tests/integration/test_incremental_load.py
# End-to-end integration test for the incremental load feature.
#
# Runs the real pipeline twice against the real project paths (matching
# test_pipeline.py's convention), with a synthetic batch of new orders
# appended in between. Backs up and restores data/bronze/sales_data.csv and
# database/superstore.duckdb around the whole test so the repo is never
# left in a mutated state.
# =============================================================================

import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "sales_data.csv"
DB_PATH = PROJECT_ROOT / "database" / "superstore.duckdb"


@pytest.fixture
def backup_and_restore_state():
    """Snapshot the real bronze CSV and DuckDB file, restore them after the test."""
    bronze_backup = BRONZE_PATH.read_bytes()
    db_backup = DB_PATH.read_bytes() if DB_PATH.exists() else None

    yield

    BRONZE_PATH.write_bytes(bronze_backup)
    if db_backup is not None:
        DB_PATH.write_bytes(db_backup)
    elif DB_PATH.exists():
        DB_PATH.unlink()


class TestIncrementalLoadEndToEnd:
    def test_incremental_run_only_adds_new_rows(self, backup_and_restore_state):
        from orchestration.pipeline import run
        from scripts.simulate_new_orders import append_to_bronze, generate_new_orders
        from src.utils.pipeline_state import get_watermark

        # Establish a clean, fully-loaded baseline.
        run(full_refresh=True)

        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            rows_after_baseline = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        finally:
            con.close()

        watermark_after_baseline = get_watermark(DB_PATH)
        assert watermark_after_baseline is not None

        # Append 10 synthetic orders dated after every real historical order.
        as_of = date(2026, 8, 13)
        next_row_id = 100000  # Well above the real dataset's max Row ID (9,994)
        new_orders = generate_new_orders(count=10, as_of=as_of, next_row_id=next_row_id)
        append_to_bronze(new_orders, BRONZE_PATH)

        # Incremental run — must pick up exactly the 10 new rows.
        report = run(full_refresh=False)

        assert report["overall_status"] == "success"

        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            rows_after_incremental = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        finally:
            con.close()

        assert rows_after_incremental == rows_after_baseline + 10

        watermark_after_incremental = get_watermark(DB_PATH)
        assert watermark_after_incremental == "2026-08-13"

        # Re-running immediately with no new data must be a no-op (idempotency /
        # late-arrival-buffer dedup check) — row count must not change.
        run(full_refresh=False)

        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            rows_after_rerun = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        finally:
            con.close()

        assert rows_after_rerun == rows_after_incremental
