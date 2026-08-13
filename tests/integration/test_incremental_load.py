# =============================================================================
# tests/integration/test_incremental_load.py
# End-to-end integration test for the incremental load feature.
#
# Runs the real pipeline twice against the real project paths (matching
# test_pipeline.py's convention), with a synthetic batch of new orders
# appended in between. Backs up and restores every real project file the
# pipeline run mutates — data/bronze/sales_data.csv, database/superstore.duckdb,
# data/silver/cleaned_sales.parquet, all 5 gold Parquet files, and
# reports/run_stats_reference.json (the drift baseline) — around the whole
# test so the repo is never left in a mutated state.
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
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "cleaned_sales.parquet"
GOLD_PATHS = [
    PROJECT_ROOT / "data" / "gold" / "sales_by_region.parquet",
    PROJECT_ROOT / "data" / "gold" / "sales_by_category.parquet",
    PROJECT_ROOT / "data" / "gold" / "customer_segments.parquet",
    PROJECT_ROOT / "data" / "gold" / "monthly_trends.parquet",
    PROJECT_ROOT / "data" / "gold" / "product_performance.parquet",
]
DRIFT_REFERENCE_PATH = PROJECT_ROOT / "reports" / "run_stats_reference.json"

# Every path mutated by a real pipeline run that this fixture must back up
# and restore, beyond the always-present bronze CSV: DuckDB, the silver
# Parquet, all gold Parquet files, and the drift reference JSON.
RESTORABLE_PATHS = [DB_PATH, SILVER_PATH, *GOLD_PATHS, DRIFT_REFERENCE_PATH]


@pytest.fixture
def backup_and_restore_state():
    """
    Snapshot the real bronze CSV plus every other file a real pipeline run
    rewrites (DuckDB, silver Parquet, all 5 gold Parquet files, and the
    drift reference JSON), then restore all of them after the test —
    including deleting any of these files that didn't exist beforehand
    but were created by the run.
    """
    bronze_backup = BRONZE_PATH.read_bytes()
    backups = {path: (path.read_bytes() if path.exists() else None) for path in RESTORABLE_PATHS}

    yield

    BRONZE_PATH.write_bytes(bronze_backup)
    for path, backup in backups.items():
        if backup is not None:
            path.write_bytes(backup)
        elif path.exists():
            path.unlink()


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
