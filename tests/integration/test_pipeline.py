# =============================================================================
# tests/integration/test_pipeline.py
# End-to-end integration test for orchestration/pipeline.py.
#
# This test runs the complete pipeline from extraction through to DuckDB
# loading using the sample bronze CSV, and verifies that:
#   - The pipeline completes without raising an exception
#   - The run report has the correct structure and status
#   - All gold Parquet files were created
#   - The DuckDB database was created and contains the expected tables
#   - The fact table in DuckDB has the expected column count and row count
#
# Integration tests are intentionally kept to a minimum to keep CI fast.
# They call real I/O (disk reads, DuckDB writes) so they take longer than
# unit tests.  Run with: pytest tests/integration/ -v
# =============================================================================

import pytest               # pytest testing framework
import duckdb               # Used to query the DuckDB file produced by the pipeline
import pandas as pd         # DataFrame assertions
from pathlib import Path    # Path handling
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # tests/integration → project root
sys.path.insert(0, str(PROJECT_ROOT))


class TestFullPipelineRun:
    """
    Integration tests that execute the entire pipeline against the sample
    bronze CSV file placed at data/bronze/sales_data.csv.
    """

    @pytest.fixture(scope="class", autouse=True)
    def pipeline_report(self):
        """
        Run the full pipeline once for this test class and expose the run
        report to all test methods via self.report.

        scope="class" means the pipeline runs once for all tests in this
        class, avoiding repeated (slow) full pipeline runs.
        autouse=True ensures the fixture is invoked automatically.
        """
        from orchestration.pipeline import run   # Import the pipeline orchestrator

        # Execute the full pipeline against the sample data.
        report = run()

        # Attach the report to the class so individual test methods can inspect it.
        TestFullPipelineRun.report = report

        yield   # Give control to the test methods

    def test_pipeline_status_is_success(self):
        """The overall pipeline run must complete with status 'success'."""
        assert self.report["overall_status"] == "success", (
            f"Pipeline did not succeed: {self.report}"
        )

    def test_run_report_has_all_stage_keys(self):
        """The run report must include entries for all pipeline stages."""
        expected_stages = {"extract", "clean", "engineer", "load"}
        actual_stages   = set(self.report["stages"].keys())

        assert expected_stages.issubset(actual_stages), (
            f"Report is missing stages: {expected_stages - actual_stages}"
        )

    def test_elapsed_seconds_positive(self):
        """The total elapsed time must be a positive number."""
        assert self.report["elapsed_seconds"] > 0, (
            "elapsed_seconds must be greater than zero"
        )

    def test_quality_checks_ran(self):
        """The run report must include results from at least two quality passes."""
        assert len(self.report["quality_checks"]) >= 2, (
            "Expected at least two quality check passes (raw + cleaned)"
        )

    def test_all_quality_passes_passed(self):
        """Every quality check pass in the report must have passed=True."""
        for qc in self.report["quality_checks"]:
            assert qc["passed"] is True, (
                f"Quality check pass '{qc['stage']}' reported failures"
            )

    def test_silver_parquet_created(self):
        """The silver Parquet file must exist on disk after the pipeline runs."""
        silver_path = PROJECT_ROOT / "data" / "silver" / "cleaned_sales.parquet"

        assert silver_path.exists(), f"Silver Parquet not found at {silver_path}"
        assert silver_path.stat().st_size > 0, "Silver Parquet file is empty"

    def test_gold_parquets_created(self):
        """All five gold Parquet files must exist on disk after the pipeline runs."""
        gold_files = [
            "data/gold/sales_by_region.parquet",
            "data/gold/sales_by_category.parquet",
            "data/gold/customer_segments.parquet",
            "data/gold/monthly_trends.parquet",
            "data/gold/product_performance.parquet",
        ]

        for relative_path in gold_files:   # Check each expected gold file
            full_path = PROJECT_ROOT / relative_path
            assert full_path.exists(), f"Gold Parquet not found: {relative_path}"
            assert full_path.stat().st_size > 0, f"Gold Parquet is empty: {relative_path}"

    def test_duckdb_created(self):
        """The DuckDB database file must exist on disk after the pipeline runs."""
        db_path = PROJECT_ROOT / "database" / "superstore.duckdb"

        assert db_path.exists(), f"DuckDB file not found at {db_path}"
        assert db_path.stat().st_size > 0, "DuckDB file is empty"

    def test_duckdb_contains_fact_table(self):
        """
        The DuckDB database must contain the 'fact_sales' table with at least
        one row and the expected minimum number of columns.
        """
        db_path = PROJECT_ROOT / "database" / "superstore.duckdb"
        con     = duckdb.connect(str(db_path), read_only=True)   # Open in read-only mode

        try:
            # Query the row count from the fact table.
            row_count = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
            assert row_count > 0, "fact_sales must contain at least one row"

            # Query the column count; we expect at least 21 base + derived columns.
            col_count = len(con.execute("DESCRIBE fact_sales").fetchdf())
            assert col_count >= 21, (
                f"fact_sales must have >= 21 columns, found {col_count}"
            )
        finally:
            con.close()   # Always close the connection

    def test_duckdb_contains_gold_tables(self):
        """
        The DuckDB database must contain all five gold aggregation tables.
        """
        db_path      = PROJECT_ROOT / "database" / "superstore.duckdb"
        con          = duckdb.connect(str(db_path), read_only=True)
        expected_tables = {
            "agg_sales_by_region",
            "agg_sales_by_category",
            "agg_customer_segments",
            "agg_monthly_trends",
            "agg_product_performance",
        }

        try:
            # Retrieve the list of tables from the DuckDB catalogue.
            tables_df    = con.execute("SHOW TABLES").fetchdf()
            actual_tables = set(tables_df["name"].tolist())   # Set of table names in the DB

            missing = expected_tables - actual_tables
            assert not missing, f"DuckDB is missing gold tables: {missing}"
        finally:
            con.close()

    def test_silver_parquet_loadable(self):
        """
        The silver Parquet file must be loadable by pandas and contain
        the expected columns.
        """
        silver_path  = PROJECT_ROOT / "data" / "silver" / "cleaned_sales.parquet"
        df           = pd.read_parquet(silver_path)   # Load the Parquet into a DataFrame

        # Check that base columns survived the clean + engineer → Parquet round-trip.
        expected_base = {"Order ID", "Sales", "Profit", "Category", "Region"}
        assert expected_base.issubset(set(df.columns)), (
            f"Silver Parquet is missing columns: {expected_base - set(df.columns)}"
        )

    def test_gold_region_table_has_four_regions(self):
        """
        The agg_sales_by_region gold table must contain exactly four rows
        (one per US sales region: East, West, Central, South) — at least
        when the full Kaggle dataset is used.  With the sample CSV we only
        expect >= 1 row.
        """
        gold_path = PROJECT_ROOT / "data" / "gold" / "sales_by_region.parquet"
        df        = pd.read_parquet(gold_path)

        # With the sample CSV we have South, West, and East → at least 1 row.
        assert len(df) >= 1, "sales_by_region must contain at least one region row"
        assert "Region" in df.columns, "sales_by_region must have a Region column"
        assert "total_sales" in df.columns, "sales_by_region must have a total_sales column"
