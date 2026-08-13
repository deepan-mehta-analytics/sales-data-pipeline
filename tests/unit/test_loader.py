# =============================================================================
# tests/unit/test_loader.py
# Unit tests for src/load/loader.py's incremental fact_sales behavior.
#
# Uses tmp_path-based bronze/silver/gold/database paths via monkeypatching
# loader.PROJECT_ROOT's config resolution, so these tests never touch the
# real project's data/ or database/ directories.
# =============================================================================

import duckdb
import pandas as pd
import pytest

from src.load import loader


@pytest.fixture
def isolated_paths(tmp_path, monkeypatch):
    """
    Point loader._load_config() at a temporary config pointing every path
    (silver, gold, database) inside tmp_path, so tests never touch the real
    project's data/database directories.
    """
    fake_config = {
        "paths": {
            "silver": "silver/cleaned_sales.parquet",
            "gold": {
                "sales_by_region": "gold/sales_by_region.parquet",
                "sales_by_category": "gold/sales_by_category.parquet",
                "customer_segments": "gold/customer_segments.parquet",
                "monthly_trends": "gold/monthly_trends.parquet",
                "product_performance": "gold/product_performance.parquet",
            },
            "database": "db/test.duckdb",
        }
    }
    monkeypatch.setattr(loader, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(loader, "_load_config", lambda: fake_config)
    return tmp_path


def _make_batch(row_ids, order_dates):
    """
    Build a minimal enriched-shape DataFrame with every column the gold
    aggregation builders in loader.py actually read — in particular
    build_monthly_trends() needs order_year/order_month/order_month_name,
    which feature_engineer.py normally adds; this fixture adds them
    directly since these tests bypass clean()/engineer() entirely.
    """
    n = len(row_ids)
    parsed_dates = pd.to_datetime(order_dates)
    return pd.DataFrame(
        {
            "Row ID": row_ids,
            "Order ID": [f"CA-2017-{i:06d}" for i in row_ids],
            "Order Date": parsed_dates,
            "Region": ["East"] * n,
            "Category": ["Furniture"] * n,
            "Sub-Category": ["Chairs"] * n,
            "Product ID": [f"FUR-CH-{i:08d}" for i in row_ids],
            "Product Name": ["Test Chair"] * n,
            "Customer ID": [f"CG-{i:05d}" for i in row_ids],
            "Segment": ["Consumer"] * n,
            "Sales": [100.0] * n,
            "Quantity": [1] * n,
            "Discount": [0.0] * n,
            "Profit": [20.0] * n,
            "profit_margin_pct": [20.0] * n,
            "order_year": parsed_dates.year,
            "order_month": parsed_dates.month,
            "order_month_name": parsed_dates.strftime("%b"),
        }
    )


class TestLoadFirstRun:
    def test_first_run_loads_full_batch(self, isolated_paths):
        batch = _make_batch([1, 2, 3], ["2017-01-01", "2017-01-02", "2017-01-03"])

        meta = loader.load(batch)

        assert meta["rows_loaded"] == 3
        assert meta["new_rows_inserted"] == 3
        assert meta["watermark_candidate"] == "2017-01-03"

    def test_first_run_creates_gold_tables(self, isolated_paths):
        batch = _make_batch([1, 2, 3], ["2017-01-01", "2017-01-02", "2017-01-03"])

        meta = loader.load(batch)

        db_path = isolated_paths / "db" / "test.duckdb"
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            tables = set(con.execute("SHOW TABLES").fetchdf()["name"].tolist())
        finally:
            con.close()

        assert "fact_sales" in tables
        assert "agg_sales_by_region" in tables
        assert set(meta["gold_tables"]) == {
            "agg_sales_by_region",
            "agg_sales_by_category",
            "agg_customer_segments",
            "agg_monthly_trends",
            "agg_product_performance",
        }


class TestLoadIncremental:
    def test_second_run_appends_only_new_rows(self, isolated_paths):
        first_batch = _make_batch([1, 2, 3], ["2017-01-01", "2017-01-02", "2017-01-03"])
        loader.load(first_batch)

        second_batch = _make_batch([4, 5], ["2017-01-04", "2017-01-05"])
        meta = loader.load(second_batch)

        assert meta["new_rows_inserted"] == 2
        assert meta["rows_loaded"] == 5, "rows_loaded must reflect the FULL accumulated fact_sales"
        assert meta["watermark_candidate"] == "2017-01-05"

    def test_repeated_batch_is_deduped(self, isolated_paths):
        """Re-processing the same Row IDs (e.g. a late-arrival buffer re-check) must not duplicate rows."""
        batch = _make_batch([1, 2, 3], ["2017-01-01", "2017-01-02", "2017-01-03"])
        loader.load(batch)

        meta = loader.load(batch)  # Same Row IDs again

        assert meta["new_rows_inserted"] == 0
        assert meta["rows_loaded"] == 3, "fact_sales must still have exactly 3 rows, not 6"

    def test_gold_tables_reflect_full_accumulated_data(self, isolated_paths):
        """agg_* tables must be built from the FULL fact_sales, not just the latest batch."""
        loader.load(_make_batch([1, 2], ["2017-01-01", "2017-01-02"]))
        loader.load(_make_batch([3, 4], ["2017-01-03", "2017-01-04"]))

        gold_path = isolated_paths / "gold" / "sales_by_region.parquet"
        agg_df = pd.read_parquet(gold_path)

        assert agg_df.loc[agg_df["Region"] == "East", "total_rows"].iloc[0] == 4

    def test_empty_batch_inserts_nothing_and_has_no_watermark(self, isolated_paths):
        loader.load(_make_batch([1], ["2017-01-01"]))  # Establish a baseline

        empty_batch = _make_batch([1], ["2017-01-01"]).iloc[0:0]  # Zero rows, same schema
        meta = loader.load(empty_batch)

        assert meta["new_rows_inserted"] == 0
        assert meta["watermark_candidate"] is None


class TestLoadFullRefresh:
    def test_full_refresh_replaces_existing_data(self, isolated_paths):
        loader.load(_make_batch([1, 2, 3], ["2017-01-01", "2017-01-02", "2017-01-03"]))

        # A full_refresh call with a totally different batch must REPLACE, not append.
        meta = loader.load(_make_batch([10, 11], ["2017-06-01", "2017-06-02"]), full_refresh=True)

        assert meta["rows_loaded"] == 2, "Full refresh must not keep the old rows"
        assert meta["new_rows_inserted"] == 2
