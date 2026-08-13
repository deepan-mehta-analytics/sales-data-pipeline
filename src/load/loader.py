# =============================================================================
# src/load/loader.py
# Gold-layer loading step for the Superstore Sales Data Pipeline.
#
# Responsibilities:
#   1. Persist the enriched silver DataFrame as a Parquet file.
#   2. Compute five gold-layer business aggregations.
#   3. Save each aggregation as its own Parquet file.
#   4. Load everything into a DuckDB analytical database for SQL querying.
#
# Why DuckDB?
#   DuckDB is an embedded, columnar OLAP engine that requires zero server
#   infrastructure.  It reads Parquet files natively, supports full SQL,
#   and can be queried directly from Python or from BI tools via JDBC/ODBC.
#   It is the modern equivalent of SQLite but optimised for analytical queries.
#
# Why Parquet?
#   Parquet is a columnar binary format that compresses 5-10× smaller than
#   CSV, reads far faster for column-selective queries, and preserves dtypes
#   perfectly (no string/int ambiguity on reload).
# =============================================================================

from pathlib import Path  # Cross-platform path resolution and directory creation

import duckdb  # Embedded OLAP analytical database
import pandas as pd  # Core data-manipulation library
import yaml  # Reads config.yaml for paths and table names

from src.utils.logger import get_logger  # Centralised JSON logger

# Obtain a module-level logger.
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # load → src → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Pipeline configuration


def _load_config() -> dict:
    """Read and return config.yaml as a Python dict."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# =============================================================================
# Parquet writing helpers
# =============================================================================


def _save_parquet(df: pd.DataFrame, path: Path, label: str) -> None:
    """
    Serialise a DataFrame to a Parquet file, creating parent directories
    if they do not already exist.

    Parameters
    ----------
    df    : pd.DataFrame  Data to persist.
    path  : Path          Absolute path of the output Parquet file.
    label : str           Human-readable name used in log messages.
    """
    path.parent.mkdir(parents=True, exist_ok=True)  # Create the output directory tree

    # Write to Parquet using pyarrow as the engine (best type support).
    # index=False omits the pandas RangeIndex from the file (saves space).
    df.to_parquet(path, engine="pyarrow", index=False, compression="snappy")

    logger.info(  # Log the file written and its size on disk
        f"Parquet written: {label}",
        extra={"path": str(path), "rows": len(df), "size_bytes": path.stat().st_size},
    )


# =============================================================================
# Gold-layer aggregation builders
# Each function accepts the enriched silver DataFrame and returns a small,
# business-focused summary DataFrame ready for the gold layer.
# =============================================================================


def build_sales_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate total sales, profit, order count, and average discount
    grouped by US sales region.

    Parameters
    ----------
    df : pd.DataFrame  Enriched silver-layer DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per region with regional KPI columns.
    """
    agg = (
        df.groupby("Region")  # Group every transaction by its sales region
        .agg(
            total_sales=("Sales", "sum"),  # Sum of all sales revenue
            total_profit=("Profit", "sum"),  # Sum of all net profit
            total_orders=("Order ID", "nunique"),  # Count of distinct order IDs
            total_rows=("Row ID", "count"),  # Count of individual line items
            avg_discount=("Discount", "mean"),  # Mean discount rate
            avg_profit_margin=("profit_margin_pct", "mean"),  # Mean profit margin %
        )
        .reset_index()  # Promote Region from index back to a regular column
    )

    # Round float columns to 2 decimal places for clean display.
    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    agg["avg_profit_margin"] = agg["avg_profit_margin"].round(2)

    return agg  # Return the aggregated DataFrame


def build_sales_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sales and profit by product Category and Sub-Category.

    Parameters
    ----------
    df : pd.DataFrame  Enriched silver-layer DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per Category / Sub-Category combination with KPI columns.
    """
    agg = (
        df.groupby(["Category", "Sub-Category"])  # Group by category hierarchy
        .agg(
            total_sales=("Sales", "sum"),  # Total revenue per sub-category
            total_profit=("Profit", "sum"),  # Total profit per sub-category
            total_units=("Quantity", "sum"),  # Total units sold
            total_orders=("Order ID", "nunique"),  # Distinct orders including this sub-cat
            avg_discount=("Discount", "mean"),  # Mean discount applied
            avg_profit_margin=("profit_margin_pct", "mean"),  # Mean margin %
        )
        .reset_index()  # Restore Category and Sub-Category as plain columns
    )

    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    agg["avg_profit_margin"] = agg["avg_profit_margin"].round(2)

    # Sort by total_sales descending so the most valuable sub-categories appear first.
    agg = agg.sort_values("total_sales", ascending=False).reset_index(drop=True)

    return agg


def build_customer_segments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarise performance by customer Segment (Consumer / Corporate / Home Office).

    Parameters
    ----------
    df : pd.DataFrame  Enriched silver-layer DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per customer segment with acquisition and revenue KPIs.
    """
    agg = (
        df.groupby("Segment")  # Group every transaction by customer segment
        .agg(
            total_customers=("Customer ID", "nunique"),  # Unique customers in this segment
            total_orders=("Order ID", "nunique"),  # Unique orders placed
            total_sales=("Sales", "sum"),  # Total revenue generated
            total_profit=("Profit", "sum"),  # Total net profit
            avg_order_value=("Sales", "mean"),  # Average line-item revenue
            avg_profit_margin=("profit_margin_pct", "mean"),  # Average margin %
        )
        .reset_index()  # Restore Segment as a plain column
    )

    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)
    agg["avg_order_value"] = agg["avg_order_value"].round(2)
    agg["avg_profit_margin"] = agg["avg_profit_margin"].round(2)

    return agg


def build_monthly_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a month-by-month time series of sales, profit, and order volume.

    This table is the foundation for trend charts, seasonality analysis,
    and year-over-year comparisons in BI tools.

    Parameters
    ----------
    df : pd.DataFrame  Enriched silver-layer DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per year-month combination, sorted chronologically.
    """
    agg = (
        df.groupby(["order_year", "order_month", "order_month_name"])  # Group by year and month
        .agg(
            total_sales=("Sales", "sum"),  # Monthly revenue total
            total_profit=("Profit", "sum"),  # Monthly profit total
            total_orders=("Order ID", "nunique"),  # Monthly distinct orders
            total_units=("Quantity", "sum"),  # Monthly units sold
        )
        .reset_index()  # Restore year/month as plain columns
    )

    # Sort chronologically by year then month for correct time-series order.
    agg = agg.sort_values(["order_year", "order_month"]).reset_index(drop=True)

    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)

    return agg


def build_product_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank every unique product by total sales revenue.

    Provides a product-level performance table useful for identifying
    bestsellers and loss-leading SKUs.

    Parameters
    ----------
    df : pd.DataFrame  Enriched silver-layer DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per product sorted by total_sales descending.
    """
    agg = (
        df.groupby(["Product ID", "Product Name", "Category", "Sub-Category"])
        .agg(
            total_sales=("Sales", "sum"),  # Lifetime revenue for this product
            total_profit=("Profit", "sum"),  # Lifetime profit for this product
            total_units=("Quantity", "sum"),  # Total units sold
            total_orders=("Order ID", "nunique"),  # Times this product appeared in an order
            avg_discount=("Discount", "mean"),  # Average discount given on this product
            avg_profit_margin=("profit_margin_pct", "mean"),  # Average margin %
        )
        .reset_index()
    )

    agg["total_sales"] = agg["total_sales"].round(2)
    agg["total_profit"] = agg["total_profit"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    agg["avg_profit_margin"] = agg["avg_profit_margin"].round(2)

    # Sort by total sales descending so the top performers appear first.
    agg = agg.sort_values("total_sales", ascending=False).reset_index(drop=True)

    return agg


# =============================================================================
# DuckDB fact_sales writer
# =============================================================================


def _write_fact_sales(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, full_refresh: bool) -> int:
    """
    Write a batch into fact_sales — either a full reload (full_refresh=True,
    or fact_sales doesn't exist yet) or an incremental insert of only rows
    whose "Row ID" isn't already present.

    Returns
    -------
    int
        Number of rows actually inserted by this call. 0 for an empty batch,
        or for a batch that's entirely rows already present (e.g. a
        late-arrival buffer re-check that found nothing new).
    """
    table_exists = (
        con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fact_sales'").fetchone()[0] > 0
    )

    if full_refresh or not table_exists:
        logger.info("Writing fact_sales (full reload)", extra={"rows": len(df), "full_refresh": full_refresh})
        con.execute("DROP TABLE IF EXISTS fact_sales")
        con.register("_full_batch", df)
        con.execute("CREATE TABLE fact_sales AS SELECT * FROM _full_batch")
        con.unregister("_full_batch")
        return len(df)

    if df.empty:
        logger.info("No new rows to insert into fact_sales")
        return 0

    before = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    con.register("_new_batch", df)
    con.execute("""
        INSERT INTO fact_sales
        SELECT * FROM _new_batch AS nb
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_sales fs WHERE fs."Row ID" = nb."Row ID"
        )
        """)
    con.unregister("_new_batch")
    after = con.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    inserted = after - before

    logger.info(
        "fact_sales incremental insert complete",
        extra={"new_rows_inserted": inserted, "batch_size": len(df)},
    )
    return inserted


# =============================================================================
# load  –  Public entry point
# =============================================================================


def load(df: pd.DataFrame, full_refresh: bool = False) -> dict:
    """
    Execute the full loading pipeline: incremental fact_sales write ->
    re-read the full accumulated fact table -> gold Parquets + DuckDB
    aggregation tables rebuilt from that full picture.

    Parameters
    ----------
    df : pd.DataFrame
        Enriched silver-layer DataFrame — the NEW batch only when doing an
        incremental load, or the complete dataset when full_refresh=True
        or this is the first run ever (fact_sales doesn't exist yet).
    full_refresh : bool, optional
        When True, replaces fact_sales entirely instead of inserting.

    Returns
    -------
    metadata : dict
        Loading statistics, including:
        - rows_loaded: total rows in fact_sales AFTER this call (the full
          accumulated count, not just this batch)
        - new_rows_inserted: rows actually added by this call
        - watermark_candidate: ISO date string (max Order Date in this
          batch), or None if the batch was empty — the caller only advances
          the stored watermark using this value, and only after this whole
          function returns without raising
    """
    logger.info("Starting load step", extra={"input_rows": len(df), "full_refresh": full_refresh})

    config = _load_config()  # Read paths and table names from config.yaml
    db_path = PROJECT_ROOT / config["paths"]["database"]  # Resolve DuckDB file path
    db_path.parent.mkdir(parents=True, exist_ok=True)  # Create the database directory

    con = duckdb.connect(str(db_path))
    try:
        new_rows_inserted = _write_fact_sales(con, df, full_refresh)

        # Re-read the FULL accumulated fact table — gold aggregations and the
        # Silver/Gold Parquet files must always reflect everything ever
        # loaded, not just this run's new batch (see
        # docs/superpowers/specs/2026-08-13-incremental-load-design.md, §5).
        full_df = con.execute("SELECT * FROM fact_sales").fetchdf()

        # -------------------------------------------------------------------
        # Save the FULL accumulated DataFrame as the silver Parquet file.
        # -------------------------------------------------------------------
        silver_path = PROJECT_ROOT / config["paths"]["silver"]  # Resolve silver layer path
        _save_parquet(full_df, silver_path, label="silver")  # Write to Parquet

        # -------------------------------------------------------------------
        # Build all gold-layer aggregations from the full accumulated data.
        # -------------------------------------------------------------------
        logger.info("Building gold-layer aggregations")

        gold_tables = {
            "agg_sales_by_region": build_sales_by_region(full_df),  # Regional performance
            "agg_sales_by_category": build_sales_by_category(full_df),  # Category / sub-cat performance
            "agg_customer_segments": build_customer_segments(full_df),  # Segment KPIs
            "agg_monthly_trends": build_monthly_trends(full_df),  # Time-series monthly data
            "agg_product_performance": build_product_performance(full_df),  # Product ranking
        }

        # -------------------------------------------------------------------
        # Save each gold aggregation as its own Parquet file.
        # -------------------------------------------------------------------
        gold_paths = config["paths"]["gold"]  # Dict of {table_name: relative_path}

        for table_name, agg_df in gold_tables.items():  # Iterate over each aggregation
            config_key = table_name.replace("agg_", "")  # e.g. 'agg_sales_by_region' -> 'sales_by_region'

            if config_key in gold_paths:  # Only write tables that have a declared output path
                gold_path = PROJECT_ROOT / gold_paths[config_key]  # Resolve the absolute path
                _save_parquet(agg_df, gold_path, label=table_name)  # Write to Parquet

        # -------------------------------------------------------------------
        # Load all gold aggregations into DuckDB (fact_sales already written
        # by _write_fact_sales above).
        # -------------------------------------------------------------------
        for table_name, agg_df in gold_tables.items():
            logger.info(f"Loading {table_name} into DuckDB")
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.register("_agg_df", agg_df)
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _agg_df")
            con.unregister("_agg_df")

        tables = con.execute("SHOW TABLES").fetchdf()
        logger.info("DuckDB tables", extra={"tables": tables["name"].tolist()})
    finally:
        con.close()  # Always close the connection to flush and release the file lock

    # Compute the watermark candidate from THIS batch only (not the full
    # accumulated data) — it represents "the newest Order Date this run
    # actually saw", which is what the caller advances the stored watermark
    # to, once the whole run has succeeded.
    watermark_candidate = None
    if not df.empty:
        max_order_date = df["Order Date"].max()
        if pd.notna(max_order_date):
            watermark_candidate = pd.Timestamp(max_order_date).strftime("%Y-%m-%d")

    metadata = {
        "silver_path": str(silver_path),  # Silver Parquet file path
        "db_path": str(db_path),  # DuckDB file path
        "gold_tables": list(gold_tables.keys()),  # Names of gold tables created
        "rows_loaded": len(full_df),  # Total rows in fact_sales after this call
        "new_rows_inserted": new_rows_inserted,  # Rows actually added by this call
        "watermark_candidate": watermark_candidate,  # New watermark, or None if batch was empty
    }

    logger.info(  # Log the load summary for the pipeline audit trail
        "Load step complete",
        extra={
            "rows_loaded": metadata["rows_loaded"],
            "new_rows_inserted": metadata["new_rows_inserted"],
            "gold_tables": metadata["gold_tables"],
        },
    )

    return metadata  # Return the audit metadata to the orchestrator
