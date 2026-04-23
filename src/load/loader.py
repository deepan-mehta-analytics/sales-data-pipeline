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

import pandas as pd  # Core data-manipulation library
import duckdb  # Embedded OLAP analytical database
import yaml  # Reads config.yaml for paths and table names
from pathlib import Path  # Cross-platform path resolution and directory creation
from typing import Dict  # Type hint

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
# DuckDB loader
# =============================================================================


def _load_to_duckdb(
    silver_df: pd.DataFrame,
    gold_tables: Dict[str, pd.DataFrame],
    db_path: Path,
) -> None:
    """
    Create (or replace) all tables in the DuckDB analytical database.

    The database file is created automatically if it does not exist.
    All tables are replaced on each pipeline run to ensure idempotency —
    running the pipeline twice produces the same result as running it once.

    Parameters
    ----------
    silver_df   : pd.DataFrame             Enriched silver DataFrame (fact table).
    gold_tables : dict[str, pd.DataFrame]  Gold aggregation DataFrames by table name.
    db_path     : Path                     Absolute path to the .duckdb file.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)  # Create the database directory

    # Connect to DuckDB, creating the file if it does not exist.
    # The connection is used as a context manager so it closes cleanly on exit.
    con = duckdb.connect(str(db_path))

    try:
        # -------------------------------------------------------------------
        # Fact table: fact_sales
        # The full enriched silver DataFrame is loaded as the central fact
        # table.  Downstream gold views / tables are built from this.
        # -------------------------------------------------------------------
        logger.info("Loading fact_sales into DuckDB")

        # DROP TABLE IF EXISTS prevents errors on re-runs.
        con.execute("DROP TABLE IF EXISTS fact_sales")

        # Register the DataFrame as a DuckDB virtual table, then create a
        # permanent table from it.  This is more reliable than relying on
        # DuckDB's automatic DataFrame detection.
        con.register("_silver_df", silver_df)  # Expose DataFrame to DuckDB's SQL engine
        con.execute("CREATE TABLE fact_sales AS SELECT * FROM _silver_df")  # Materialise as permanent table
        con.unregister("_silver_df")  # Remove the temporary view

        logger.info(
            "fact_sales loaded",
            extra={"rows": len(silver_df), "columns": len(silver_df.columns)},
        )

        # -------------------------------------------------------------------
        # Gold tables: one table per aggregation
        # -------------------------------------------------------------------
        for table_name, agg_df in gold_tables.items():  # Iterate over each gold aggregation
            logger.info(f"Loading {table_name} into DuckDB")

            con.execute(f"DROP TABLE IF EXISTS {table_name}")  # Drop old version if present

            con.register("_agg_df", agg_df)  # Expose the aggregation to DuckDB SQL
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM _agg_df")  # Materialise
            con.unregister("_agg_df")  # Remove the temporary view

            logger.info(
                f"{table_name} loaded",
                extra={"rows": len(agg_df), "columns": len(agg_df.columns)},
            )

        # Verify the tables exist by listing them.
        tables = con.execute("SHOW TABLES").fetchdf()  # Query the DuckDB catalogue
        logger.info("DuckDB tables", extra={"tables": tables["name"].tolist()})

    finally:
        con.close()  # Always close the connection to flush and release the file lock


# =============================================================================
# load  –  Public entry point
# =============================================================================


def load(df: pd.DataFrame) -> dict:
    """
    Execute the full loading pipeline: silver Parquet → gold Parquets → DuckDB.

    Parameters
    ----------
    df : pd.DataFrame
        Enriched silver-layer DataFrame (output of feature_engineer.engineer()).

    Returns
    -------
    metadata : dict
        Loading statistics including paths written and DuckDB table names.
    """
    logger.info("Starting load step", extra={"input_rows": len(df)})

    config = _load_config()  # Read paths and table names from config.yaml

    # -----------------------------------------------------------------------
    # 1. Save the enriched DataFrame as the silver Parquet file.
    # -----------------------------------------------------------------------
    silver_path = PROJECT_ROOT / config["paths"]["silver"]  # Resolve silver layer path
    _save_parquet(df, silver_path, label="silver")  # Write to Parquet

    # -----------------------------------------------------------------------
    # 2. Build all gold-layer aggregations.
    # -----------------------------------------------------------------------
    logger.info("Building gold-layer aggregations")

    gold_tables = {
        "agg_sales_by_region": build_sales_by_region(df),  # Regional performance
        "agg_sales_by_category": build_sales_by_category(df),  # Category / sub-cat performance
        "agg_customer_segments": build_customer_segments(df),  # Segment KPIs
        "agg_monthly_trends": build_monthly_trends(df),  # Time-series monthly data
        "agg_product_performance": build_product_performance(df),  # Product ranking
    }

    # -----------------------------------------------------------------------
    # 3. Save each gold aggregation as its own Parquet file.
    # -----------------------------------------------------------------------
    gold_paths = config["paths"]["gold"]  # Dict of {table_name: relative_path}

    for table_name, agg_df in gold_tables.items():  # Iterate over each aggregation
        # Derive the config key from the table name (strip 'agg_' prefix).
        config_key = table_name.replace("agg_", "")  # e.g. 'agg_sales_by_region' → 'sales_by_region'

        if config_key in gold_paths:  # Only write tables that have a declared output path
            gold_path = PROJECT_ROOT / gold_paths[config_key]  # Resolve the absolute path
            _save_parquet(agg_df, gold_path, label=table_name)  # Write to Parquet

    # -----------------------------------------------------------------------
    # 4. Load silver fact table and all gold aggregations into DuckDB.
    # -----------------------------------------------------------------------
    db_path = PROJECT_ROOT / config["paths"]["database"]  # Resolve DuckDB file path
    _load_to_duckdb(df, gold_tables, db_path)  # Execute the DuckDB load

    # Build a metadata dict summarising what the load step produced.
    metadata = {
        "silver_path": str(silver_path),  # Silver Parquet file path
        "db_path": str(db_path),  # DuckDB file path
        "gold_tables": list(gold_tables.keys()),  # Names of gold tables created
        "rows_loaded": len(df),  # Total rows in the fact table
    }

    logger.info(  # Log the load summary for the pipeline audit trail
        "Load step complete",
        extra={
            "rows_loaded": metadata["rows_loaded"],
            "gold_tables": metadata["gold_tables"],
        },
    )

    return metadata  # Return the audit metadata to the orchestrator
