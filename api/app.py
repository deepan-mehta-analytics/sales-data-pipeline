# =============================================================================
# api/app.py
# FastAPI query layer for the Superstore Sales Data Pipeline (v1.2).
#
# Exposes the five DuckDB gold tables as typed REST endpoints so downstream
# consumers — dashboards, notebooks, or other services — can query pipeline
# outputs without direct database access.
#
# Endpoints:
#   GET /health            Health check — confirms the API is reachable
#   GET /sales/regions     Regional revenue and profitability KPIs (4 rows)
#   GET /sales/trends      Monthly time-series of sales and volume (48 rows)
#   GET /products/top      Top-N products ranked by total revenue (default 10)
#   GET /segments          Customer segment KPIs (3 rows)
#
# Interactive docs:  http://localhost:8000/docs  (Swagger UI)
# OpenAPI schema:    http://localhost:8000/openapi.json
# =============================================================================

from typing import List, Optional  # Type hints for route return types and optional params

import duckdb  # DuckDB connection type used in route signatures
from fastapi import Depends, FastAPI, Query  # Core FastAPI building blocks

from api.database import get_db  # DuckDB read-only connection dependency
from api.models import (  # Pydantic response schemas for each endpoint
    CustomerSegment,
    MonthlyTrend,
    ProductPerformance,
    RegionSales,
)

# ── Application instance ──────────────────────────────────────────────────────
app = FastAPI(  # Create the FastAPI application
    title="Superstore Sales API",  # Title shown in the Swagger UI header
    description=(  # Multi-line description rendered in the OpenAPI docs
        "REST query layer exposing the DuckDB gold tables produced by the "
        "Superstore Sales ETL pipeline. Run the pipeline before starting the API."
    ),
    version="1.2.0",  # API version surfaced in the OpenAPI schema
)


# =============================================================================
# Health check
# =============================================================================


@app.get("/health", tags=["Meta"])  # Lightweight endpoint for load-balancer and CI smoke tests
def health_check() -> dict:
    """Return a simple liveness confirmation with the current API version."""
    return {"status": "ok", "version": "1.2.0"}  # Fixed response — no DB query needed


# =============================================================================
# /sales/regions
# =============================================================================


@app.get(  # Register the GET route
    "/sales/regions",  # URL path
    response_model=List[RegionSales],  # Pydantic schema applied to every item in the list
    tags=["Sales"],  # Groups this endpoint under "Sales" in the Swagger UI
    summary="Regional sales and profitability KPIs",  # Short description in the docs
)
def get_sales_by_region(
    db: duckdb.DuckDBPyConnection = Depends(get_db),  # Inject read-only DuckDB connection
) -> List[dict]:
    """
    Return one row per US sales region with revenue and profitability metrics,
    ordered by total sales descending.
    """
    rows = db.execute(  # Run the aggregation query against the gold table
        """
        SELECT
            Region            AS region,             -- normalise to snake_case for JSON
            total_sales,                              -- aggregate revenue
            total_profit,                             -- aggregate net profit
            total_orders,                             -- distinct order count
            avg_discount,                             -- mean discount rate
            avg_profit_margin                         -- mean profit margin %
        FROM  agg_sales_by_region
        ORDER BY total_sales DESC                     -- most valuable region first
        """
    ).fetchdf()  # Fetch as pandas DataFrame for easy serialisation

    return rows.to_dict(orient="records")  # Convert to list of dicts for Pydantic to validate


# =============================================================================
# /sales/trends
# =============================================================================


@app.get(
    "/sales/trends",
    response_model=List[MonthlyTrend],
    tags=["Sales"],
    summary="Monthly time-series of sales, profit, and order volume",
)
def get_monthly_trends(
    year: Optional[int] = Query(  # Optional query parameter — filter by a specific year
        None,
        description="Filter to a single year (e.g. ?year=2017). Omit for all years.",
        ge=2014,  # Earliest year in the dataset
        le=2017,  # Latest year in the dataset
    ),
    db: duckdb.DuckDBPyConnection = Depends(get_db),  # Injected DuckDB connection
) -> List[dict]:
    """
    Return monthly sales and volume metrics ordered chronologically.
    Pass `?year=<YYYY>` to restrict to a single year (2014 – 2017).
    """
    if year is not None:  # Year filter provided — use a parameterised WHERE clause
        rows = db.execute(
            """
            SELECT
                order_year,
                order_month,
                order_month_name,
                total_sales,
                total_profit,
                total_orders,
                total_units
            FROM  agg_monthly_trends
            WHERE order_year = ?                      -- bind the year parameter safely
            ORDER BY order_year, order_month          -- chronological order
            """,
            [year],  # Pass as a list to avoid SQL injection
        ).fetchdf()
    else:  # No filter — return all 48 months
        rows = db.execute("""
            SELECT
                order_year,
                order_month,
                order_month_name,
                total_sales,
                total_profit,
                total_orders,
                total_units
            FROM  agg_monthly_trends
            ORDER BY order_year, order_month          -- chronological order across all years
            """).fetchdf()

    return rows.to_dict(orient="records")  # Serialise to list of dicts


# =============================================================================
# /products/top
# =============================================================================


@app.get(
    "/products/top",
    response_model=List[ProductPerformance],
    tags=["Products"],
    summary="Top products by total sales revenue",
)
def get_top_products(
    limit: int = Query(  # Optional limit parameter with validation bounds
        10,  # Default: top 10
        ge=1,  # Minimum: 1 product
        le=100,  # Maximum: 100 products per call (prevent unbounded responses)
        description="Number of top products to return (1 – 100, default 10).",
    ),
    db: duckdb.DuckDBPyConnection = Depends(get_db),  # Injected DuckDB connection
) -> List[dict]:
    """
    Return the top-N products ranked by lifetime total sales revenue.
    Use `?limit=N` to control how many results are returned (default 10).
    """
    rows = db.execute(  # Parameterised LIMIT to avoid injection and honour the bound
        """
        SELECT
            "Product ID"    AS product_id,            -- alias space-containing col name
            "Product Name"  AS product_name,          -- alias space-containing col name
            Category        AS category,              -- normalise case
            "Sub-Category"  AS sub_category,          -- alias hyphen-containing col name
            total_sales,                              -- lifetime revenue
            total_profit,                             -- lifetime net profit
            total_orders,                             -- times product appeared in an order
            avg_profit_margin                         -- mean margin %
        FROM  agg_product_performance
        ORDER BY total_sales DESC                     -- highest-revenue product first
        LIMIT ?                                       -- parameterised limit (safe)
        """,
        [limit],  # Bind limit as a parameter
    ).fetchdf()

    return rows.to_dict(orient="records")  # Serialise to list of dicts


# =============================================================================
# /segments
# =============================================================================


@app.get(
    "/segments",
    response_model=List[CustomerSegment],
    tags=["Customers"],
    summary="Customer segment revenue and profitability KPIs",
)
def get_customer_segments(
    db: duckdb.DuckDBPyConnection = Depends(get_db),  # Injected DuckDB connection
) -> List[dict]:
    """
    Return one row per customer segment (Consumer / Corporate / Home Office)
    with acquisition count, revenue, and profitability metrics.
    """
    rows = db.execute("""
        SELECT
            Segment         AS segment,               -- normalise case
            total_customers,                          -- unique customer count
            total_orders,                             -- total distinct orders
            total_sales,                              -- aggregate revenue
            total_profit,                             -- aggregate net profit
            avg_order_value,                          -- mean line-item revenue
            avg_profit_margin                         -- mean margin %
        FROM  agg_customer_segments
        ORDER BY total_sales DESC                     -- most valuable segment first
        """).fetchdf()

    return rows.to_dict(orient="records")  # Serialise to list of dicts
