# =============================================================================
# tests/integration/test_api.py
# Smoke tests for the FastAPI query layer (v1.2).
#
# Strategy:
#   - Each test uses FastAPI's built-in TestClient (Starlette wrapper around
#     httpx) — no real HTTP server is started; requests are handled in-process.
#   - A lightweight test DuckDB is created in a tmp_path fixture with a small
#     set of realistic rows per gold table.  The real superstore.duckdb is
#     never touched during tests.
#   - The `get_db` dependency is overridden via FastAPI's dependency_overrides
#     mechanism so the routes query the test database.
#   - The override is cleared after every test via fixture teardown.
#
# Coverage:
#   - /health                        — 200 + body shape
#   - /sales/regions                 — 200, row count, field presence
#   - /sales/trends (all years)      — 200, row count, chronological order
#   - /sales/trends?year=2017        — 200, filter applied correctly
#   - /sales/trends?year=9999        — 200, empty list (no rows match)
#   - /products/top (default limit)  — 200, default limit honoured
#   - /products/top?limit=2          — 200, custom limit honoured
#   - /products/top?limit=200        — 422, limit > max bound rejected
#   - /segments                      — 200, row count, field presence
# =============================================================================

from pathlib import Path  # Path type hint for the test_db fixture
from typing import Generator  # Type hint for the get_db override generator

import duckdb  # Used to create and populate the test DuckDB
import pytest  # Test framework and fixture engine
from fastapi.testclient import TestClient  # Starlette test client wrapping httpx

from api.app import app  # FastAPI application instance
from api.database import get_db  # Dependency to override in tests

# =============================================================================
# Test database fixture
# =============================================================================


@pytest.fixture(scope="module")  # Module scope: create the DB once for all tests in this file
def test_db(tmp_path_factory) -> Path:
    """
    Build a temporary DuckDB with minimal gold-table data.

    Uses tmp_path_factory (module-scoped alternative to tmp_path) so the same
    database file is reused across all tests in this module.
    """
    db_dir = tmp_path_factory.mktemp("db")  # Create a temporary directory for this module
    db_path = db_dir / "test_superstore.duckdb"  # Full path to the test database file

    con = duckdb.connect(str(db_path))  # Open a writable connection to create the schema

    # ── agg_sales_by_region ──────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE agg_sales_by_region (
            Region            VARCHAR,   -- sales region name
            total_sales       DOUBLE,    -- aggregate revenue
            total_profit      DOUBLE,    -- aggregate net profit
            total_orders      INTEGER,   -- distinct order count
            total_rows        INTEGER,   -- total line items
            avg_discount      DOUBLE,    -- mean discount rate
            avg_profit_margin DOUBLE     -- mean profit margin %
        )
        """)  # Create the region aggregation table

    con.execute("""
        INSERT INTO agg_sales_by_region VALUES
            ('West',    725457.82,  108418.45, 3203, 9994, 0.1440,  21.95),
            ('East',    678781.24,   91522.78, 2848, 9994, 0.1535,  20.14),
            ('Central', 501239.89,  -52196.38, 2323, 9994, 0.2219, -10.41),
            ('South',   391721.91,   46749.43, 1615, 9994, 0.1463,  16.82)
        """)  # Insert four region rows matching the real dataset's structure

    # ── agg_monthly_trends ───────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE agg_monthly_trends (
            order_year       INTEGER,  -- four-digit year
            order_month      INTEGER,  -- month number 1-12
            order_month_name VARCHAR,  -- month name string
            total_sales      DOUBLE,   -- monthly revenue total
            total_profit     DOUBLE,   -- monthly profit total
            total_orders     INTEGER,  -- distinct orders this month
            total_units      INTEGER   -- total units sold this month
        )
        """)  # Create the monthly time-series table

    con.execute("""
        INSERT INTO agg_monthly_trends VALUES
            (2016, 11, 'November', 71395.46,  9215.32, 318, 789),
            (2016, 12, 'December', 54201.11,  6843.90, 249, 612),
            (2017, 11, 'November', 118447.83, 15023.54, 531, 1324),
            (2017, 12, 'December', 96999.32,  14039.26, 492, 1211)
        """)  # Insert sample rows spanning two years for year-filter tests

    # ── agg_product_performance ──────────────────────────────────────────────
    con.execute("""
        CREATE TABLE agg_product_performance (
            "Product ID"      VARCHAR,  -- unique product identifier
            "Product Name"    VARCHAR,  -- full product display name
            Category          VARCHAR,  -- top-level category
            "Sub-Category"    VARCHAR,  -- sub-category
            total_sales       DOUBLE,   -- lifetime revenue
            total_profit      DOUBLE,   -- lifetime net profit
            total_units       INTEGER,  -- total units sold
            total_orders      INTEGER,  -- order appearances
            avg_discount      DOUBLE,   -- mean discount rate
            avg_profit_margin DOUBLE    -- mean margin %
        )
        """)  # Create the product performance table

    con.execute("""
        INSERT INTO agg_product_performance VALUES
            ('TEC-MA-10002412', 'Canon imageCLASS 2200',   'Technology',      'Machines',  61599.82, 25199.83, 5, 8, 0.0, 40.91),
            ('TEC-CO-10004722', 'Fellowes PB500',           'Technology',      'Copiers',   55617.82, 17999.64, 7, 8, 0.0, 32.37),
            ('OFF-BI-10003656', 'GBC DocuBind TL300',       'Office Supplies', 'Binders',    4404.57,  1453.51, 6, 6, 0.1, 33.00),
            ('FUR-BO-10001798', 'Bush Somerset Collection', 'Furniture',       'Bookcases',  4099.98,   457.81, 3, 4, 0.0, 11.17),
            ('OFF-EN-10001500', 'Kraft Clasp Envelopes',    'Office Supplies', 'Envelopes',   499.58,   162.87, 4, 3, 0.0, 32.61)
        """)  # Insert 5 products — enough to test default limit (10) and custom limit (2)

    # ── agg_customer_segments ────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE agg_customer_segments (
            Segment           VARCHAR,  -- customer segment name
            total_customers   INTEGER,  -- unique customer count
            total_orders      INTEGER,  -- distinct orders placed
            total_sales       DOUBLE,   -- aggregate revenue
            total_profit      DOUBLE,   -- aggregate net profit
            avg_order_value   DOUBLE,   -- mean line-item revenue
            avg_profit_margin DOUBLE    -- mean margin %
        )
        """)  # Create the customer segments table

    con.execute("""
        INSERT INTO agg_customer_segments VALUES
            ('Consumer',    410, 5191, 1161401.35, 134119.21, 223.74, 11.55),
            ('Corporate',   236, 3020,  706146.37,  91979.13, 233.82, 13.02),
            ('Home Office', 146, 1783,  429653.15,  60298.68, 241.03, 14.03)
        """)  # Insert three segment rows matching the real dataset's structure

    con.close()  # Close the writable connection before tests open read-only connections

    return db_path  # Return the path so the client fixture can inject it


# =============================================================================
# TestClient fixture with dependency override
# =============================================================================


@pytest.fixture(scope="module")  # Module scope: reuse the same client across all tests
def client(test_db: Path) -> Generator:
    """
    Return a TestClient wired to the test DuckDB via dependency override.

    FastAPI's dependency_overrides replaces get_db with a function that opens
    the test database, so no real superstore.duckdb is accessed during tests.
    """

    def override_get_db() -> Generator:  # Replacement dependency pointing at the test DB
        con = duckdb.connect(str(test_db), read_only=True)  # Open the test DuckDB read-only
        try:
            yield con  # Yield the connection to the route handler
        finally:
            con.close()  # Always close after each request

    app.dependency_overrides[get_db] = override_get_db  # Register the override globally
    yield TestClient(app)  # Yield the configured client to each test
    app.dependency_overrides.clear()  # Remove override after all tests in the module finish


# =============================================================================
# /health
# =============================================================================


def test_health_returns_200(client: TestClient) -> None:
    """Health endpoint must return 200 with status 'ok'."""
    response = client.get("/health")  # Call the health endpoint
    assert response.status_code == 200  # Confirm the response is successful
    body = response.json()  # Parse the JSON body
    assert body["status"] == "ok"  # Confirm the status field value
    assert body["version"] == "1.2.0"  # Confirm the version is current


# =============================================================================
# /sales/regions
# =============================================================================


def test_regions_returns_four_rows(client: TestClient) -> None:
    """Regions endpoint must return all four US sales regions."""
    response = client.get("/sales/regions")  # Call the regions endpoint
    assert response.status_code == 200  # Confirm success
    data = response.json()  # Parse response body
    assert len(data) == 4  # Expect exactly 4 rows (one per US region)


def test_regions_ordered_by_sales_desc(client: TestClient) -> None:
    """Regions must be ordered by total_sales descending (West first)."""
    response = client.get("/sales/regions")  # Call the endpoint
    data = response.json()  # Parse response
    sales = [row["total_sales"] for row in data]  # Extract the total_sales column
    assert sales == sorted(sales, reverse=True)  # Verify descending order


def test_regions_contains_required_fields(client: TestClient) -> None:
    """Each region row must include all fields defined in RegionSales."""
    response = client.get("/sales/regions")  # Call the endpoint
    row = response.json()[0]  # Inspect the first row
    required = {"region", "total_sales", "total_profit", "total_orders", "avg_discount", "avg_profit_margin"}
    assert required.issubset(row.keys())  # Confirm all required fields are present


# =============================================================================
# /sales/trends
# =============================================================================


def test_trends_all_years_returns_four_rows(client: TestClient) -> None:
    """Without year filter, trends must return all 4 test rows (2016-2017)."""
    response = client.get("/sales/trends")  # Call without year param
    assert response.status_code == 200  # Confirm success
    assert len(response.json()) == 4  # Expect all 4 rows inserted in the fixture


def test_trends_year_filter_applied(client: TestClient) -> None:
    """Year filter must return only rows matching the requested year."""
    response = client.get("/sales/trends?year=2017")  # Request only 2017 data
    assert response.status_code == 200  # Confirm success
    data = response.json()  # Parse response
    assert len(data) == 2  # Expect 2 rows (November and December 2017)
    assert all(row["order_year"] == 2017 for row in data)  # All returned rows must be 2017


def test_trends_unknown_year_returns_empty(client: TestClient) -> None:
    """A valid but unmatched year must return an empty list (not an error)."""
    response = client.get("/sales/trends?year=2015")  # No 2015 rows in test data
    assert response.status_code == 200  # Must not raise a 404 or 500
    assert response.json() == []  # Expect an empty list


def test_trends_ordered_chronologically(client: TestClient) -> None:
    """Trends must be returned in ascending year-month order."""
    response = client.get("/sales/trends")  # Call without filter
    data = response.json()  # Parse response
    keys = [(row["order_year"], row["order_month"]) for row in data]  # Extract year-month tuples
    assert keys == sorted(keys)  # Confirm ascending chronological order


# =============================================================================
# /products/top
# =============================================================================


def test_products_default_limit(client: TestClient) -> None:
    """Default limit of 10 must return all 5 available test products."""
    response = client.get("/products/top")  # Call with default limit
    assert response.status_code == 200  # Confirm success
    assert len(response.json()) == 5  # Only 5 products in test DB — all returned


def test_products_custom_limit(client: TestClient) -> None:
    """Custom limit of 2 must return exactly 2 products."""
    response = client.get("/products/top?limit=2")  # Request top 2
    assert response.status_code == 200  # Confirm success
    assert len(response.json()) == 2  # Exactly 2 rows returned


def test_products_limit_too_large_rejected(client: TestClient) -> None:
    """A limit exceeding the maximum bound (100) must return 422 Unprocessable Entity."""
    response = client.get("/products/top?limit=200")  # Exceeds the ge/le bounds
    assert response.status_code == 422  # FastAPI rejects out-of-bound query params


def test_products_contains_required_fields(client: TestClient) -> None:
    """Each product row must include all fields defined in ProductPerformance."""
    response = client.get("/products/top")  # Call the endpoint
    row = response.json()[0]  # Inspect the first (top) product
    required = {
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "total_sales",
        "total_profit",
        "total_orders",
        "avg_profit_margin",
    }
    assert required.issubset(row.keys())  # All required fields must be present


def test_products_ordered_by_sales_desc(client: TestClient) -> None:
    """Products must be ordered by total_sales descending."""
    response = client.get("/products/top")  # Call the endpoint
    data = response.json()  # Parse response
    sales = [row["total_sales"] for row in data]  # Extract the total_sales column
    assert sales == sorted(sales, reverse=True)  # Verify descending order


# =============================================================================
# /segments
# =============================================================================


def test_segments_returns_three_rows(client: TestClient) -> None:
    """Segments endpoint must return all three customer segments."""
    response = client.get("/segments")  # Call the segments endpoint
    assert response.status_code == 200  # Confirm success
    assert len(response.json()) == 3  # Expect Consumer / Corporate / Home Office


def test_segments_contains_required_fields(client: TestClient) -> None:
    """Each segment row must include all fields defined in CustomerSegment."""
    response = client.get("/segments")  # Call the endpoint
    row = response.json()[0]  # Inspect the first row
    required = {
        "segment",
        "total_customers",
        "total_orders",
        "total_sales",
        "total_profit",
        "avg_order_value",
        "avg_profit_margin",
    }
    assert required.issubset(row.keys())  # All required fields must be present
