# Architecture

## Overview

The Superstore Sales Data Pipeline implements the **Medallion Architecture** — a layered data organisation pattern used by Databricks, Snowflake, and modern lakehouses. Raw data flows through three quality layers (Bronze → Silver → Gold) before being loaded into DuckDB for analytical querying.

---

## Pipeline DAG

```
[Bronze CSV]
     │
     ▼
[Extract]           src/extract/extractor.py
     │               • Reads CSV with correct encoding (latin-1)
     │               • Enforces dtypes from schema.yaml
     │               • Validates column presence
     ▼
[Quality Check]     src/quality/validators.py
     │               • Schema validation
     │               • Null checks on mandatory columns
     │               • Numeric range checks
     │               • Categorical allowed-value checks
     │               • Duplicate row detection
     ▼
[Clean]             src/transform/cleaner.py
     │               • Parses Order Date / Ship Date to datetime
     │               • Strips whitespace from string columns
     │               • Normalises categoricals to title case
     │               • Standardises Postal Code format
     │               • Removes fully-duplicated rows
     │               • Casts numeric columns to correct dtypes
     ▼
[Quality Check]     src/quality/validators.py  (re-run with date checks)
     │               • Same checks as above
     │               • + Ship Date ≥ Order Date validation
     ▼
[Engineer]          src/transform/feature_engineer.py
     │               • Time features: year, month, quarter, shipping_days
     │               • Financial features: profit_margin_pct, revenue_per_unit
     │               • Categorical features: profit_tier, shipping_speed
     ▼
[Load]              src/load/loader.py
     │               • Writes enriched DataFrame to Silver Parquet
     │               • Builds 5 Gold aggregation tables
     │               • Writes Gold aggregations to Parquet
     │               • Loads all tables into DuckDB
     ▼
[DuckDB + Parquet]
```

---

## Layer Definitions

### Bronze — Raw Ingestion
- **Location:** `data/bronze/sales_data.csv`
- **Contents:** Exact copy of the Kaggle Superstore CSV, never modified
- **Purpose:** Immutable source of truth; allows full re-processing from scratch

### Silver — Cleaned & Enriched
- **Location:** `data/silver/cleaned_sales.parquet`
- **Contents:** All 9,994 rows with cleaned types and 13 derived feature columns
- **Purpose:** Single source of truth for all downstream analytical queries

### Gold — Business Aggregations
- **Location:** `data/gold/*.parquet` and DuckDB tables
- **Contents:** Five pre-aggregated business views

| Table | Rows | Grain |
|---|---|---|
| `agg_sales_by_region` | 4 | One row per US region |
| `agg_sales_by_category` | 17 | One row per Category + Sub-Category |
| `agg_customer_segments` | 3 | One row per customer segment |
| `agg_monthly_trends` | 48 | One row per year-month |
| `agg_product_performance` | 1,850 | One row per product |

---

## Design Decisions

**DuckDB as the analytical store** — Zero-infrastructure OLAP engine that reads Parquet natively and supports full SQL. No server to provision, no credentials to manage. Equivalent to what you'd use Redshift or BigQuery for in a cloud deployment.

**Parquet over CSV** — Columnar binary format that is 5–10× smaller than CSV, preserves dtypes on reload, and enables predicate pushdown for column-selective queries.

**Quality gates before and after transformation** — Running validators twice catches both source data corruption (pre-clean) and transformation bugs (post-clean).

**Separation of concerns** — Each pipeline concern lives in its own module: extraction ≠ cleaning ≠ engineering ≠ loading. This makes each step independently testable and replaceable.

**DAG-style orchestration** — The orchestrator in `pipeline.py` is stateless and idempotent: running it twice produces the same result as running it once.

---

## DuckDB Query Examples

```sql
-- Top 5 most profitable sub-categories
SELECT "Sub-Category", ROUND(total_profit, 2) AS profit
FROM agg_sales_by_category
ORDER BY total_profit DESC
LIMIT 5;

-- Monthly revenue trend for 2017
SELECT order_month_name, total_sales
FROM agg_monthly_trends
WHERE order_year = 2017
ORDER BY order_month;

-- Profit margin by customer segment
SELECT Segment, ROUND(avg_profit_margin, 2) AS margin_pct
FROM agg_customer_segments
ORDER BY avg_profit_margin DESC;

-- Loss-making products (profit_tier = 'Loss')
SELECT "Product Name", total_profit
FROM agg_product_performance
WHERE total_profit < 0
ORDER BY total_profit ASC
LIMIT 10;
```

Connect to the database with:

```python
import duckdb
con = duckdb.connect("database/superstore.duckdb")
df  = con.execute("SELECT * FROM agg_monthly_trends").fetchdf()
```
