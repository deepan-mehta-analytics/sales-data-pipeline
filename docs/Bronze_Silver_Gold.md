# Bronze → Silver → Gold: Medallion Architecture in This Pipeline

This document explains how the Medallion Architecture (Bronze → Silver → Gold) is actually
implemented in `sales-data-pipeline` — the specific modules, file paths, and tables involved at
each layer. For general background on the pattern, see the [Databricks reference
docs](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion); everything below
describes this repo's own implementation, not the generic concept.

Every layer is produced by one pipeline run — `python orchestration/pipeline.py` (or `make run`,
or the self-hosted Airflow DAG) — which chains extraction, cleaning, feature engineering, and
loading into a single DAG-style flow:

```
[Extract]  ──►  [Quality Check (raw)]  ──►  [Clean]
    ──►  [Quality Check (cleaned)]  ──►  [Engineer]  ──►  [Load]
```

---

## 🟤 Bronze Layer — Raw Ingestion

**Module:** `src/extract/extractor.py`
**Storage:** `data/bronze/sales_data.csv`

The Bronze layer is the unmodified Kaggle Superstore CSV. `extract()`:

- Reads the CSV with the encoding, separator, and date format declared in `config/config.yaml`
  (`latin-1`, comma-delimited).
- Applies dtypes from `config/schema.yaml` at parse time (e.g. `Postal Code` forced to string to
  preserve leading zeros; `Order Date`/`Ship Date` kept as strings — parsing happens in Silver).
- Validates that every column declared in `schema.yaml` is present, raising `ValueError` if the
  file doesn't match the expected shape.
- Returns the raw `DataFrame` plus a metadata dict (row/column counts, source path) for the run
  report — no cleaning or transformation happens at this stage.

Immediately after extraction, `orchestration/pipeline.py` runs `run_quality_checks()` on the raw
data (`src/quality/validators.py`) with date-order checks skipped, since dates haven't been parsed
yet. A failed check aborts the run if `fail_on_quality_error: true` in `config.yaml`.

---

## ⚪ Silver Layer — Cleaned, Validated, Enriched

**Modules:** `src/transform/cleaner.py`, `src/transform/feature_engineer.py`
**Storage:** `data/silver/cleaned_sales.parquet` · DuckDB table `fact_sales`

Silver is built in two steps, both wired into `orchestration/pipeline.py`:

**1. Clean (`cleaner.py`)**
- `strip_whitespace()` — trims leading/trailing whitespace from every string column.
- `parse_dates()` — converts `Order Date`/`Ship Date` from strings to real `datetime` using the
  format in `config.yaml`.
- Deduplicates fully-identical rows (`drop_duplicates()`).

After cleaning, `run_quality_checks()` runs again — this time with date-order checks enabled
(`Ship Date >= Order Date`), since dates are now real `datetime` values.

**2. Engineer (`feature_engineer.py`)** — adds 13 derived columns on top of the cleaned data:

| Column | Description |
|---|---|
| `order_year`, `order_month`, `order_month_name`, `order_quarter`, `order_day_of_week` | Calendar breakdown of `Order Date` |
| `shipping_days` | Days elapsed between order and shipment |
| `profit_margin_pct` | Profit as a percentage of sales revenue |
| `discount_amount` | Absolute discount value (`Sales × Discount`) |
| `revenue_per_unit`, `profit_per_unit` | Revenue/profit normalised by `Quantity` |
| `is_profitable` | Boolean — `Profit > 0` |
| `profit_tier` | Bucketed: `Loss` / `Low` / `Medium` / `High` from `profit_margin_pct` |
| `shipping_speed` | Bucketed: `Same Day/Overnight` / `Express` / `Standard` / `Slow` from `shipping_days` |

The result is the enriched Silver `DataFrame` — written to `data/silver/cleaned_sales.parquet` by
`src/load/loader.py` and loaded as the `fact_sales` table in the embedded DuckDB database
(`database/superstore.duckdb`). This fact table is the single source of truth every Gold
aggregation and the FastAPI query layer read from.

---

## 🟡 Gold Layer — Business-Ready Aggregations

**Module:** `src/load/loader.py`
**Storage:** `data/gold/*.parquet` · 5 DuckDB tables · BigQuery (v2.0, synced from Airflow only)

`load()` builds five aggregation tables directly from the enriched Silver `DataFrame`, writes each
as its own Parquet file, and materialises all of them as DuckDB tables (dropped and recreated on
every run, so re-runs are idempotent):

| Gold table | Builder function | Grain |
|---|---|---|
| `agg_sales_by_region` | `build_sales_by_region()` | One row per US region |
| `agg_sales_by_category` | `build_sales_by_category()` | One row per Category / Sub-Category |
| `agg_customer_segments` | `build_customer_segments()` | One row per customer Segment |
| `agg_monthly_trends` | `build_monthly_trends()` | One row per year-month (time series) |
| `agg_product_performance` | `build_product_performance()` | One row per unique product, ranked by revenue |

### v2.0 — Gold → BigQuery sync

`src/load/bigquery_loader.py` reads the same Gold Parquet files (never recomputes an aggregation)
and loads them into a partitioned/clustered BigQuery dataset — a cloud analytical store that sits
alongside the local DuckDB store, not a replacement for it. This sync only runs from the
self-hosted Airflow DAG's `load_bigquery` task (`dags/sales_pipeline_dag.py`); it is never invoked
from `orchestration/pipeline.py` or the GitHub Actions daily cron. See
`docs/superpowers/specs/2026-08-09-bigquery-warehouse-design.md` for the full design rationale
(table layout, partitioning/clustering choice, credential handling, cost pre-mortem).

---

## File Reference

| Layer | Stage | Module | Output |
|---|---|---|---|
| 🟤 Bronze | Extract | `src/extract/extractor.py` | `data/bronze/sales_data.csv` (source, read-only) |
| ⚪ Silver | Clean | `src/transform/cleaner.py` | Cleaned `DataFrame` (in-memory) |
| ⚪ Silver | Engineer | `src/transform/feature_engineer.py` | `data/silver/cleaned_sales.parquet` · `fact_sales` |
| 🟡 Gold | Load | `src/load/loader.py` | `data/gold/*.parquet` · 5 `agg_*` DuckDB tables |
| 🟡 Gold (v2.0) | BigQuery sync | `src/load/bigquery_loader.py` | BigQuery `fact_sales` + 5 `agg_*` tables (Airflow only) |

Orchestrated end-to-end by `orchestration/pipeline.py` (also wired into the Airflow DAG at
`dags/sales_pipeline_dag.py`), with quality gates from `src/quality/validators.py` enforced after
both the Bronze and Silver stages.
