# PROJECT-STATUS.md — Superstore Sales Data Pipeline

> **Last updated:** 2026-08-09  
> **Maintainer:** Deepan Mehta  
> **Repo:** `deepan-mehta-analytics/sales-data-pipeline`

---

## 🟢 Overall Status

| Field | Value |
|---|---|
| **Version** | 1.2.2 |
| **GitHub Release** | [v1.2.2 — CI Coverage Fix](https://github.com/deepan-mehta-analytics/sales-data-pipeline/releases/tag/v1.2.2) |
| **Phase** | v2.0 in progress — Airflow DAG shipped; BigQuery, incremental load, MLflow tracking pending |
| **Latest commit** | `cf4ce5ca` — `fix(feature-engineer): cast Quantity once for both np.where operands` |
| **Branch** | `main` |
| **CI** | GitHub Actions — Lint + Test + Coverage on every push |
| **Scheduled pipeline** | Daily via `.github/workflows/pipeline.yml` |

---

## 📦 Pipeline Phase Tracker

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 — Extract | Bronze CSV ingestion + schema validation | ✅ Done | `src/extract/extractor.py` |
| 2 — Quality (raw) | 5 checks on raw DataFrame before any transform | ✅ Done | `src/quality/validators.py` |
| 3 — Clean | Date parsing, whitespace, dedup, postal codes, dtype casting | ✅ Done | `src/transform/cleaner.py` |
| 4 — Quality (cleaned) | 6 checks including `ship_after_order` temporal rule | ✅ Done | `src/quality/validators.py` |
| 5 — Feature Engineer | 13 derived columns (time, financial, categorical) | ✅ Done | `src/transform/feature_engineer.py` |
| 6 — Load | Silver Parquet + 5 Gold Parquets + DuckDB | ✅ Done | `src/load/loader.py` |
| 7 — Orchestrate | DAG-style orchestrator with stage timing and quality gates | ✅ Done | `orchestration/pipeline.py` |
| 8 — CI/CD | Lint + unit + integration on push; daily scheduled run | ✅ Done | `.github/workflows/` |
| 9 — Docker | Multi-stage container build + Compose | ✅ Done | `Dockerfile`, `docker-compose.yml` |
| 10 — Documentation | README, architecture.md, data_dictionary.md, evidence screenshots | ✅ Done | `docs/`, `README.md` |

---

## 📊 Pipeline Execution Metrics

| Metric | Value |
|---|---|
| Source rows (Superstore CSV) | 9,994 |
| Base columns | 21 |
| Derived feature columns | 13 |
| Total columns (Silver layer) | 34 |
| End-to-end runtime | ~617 ms |
| Quality checks (raw stage) | 5 / 5 passed |
| Quality checks (cleaned stage) | 6 / 6 passed |
| Duplicate rows removed | 0 |

---

## 🗄️ Data Artifact Inventory

### Bronze layer — `data/bronze/`

| File | Format | Description |
|---|---|---|
| `sales_data.csv` | CSV (latin-1) | Raw Kaggle Superstore dataset — 9,994 rows, 21 columns |

### Silver layer — `data/silver/`

| File | Format | Rows | Description |
|---|---|---|---|
| `cleaned_sales.parquet` | Parquet (Snappy) | 9,994 | Cleaned + 13 derived feature columns — 34 total columns |

### Gold layer — `data/gold/`

| File | Format | Rows | Key Metrics |
|---|---|---|---|
| `sales_by_region.parquet` | Parquet | 4 | total_sales, total_profit, avg_profit_margin by region |
| `sales_by_category.parquet` | Parquet | 17 | total_sales, total_profit, avg_discount by category/sub-cat |
| `customer_segments.parquet` | Parquet | 3 | total_customers, avg_order_value by segment |
| `monthly_trends.parquet` | Parquet | 48 | Monthly time-series: sales, profit, orders, units (Jan 2014 – Dec 2017) |
| `product_performance.parquet` | Parquet | 1,894 | Per-SKU: total_sales, total_profit, avg_margin |

---

## 🔵 DuckDB Analytical Store — `database/superstore.duckdb`

| Table | Type | Rows | Description |
|---|---|---|---|
| `fact_sales` | Fact | 9,994 | Full enriched silver DataFrame — central query table |
| `agg_sales_by_region` | Aggregation | 4 | Regional KPIs |
| `agg_sales_by_category` | Aggregation | 17 | Category / Sub-Category KPIs |
| `agg_customer_segments` | Aggregation | 3 | Segment KPIs |
| `agg_monthly_trends` | Aggregation | 48 | Monthly time-series |
| `agg_product_performance` | Aggregation | 1,894 | Product-level ranking |

**Key analytical finding:** Central region operating at **-10.41% profit margin** despite $501K revenue. West region leads both revenue and profitability at **+21.95% margin**.

---

## 🧪 Test Suite

| File | Type | Tests | Coverage scope |
|---|---|---|---|
| `tests/unit/test_cleaner.py` | Unit | 23 | `src/transform/cleaner.py` |
| `tests/unit/test_feature_engineer.py` | Unit | 22 | `src/transform/feature_engineer.py` |
| `tests/unit/test_validators.py` | Unit | 20 | `src/quality/validators.py` |
| `tests/unit/test_extractor.py` | Unit | 16 | `src/extract/extractor.py` |
| `tests/unit/test_path_utils.py` | Unit | 7 | `dags/path_utils.py` (v2.0) |
| `tests/integration/test_pipeline.py` | Integration | 12 | Full end-to-end pipeline |
| `tests/integration/test_api.py` | Integration | 15 | FastAPI query layer smoke tests |
| **Total** | | **115** | |

**Coverage threshold:** 70% minimum (enforced in `pyproject.toml`)

---

## 🏗️ Architecture Snapshot

```
[Bronze CSV]
    │
    ▼ extract()
[raw DataFrame]  ──►  [Quality Check — 5 checks, dates=False]
    │
    ▼ clean()
[cleaned DataFrame]  ──►  [Quality Check — 6 checks, dates=True]
    │
    ▼ engineer()
[enriched DataFrame]
    │
    ├──► Silver Parquet  →  data/silver/cleaned_sales.parquet
    ├──► Gold Parquets   →  data/gold/*.parquet  (5 tables)
    └──► DuckDB          →  database/superstore.duckdb  (1 fact + 5 agg tables)
```

---

## ⚙️ Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Language | Python | 3.11 |
| Data processing | pandas | 2.x |
| Numerical ops | NumPy | ≥1.24 |
| Parquet engine | pyarrow | ≥14.0 |
| Analytical DB | DuckDB | ≥0.10 |
| Config | PyYAML | ≥6.0 |
| Testing | pytest + pytest-cov | ≥7.4 / ≥4.1 |
| Formatting | black | ≥23.0 |
| Import sorting | isort | ≥5.12 |
| Linting | flake8 | ≥6.1 |
| Pre-commit hooks | pre-commit | ≥3.4 |
| CI/CD | GitHub Actions | — |
| Containers | Docker + Compose | — |
| Orchestration | Apache Airflow (self-hosted, LocalExecutor) | 3.3.0 |

---

## 🔁 CI/CD Workflows

| Workflow | File | Trigger | Steps |
|---|---|---|---|
| CI — Lint, Test, Coverage | `.github/workflows/ci.yml` | Push to any branch / PR to main | black check → isort check → flake8 → unit tests + coverage → integration tests → upload coverage artifact |
| Pipeline — Daily ETL Run | `.github/workflows/pipeline.yml` | Daily schedule + manual dispatch | Full pipeline run → upload gold Parquets + log as artifacts |

---

## 📝 Git History Snapshot *(most recent milestones, updated 2026-08-09 — see `git log` for full history, 54 commits total)*

| Hash | Message |
|---|---|
| `cf4ce5ca` | fix(feature-engineer): cast Quantity once for both np.where operands |
| `8a3b72a6` | fix(docker): mount project logs volume, narrow airflow-init idempotency |
| `c59eaf00` | fix(dag): sync quality-gate, drift, and profiling flags with config.yaml |
| `58a42d06` | docs: fix remaining stale product-performance count in docs/ |
| `1ebcd5b9` | docs: document the Airflow DAG (Phase 6 / v2.0) |
| `cc68e5e9` | fix(feature-engineer): cast shipping_days to float64 before np.select comparisons |
| `65162adb` | fix(feature-engineer): cast Quantity to float64 before per-unit division |
| `7cf4e21e` | fix(airflow): prevent run_id collapse-to-root and add regression tests |
| `51d9a6a0` | fix(airflow): prevent path traversal in DAG scratch-dir resolution |
| `2b10b219` | feat(airflow): add sales_pipeline_dag wrapping the ETL stages |

---

## 🗺️ Release Roadmap

### ✅ v1.0.0 — MVP ETL Pipeline *(Shipped)*

Full medallion ETL pipeline with data quality gates, feature engineering, DuckDB analytical store, CI/CD, Docker, and a 90-test suite. Production-ready for local and containerised execution.

| Area | Deliverable |
|---|---|
| Pipeline | Bronze → Silver → Gold medallion architecture |
| Quality | 6 automated data quality checks (raw + cleaned) |
| Features | 13 derived analytical columns |
| Storage | Silver Parquet + 5 Gold Parquets + DuckDB (1 fact + 5 agg tables) |
| CI/CD | GitHub Actions — lint + test + coverage + daily scheduled run |
| Containers | Multi-stage Dockerfile + Docker Compose |
| Tests | 90 tests (78 unit + 12 integration), 70 % coverage floor |
| Docs | README with execution evidence + architecture + data dictionary |

---

### ✅ v1.1 — Observability & Quality Hardening *(Shipped — 2026-05-09)*

| Item | File | Status |
|---|---|---|
| Codecov integration | `.github/workflows/ci.yml` | ✅ Done — `codecov/codecov-action@v4` wired; badge in README |
| Data profiling HTML report | `src/quality/profiler.py` | ✅ Done — ydata-profiling with pandas-describe fallback |
| Statistical drift detection | `src/quality/drift_detector.py` | ✅ Done — 9 metrics tracked; WARNING on >5% shift |
| Pipeline wiring | `orchestration/pipeline.py` | ✅ Done — stages 7 (drift) + 8 (profile) after load |
| Profiling artifact in CI | `.github/workflows/pipeline.yml` | ✅ Done — HTML report uploaded after each daily run |
| Optional dep isolation | `requirements-profiling.txt` | ✅ Done — ydata-profiling separated; core Docker image stays lean |

> **One manual step required:** Add `CODECOV_TOKEN` secret to the repo (Settings → Secrets → Actions) using the token from [codecov.io](https://codecov.io/gh/deepan-mehta-analytics/sales-data-pipeline) to activate the coverage badge.

---

### ✅ v1.2 — Query API Layer *(Shipped — 2026-05-09)*

Exposes the DuckDB gold tables as a typed REST API and publishes versioned Docker images to GHCR.

| Item | File | Status |
|---|---|---|
| FastAPI query layer | `api/app.py` | ✅ Done — 4 endpoints + `/health` |
| DuckDB read-only dependency | `api/database.py` | ✅ Done — per-request connection, 503 guard |
| Pydantic response schemas | `api/models.py` | ✅ Done — 4 typed models |
| API requirements isolation | `requirements-api.txt` | ✅ Done — fastapi, uvicorn, httpx |
| API smoke tests | `tests/integration/test_api.py` | ✅ Done — 14 tests |
| Docker API stage | `Dockerfile` (stages 3 + 4) | ✅ Done — api-builder + api-runtime |
| Docker Compose api service | `docker-compose.yml` | ✅ Done — port 8000, DuckDB volume |
| GHCR release workflow | `.github/workflows/release.yml` | ✅ Done — pipeline + API images on v* tags |
| CI extended | `.github/workflows/ci.yml` | ✅ Done — API tests + api/ lint |
| Makefile targets | `Makefile` | ✅ Done — make api, make test-api |
| README update | `README.md` | ✅ Done — "Query the API" section with curl examples |
| CHANGELOG entry | `CHANGELOG.md` | ✅ Done |
| v1.2.0 GitHub release | — | ✅ Done |

---

> **Roadmap direction (2026-08-09):** the sequence below matches the public architecture diagram in the
> [`deepan-mehta-analytics` profile README](https://github.com/deepan-mehta-analytics/deepan-mehta-analytics) —
> that diagram is the canonical source for this repo's forward roadmap. An earlier RAG-layer plan
> (ChromaDB + sentence-transformers + Claude Sonnet, scoped 2026-05-09) has been deferred in favour of
> this customer-analytics-platform direction; it was never shipped and is not part of the active roadmap.
>
> **⚠️ Sync note:** the same Mermaid diagram is now embedded in two places — this repo's
> [README.md](README.md) and the `deepan-mehta-analytics` profile README (the GitHub landing page).
> The profile README is a portfolio-wide page — its "Current Project" callout and featured diagram
> rotate to whichever repo is actively being worked on, so **it will not always be showing this
> repo's diagram**. Do not assume the profile page reflects this repo's latest roadmap at any given
> moment. When this repo's roadmap changes, update both copies in the same session; when checking
> for drift, diff this repo's README.md diagram against the profile README's *only if* the profile
> page is currently featuring this project.

### 🔄 v2.0 — Data Infrastructure *(In Progress)*

| Item | Description | Status |
|---|---|---|
| Cloud orchestration | Airflow DAG (`dags/sales_pipeline_dag.py`) wrapping all 8 pipeline stages, self-hosted via Docker Compose | ✅ Done |
| Cloud analytical store | BigQuery / Snowflake as a partitioned, clustered, cost-optimised store alongside local DuckDB | 🔜 Backlog |
| Incremental load | Delta detection — process only new/changed rows on each run (CDC support) | 🔜 Backlog |
| Experiment / run tracking | MLflow or W&B run tracking for pipeline executions and data quality metrics | 🔜 Backlog |

### 🔜 v2.1 — Customer Segmentation *(Backlog — scikit-learn, Databricks)*

| Item | Description |
|---|---|
| RFM analysis | Recency · Frequency · Monetary scoring per customer |
| Cohort analysis | Signup cohorts, engagement lifecycle tracking |
| K-Means clustering | Unsupervised persona discovery over the warehouse layer |

### 🔜 v2.2 — Retention Analytics *(Backlog — scikit-learn, Databricks)*

| Item | Description |
|---|---|
| Churn classification | At-risk flagging with re-engagement triggers |
| LTV correlation | High-value segment identification, feeding back into segmentation |

### 🔜 v2.3 — Analytics Dashboard *(Backlog — Tableau / Streamlit)*

| Item | Description |
|---|---|
| KPI tracking | Live dashboard sourced from the FastAPI serving layer |
| Segment views | Visualise RFM / K-Means personas from v2.1 |
| Retention curves | Churn and LTV-by-cohort views from v2.2 |
