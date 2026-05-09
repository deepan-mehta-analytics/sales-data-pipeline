# PROJECT-STATUS.md — Superstore Sales Data Pipeline

> **Last updated:** 2026-05-09  
> **Maintainer:** Deepan Mehta  
> **Repo:** `deepan-mehta-analytics/sales-data-pipeline`

---

## 🟢 Overall Status

| Field | Value |
|---|---|
| **Version** | 1.2.2 |
| **GitHub Release** | [v1.2.2 — CI Coverage Fix](https://github.com/deepan-mehta-analytics/sales-data-pipeline/releases/tag/v1.2.2) |
| **Phase** | v1.2 shipped — REST query layer, GHCR publishing, CI green |
| **Latest commit** | `28528c2f` — `fix(ci): omit optional-dep modules from coverage measurement` |
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
| `product_performance.parquet` | Parquet | 1,850 | Per-SKU: total_sales, total_profit, avg_margin |

---

## 🔵 DuckDB Analytical Store — `database/superstore.duckdb`

| Table | Type | Rows | Description |
|---|---|---|---|
| `fact_sales` | Fact | 9,994 | Full enriched silver DataFrame — central query table |
| `agg_sales_by_region` | Aggregation | 4 | Regional KPIs |
| `agg_sales_by_category` | Aggregation | 17 | Category / Sub-Category KPIs |
| `agg_customer_segments` | Aggregation | 3 | Segment KPIs |
| `agg_monthly_trends` | Aggregation | 48 | Monthly time-series |
| `agg_product_performance` | Aggregation | 1,850 | Product-level ranking |

**Key analytical finding:** Central region operating at **-10.41% profit margin** despite $501K revenue. West region leads both revenue and profitability at **+21.95% margin**.

---

## 🧪 Test Suite

| File | Type | Tests | Coverage scope |
|---|---|---|---|
| `tests/unit/test_cleaner.py` | Unit | 23 | `src/transform/cleaner.py` |
| `tests/unit/test_feature_engineer.py` | Unit | 19 | `src/transform/feature_engineer.py` |
| `tests/unit/test_validators.py` | Unit | 20 | `src/quality/validators.py` |
| `tests/unit/test_extractor.py` | Unit | 16 | `src/extract/extractor.py` |
| `tests/integration/test_pipeline.py` | Integration | 12 | Full end-to-end pipeline |
| **Total** | | **90** | |

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

---

## 🔁 CI/CD Workflows

| Workflow | File | Trigger | Steps |
|---|---|---|---|
| CI — Lint, Test, Coverage | `.github/workflows/ci.yml` | Push to any branch / PR to main | black check → isort check → flake8 → unit tests + coverage → integration tests → upload coverage artifact |
| Pipeline — Daily ETL Run | `.github/workflows/pipeline.yml` | Daily schedule + manual dispatch | Full pipeline run → upload gold Parquets + log as artifacts |

---

## 📝 Git History Snapshot

| Hash | Message |
|---|---|
| `3ebe8bc6` | docs: add live DuckDB query result showcasing gold layer |
| `0f45fe11` | docs: add pipeline execution evidence with screenshot |
| `5c7524a5` | style: apply black formatting to pipeline.py and test_validators.py |
| `995d3f9f` | chore: fix gitignore syntax, add gitattributes, clean tracked artefacts |
| `9a05b5e5` | fix(tests): align regex assertions and handle string-date edge case |
| `4d3c53c3` | style: apply isort import sorting |
| `d5efffed` | fix(ci): resolve flake8 lint errors and enforce whitespace rules |
| `c5b86226` | Initial commit — Production-ready sales data pipeline with ETL layers, testing, and CI/CD setup |

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

### 🔜 v2.0 — Cloud-Native Architecture *(Backlog)*

| Item | Description |
|---|---|
| Cloud storage output | Write Parquet artifacts to S3 / GCS / ADLS in addition to local disk |
| Cloud orchestration | Replace manual `orchestration/pipeline.py` entry point with an Airflow / Prefect DAG |
| Cloud analytical store | BigQuery as an optional analytical store alongside local DuckDB |
| Incremental load | Delta detection — process only new/changed rows on each run (CDC support) |
| Experiment / run tracking | MLflow or W&B run tracking for pipeline executions and data quality metrics |
