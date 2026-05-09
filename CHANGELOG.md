# Changelog

All notable changes to the Superstore Sales Data Pipeline are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project uses [Semantic Versioning](https://semver.org/).

---

## [1.2.0] — 2026-05-09

### Added
- `api/app.py` — FastAPI query layer with five endpoints: `/health`, `/sales/regions`, `/sales/trends`, `/products/top`, `/segments`
- `api/database.py` — DuckDB read-only connection dependency; yields one connection per request; 503 guard when database file is absent
- `api/models.py` — Pydantic v2 response schemas: `RegionSales`, `MonthlyTrend`, `ProductPerformance`, `CustomerSegment`
- `requirements-api.txt` — Isolated API dependencies (fastapi, uvicorn, httpx); not installed in the pipeline Docker image
- `tests/integration/test_api.py` — 14 smoke tests covering all endpoints, year filter, limit bounds, field presence, and ordering guarantees
- `Dockerfile` — `api-builder` (Stage 3) and `api-runtime` (Stage 4) added for the FastAPI service
- `docker-compose.yml` — `api` service added alongside `pipeline`; mounts the DuckDB volume; exposes port 8000
- `.github/workflows/release.yml` — GHCR Docker image publish workflow triggered on `v*.*.*` tags; pushes pipeline and API images as separate versioned + latest tags
- `.github/workflows/ci.yml` — API smoke tests added as a dedicated step; `api/` included in black / isort / flake8 checks; `requirements-api.txt` installed in CI
- `Makefile` — `make api` (start FastAPI server), `make test-api` (API smoke tests), `make test-int` narrowed to pipeline tests only; lint/format extended to `api/`

---

## [1.1.0] — 2026-05-09

### Added
- `src/quality/profiler.py` — Optional HTML data-profiling report via ydata-profiling; falls back to pandas-describe HTML when ydata-profiling is not installed
- `src/quality/drift_detector.py` — Dependency-free statistical drift detection comparing key metrics against a prior-run JSON reference snapshot
- `requirements-profiling.txt` — Separate optional requirements file for ydata-profiling to keep the core Docker image lean
- `reports/` directory — Output location for HTML profiling reports and the drift-reference JSON
- Codecov integration in `.github/workflows/ci.yml` — coverage badge wired to codecov.io via `codecov/codecov-action@v4`
- Profiling HTML report upload as a named artefact in `.github/workflows/pipeline.yml`
- `make profile` command in `Makefile` — installs ydata-profiling and runs the pipeline
- `pipeline.drift_threshold` and `pipeline.generate_profile` flags added to `config/config.yaml`
- README: Codecov badge, v1.1 status badge, release badge, profiling/observability section, roadmap table

### Fixed
- `.github/workflows/pipeline.yml` — corrected malformed YAML (`- - name:` → `- name:`) in the Run pipeline step

---

## [1.0.0] — 2024-01-01

### Added
- Bronze / Silver / Gold Medallion Architecture for layered data quality
- `src/extract/extractor.py` — CSV ingestion with dtype enforcement and schema validation
- `src/transform/cleaner.py` — Date parsing, whitespace stripping, deduplication, dtype casting
- `src/transform/feature_engineer.py` — 13 derived analytical columns (time, financial, categorical)
- `src/load/loader.py` — Silver Parquet, five Gold Parquet aggregations, DuckDB fact + agg tables
- `src/quality/validators.py` — Six data quality checks with structured ValidationResult reporting
- `src/utils/logger.py` — Centralised JSON-structured logging to stdout and file
- `orchestration/pipeline.py` — DAG-style orchestrator with stage timing, quality gates, and run report
- `config/config.yaml` — Central pipeline configuration (paths, encoding, date format, thresholds)
- `config/schema.yaml` — Column schema definition (dtypes, nullability, allowed values, range bounds)
- `tests/unit/` — Unit tests for extractor, cleaner, feature engineer, and validators
- `tests/integration/test_pipeline.py` — End-to-end pipeline integration test
- `tests/conftest.py` — Shared pytest fixtures (raw, cleaned, enriched DataFrames)
- `.github/workflows/ci.yml` — CI workflow: lint + unit tests + integration tests on every push
- `.github/workflows/pipeline.yml` — Scheduled daily pipeline execution via GitHub Actions
- `Dockerfile` — Multi-stage Docker build (builder + runtime stages)
- `docker-compose.yml` — Local Docker Compose configuration with volume mounts
- `Makefile` — Developer convenience commands (install, run, test, lint, format, clean)
- `pyproject.toml` — Modern Python packaging with black, isort, pytest, and coverage configuration
- `.pre-commit-config.yaml` — Git hooks for black, isort, flake8, and file hygiene
- `docs/architecture.md` — Pipeline architecture documentation with DAG diagram
- `docs/data_dictionary.md` — Full column reference for base and derived columns
- `notebooks/eda.ipynb` — Exploratory data analysis notebook
