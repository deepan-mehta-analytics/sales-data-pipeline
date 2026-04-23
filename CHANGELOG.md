# Changelog

All notable changes to the Superstore Sales Data Pipeline are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project uses [Semantic Versioning](https://semver.org/).

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
