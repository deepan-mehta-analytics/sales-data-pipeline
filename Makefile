# =============================================================================
# Makefile
# Developer convenience commands for the Superstore Sales Data Pipeline.
#
# Usage:
#   make install      – Set up the project and install all dependencies
#   make run          – Execute the full ETL pipeline
#   make test         – Run all tests with coverage
#   make test-unit    – Run unit tests only (fast)
#   make test-int     – Run integration tests only
#   make lint         – Run all code quality checks
#   make format       – Auto-format all code with black and isort
#   make clean        – Remove generated files and caches
#   make help         – Print this help message
#
# The .PHONY declaration tells make that these targets are not real files
# so it always executes the recipe, even if a file with the same name exists.
# =============================================================================

.PHONY: install run profile api test test-unit test-int test-api lint format clean help

# ---------------------------------------------------------------------------
# Python interpreter — override with: make run PYTHON=python3.11
# ---------------------------------------------------------------------------
PYTHON := python   # Default Python interpreter

# ---------------------------------------------------------------------------
# install
# Set up the virtual environment, install all dependencies, and configure
# pre-commit hooks so code quality checks run automatically before each commit.
# ---------------------------------------------------------------------------
install:
	@echo "📦 Installing production dependencies..."
	$(PYTHON) -m pip install --upgrade pip              # Upgrade pip first
	$(PYTHON) -m pip install -r requirements.txt        # Install runtime deps
	@echo "🛠️  Installing development dependencies..."
	$(PYTHON) -m pip install -r requirements-dev.txt    # Install dev/test deps
	@echo "🪝  Installing pre-commit hooks..."
	pre-commit install                                   # Register git hooks
	@echo "✅ Installation complete."

# ---------------------------------------------------------------------------
# run
# Execute the full ETL pipeline from bronze CSV to DuckDB.
# Includes drift detection and profiling (stages 7 and 8) automatically.
# ---------------------------------------------------------------------------
run:
	@echo "🚀 Running the full ETL pipeline..."
	$(PYTHON) orchestration/pipeline.py
	@echo "✅ Pipeline run complete."

# ---------------------------------------------------------------------------
# profile
# Install ydata-profiling and run the pipeline to generate a full HTML report.
# The report is written to reports/profile_YYYYMMDD_HHMMSS.html
# ---------------------------------------------------------------------------
profile:
	@echo "📦 Installing profiling dependencies..."
	$(PYTHON) -m pip install -r requirements-profiling.txt   # Install ydata-profiling
	@echo "🚀 Running pipeline with full ydata-profiling report..."
	$(PYTHON) orchestration/pipeline.py
	@echo "✅ Profiling report written to reports/"

# ---------------------------------------------------------------------------
# api
# Install API dependencies and start the FastAPI server locally on port 8000.
# The pipeline must have been run first to generate the DuckDB database file.
# Interactive docs available at http://localhost:8000/docs after startup.
# ---------------------------------------------------------------------------
api:
	@echo "📦 Installing API dependencies..."
	$(PYTHON) -m pip install -r requirements-api.txt   # Install fastapi + uvicorn
	@echo "🚀 Starting FastAPI query layer on http://localhost:8000 ..."
	uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

# ---------------------------------------------------------------------------
# test
# Run the complete test suite (unit + integration) with coverage measurement.
# Fails if coverage falls below the threshold set in pyproject.toml.
# ---------------------------------------------------------------------------
test:
	@echo "🧪 Running all tests with coverage..."
	pytest tests/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		-v
	@echo "✅ Tests complete. Coverage report: htmlcov/index.html"

# ---------------------------------------------------------------------------
# test-unit
# Run only unit tests — fast, no real I/O.  Good for development iteration.
# ---------------------------------------------------------------------------
test-unit:
	@echo "🧪 Running unit tests..."
	pytest tests/unit/ -v
	@echo "✅ Unit tests complete."

# ---------------------------------------------------------------------------
# test-int
# Run only pipeline integration tests — slower, performs real disk I/O.
# ---------------------------------------------------------------------------
test-int:
	@echo "🧪 Running pipeline integration tests..."
	pytest tests/integration/test_pipeline.py -v
	@echo "✅ Pipeline integration tests complete."

# ---------------------------------------------------------------------------
# test-api
# Run only the FastAPI smoke tests.  Uses an in-memory test DuckDB — no
# real superstore.duckdb is required.
# ---------------------------------------------------------------------------
test-api:
	@echo "🧪 Running API smoke tests..."
	pytest tests/integration/test_api.py -v
	@echo "✅ API smoke tests complete."

# ---------------------------------------------------------------------------
# lint
# Run all static analysis checks without modifying any files.
# Exits with code 1 if any check fails (safe for CI/CD).
# ---------------------------------------------------------------------------
lint:
	@echo "🔍 Checking formatting (black)..."
	black --check src/ tests/ orchestration/ api/
	@echo "🔍 Checking import order (isort)..."
	isort --check-only src/ tests/ orchestration/ api/
	@echo "🔍 Running linter (flake8)..."
	flake8 src/ tests/ orchestration/ api/ --max-line-length=120
	@echo "✅ All lint checks passed."

# ---------------------------------------------------------------------------
# format
# Auto-format all Python files with black and isort.
# Run this before committing to ensure lint checks pass.
# ---------------------------------------------------------------------------
format:
	@echo "✏️  Formatting code with black..."
	black src/ tests/ orchestration/ api/
	@echo "✏️  Sorting imports with isort..."
	isort src/ tests/ orchestration/ api/
	@echo "✅ Formatting complete."

# ---------------------------------------------------------------------------
# clean
# Remove all generated artefacts: compiled bytecode, test caches,
# coverage reports, and pipeline output files (Parquet + DuckDB).
# Does NOT remove raw bronze data (you need to re-download from Kaggle).
# ---------------------------------------------------------------------------
clean:
	@echo "🧹 Removing compiled Python files..."
	find . -type f -name "*.pyc" -delete          # Delete compiled bytecode files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "🧹 Removing test and coverage artefacts..."
	rm -rf .pytest_cache htmlcov coverage.xml .coverage
	@echo "🧹 Removing pipeline outputs..."
	rm -f  data/silver/*.parquet                  # Remove silver Parquet files
	rm -f  data/gold/*.parquet                    # Remove gold Parquet files
	rm -f  database/*.duckdb                      # Remove DuckDB database file
	rm -f  logs/pipeline.log                      # Remove the pipeline log file
	@echo "🧹 Removing profiling reports..."
	rm -f  reports/*.html                         # Remove generated HTML profile reports
	rm -f  reports/run_stats_reference.json       # Remove the drift-detector reference snapshot
	@echo "✅ Clean complete."

# ---------------------------------------------------------------------------
# help
# Print a summary of all available make targets.
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "Superstore Sales Data Pipeline — Makefile commands"
	@echo "────────────────────────────────────────────────────"
	@echo "  make install    Install all dependencies and git hooks"
	@echo "  make run        Execute the full ETL pipeline"
	@echo "  make profile    Install ydata-profiling and run with full HTML report"
	@echo "  make api        Install API deps and start FastAPI on port 8000"
	@echo "  make test       Run all tests with coverage report"
	@echo "  make test-unit  Run unit tests only (fast)"
	@echo "  make test-int   Run pipeline integration tests only"
	@echo "  make test-api   Run FastAPI smoke tests only"
	@echo "  make lint       Check code style and formatting"
	@echo "  make format     Auto-format code with black + isort"
	@echo "  make clean      Remove generated files and caches"
	@echo "  make help       Show this help message"
	@echo ""
