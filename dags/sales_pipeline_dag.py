# =============================================================================
# dags/sales_pipeline_dag.py
# Airflow DAG wrapping the existing Superstore ETL pipeline stages as
# individually retryable, individually observable tasks.
#
# Mirrors orchestration/pipeline.py's stage order exactly:
#   extract -> quality(raw) -> clean -> quality(cleaned) -> engineer -> load
#   -> drift -> profile
# without changing any of the underlying stage logic in src/. Intermediate
# DataFrames are handed off between tasks as Parquet files on a shared
# volume (not XCom) — Airflow's metadata-DB-backed XCom is not meant to
# carry multi-megabyte DataFrames.
#
# Trigger manually:  airflow dags trigger sales_pipeline_dag
# Or from the UI:    http://localhost:8080/dags/sales_pipeline_dag
# =============================================================================

import os  # Reads GCP_PROJECT_ID / BQ_DATASET_ID env vars for the BigQuery sync task
from datetime import datetime, timedelta  # DAG start_date; timedelta for load_bigquery_task's retry_delay
from pathlib import Path  # Cross-platform path handling for intermediate Parquet files

import pandas as pd  # Parquet read/write for inter-task hand-off
import yaml  # Reads config/config.yaml, mirroring orchestration/pipeline.py's config loading
from airflow.decorators import dag, task  # TaskFlow API
from airflow.exceptions import AirflowException  # Raised to fail a task deliberately on a bad quality gate
from path_utils import resolve_run_dir  # Airflow-independent, unit-tested run_id sanitization + containment check

from src.extract.extractor import extract  # Bronze-layer CSV ingestion
from src.load.bigquery_loader import sync_to_bigquery  # v2.0 — BigQuery cloud analytical store sync
from src.load.loader import load  # Gold-layer loading — writes Parquet + DuckDB
from src.quality.drift_detector import detect_drift  # Statistical drift detection vs prior run
from src.quality.profiler import generate_profile  # HTML data-profiling report generator
from src.quality.validators import run_quality_checks  # Data quality validation
from src.transform.cleaner import clean  # Silver-layer cleaning
from src.transform.feature_engineer import engineer  # Feature engineering
from src.utils.pipeline_state import get_watermark, set_watermark  # Incremental-load watermark storage

# Project root as mounted inside the Airflow containers (see docker-compose.yml x-airflow-common).
PROJECT_ROOT = Path("/opt/airflow/project")  # Matches the volume mount target, not the image's own /opt/airflow

# Scratch directory for intermediate Parquet hand-offs between DAG tasks, keyed by run.
AIRFLOW_TMP_ROOT = PROJECT_ROOT / "data" / "airflow_tmp"  # Gitignored — see .gitignore

# Absolute path to the configuration file, same file orchestration/pipeline.py reads.
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Resolved against the container's mounted project root


def _load_config() -> dict:
    """Read and return the pipeline configuration as a Python dict."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open config.yaml
        return yaml.safe_load(fh)  # Parse YAML


# Load config once at module (DAG-parse) time — mirrors orchestration/pipeline.py's
# _load_config() call pattern, but read once here since DAG-parse happens per-file-load
# rather than per-run. Same keys, same defaults, same `.get("pipeline", {}).get(...)` shape.
_CONFIG = _load_config()  # Parsed config/config.yaml contents
FAIL_ON_QUALITY_ERROR = _CONFIG.get("pipeline", {}).get("fail_on_quality_error", True)  # Quality-gate enforcement flag
DRIFT_THRESHOLD = _CONFIG.get("pipeline", {}).get("drift_threshold", 0.05)  # Relative drift threshold (5% default)
GENERATE_PROFILE = _CONFIG.get("pipeline", {}).get("generate_profile", True)  # Whether to run the profiling stage
INCREMENTAL_BUFFER_DAYS = _CONFIG.get("pipeline", {}).get("incremental_buffer_days", 3)  # Late-arrival re-check window
DB_PATH = PROJECT_ROOT / _CONFIG["paths"]["database"]  # Resolved DuckDB path — same file load_task writes to


def _tmp_dir(run_id: str) -> Path:
    """Return (creating if needed) the scratch directory for one DAG run."""
    # run_id can be caller-supplied (Airflow's REST API accepts an optional dag_run_id
    # override), so it cannot be trusted as a bare path segment. The sanitization and
    # containment-check logic (including rejecting run_ids that collapse onto the root
    # itself, e.g. "", ".") lives in path_utils.py — a stdlib-only module with its own
    # unit tests, since this DAG module can't be imported without airflow installed.
    run_dir = resolve_run_dir(AIRFLOW_TMP_ROOT, run_id)  # sanitize run_id, validate containment + distinctness
    run_dir.mkdir(parents=True, exist_ok=True)  # Create the run-scoped scratch directory
    return run_dir  # Hand back the resolved path


@dag(
    dag_id="sales_pipeline_dag",  # Unique DAG identifier shown in the UI
    description="Superstore Sales ETL pipeline — Airflow orchestration of the existing src/ stages",
    schedule=None,  # Manually/API triggered; the GitHub Actions daily cron remains the scheduled path
    start_date=datetime(2026, 1, 1),  # Static start date; Airflow requires one, it isn't used for scheduling here
    catchup=False,  # Do not backfill historical runs
    max_active_runs=1,  # Single writer: load_task shares data/silver, data/gold, database/superstore.duckdb
    tags=["sales-pipeline", "etl", "portfolio"],  # UI filter tags
    params={"full_refresh": False},  # Trigger with --conf '{"full_refresh": true}' to force a full reload
)
def sales_pipeline_dag():
    """Superstore Sales ETL pipeline, orchestrated stage-by-stage via Airflow."""

    @task
    def extract_task(run_id: str = None, params: dict = None) -> str:
        """Run the extract stage; persist the raw DataFrame for the next task."""
        full_refresh = bool((params or {}).get("full_refresh", False))  # Extract full_refresh flag from DAG params
        watermark = None if full_refresh else get_watermark(DB_PATH)  # Get stored watermark unless doing full reload
        raw_df, metadata = extract(since=watermark, buffer_days=INCREMENTAL_BUFFER_DAYS)  # Bronze-layer CSV ingestion
        out_path = _tmp_dir(run_id) / "raw.parquet"  # This task's hand-off file
        raw_df.to_parquet(out_path, index=False)  # Persist for quality_raw_task to read
        return str(out_path)  # Hand the path (not the DataFrame) forward via XCom

    @task
    def quality_raw_task(raw_path: str) -> str:
        """Run raw-data quality checks; fail this task if the quality gate fails."""
        raw_df = pd.read_parquet(raw_path)  # Reload the raw DataFrame from the prior task
        report = run_quality_checks(raw_df, run_date_checks=False)  # Dates are still strings at this stage
        if not report.overall_passed and FAIL_ON_QUALITY_ERROR:  # Same gate as pipeline.py: only raise when enabled
            raise AirflowException(
                f"Raw-data quality gate failed: {report.failed_checks} of {report.total_checks} checks failed"
            )
        return raw_path  # Pass the same path through unchanged

    @task
    def clean_task(raw_path: str, run_id: str = None) -> str:
        """Run the clean stage; persist the cleaned DataFrame."""
        raw_df = pd.read_parquet(raw_path)  # Reload the raw DataFrame
        cleaned_df, metadata = clean(raw_df)  # Silver-layer cleaning (unchanged from src/)
        out_path = _tmp_dir(run_id) / "cleaned.parquet"  # This task's hand-off file
        cleaned_df.to_parquet(out_path, index=False)  # Persist for quality_cleaned_task to read
        return str(out_path)  # Hand the path forward

    @task
    def quality_cleaned_task(cleaned_path: str) -> str:
        """Run cleaned-data quality checks, including temporal (ship-after-order) validation."""
        cleaned_df = pd.read_parquet(cleaned_path)  # Reload the cleaned DataFrame
        report = run_quality_checks(cleaned_df, run_date_checks=True)  # Dates are now datetime-typed
        if not report.overall_passed and FAIL_ON_QUALITY_ERROR:  # Same gate as pipeline.py: only raise when enabled
            raise AirflowException(
                f"Cleaned-data quality gate failed: {report.failed_checks} of {report.total_checks} checks failed"
            )
        return cleaned_path  # Pass the same path through unchanged

    @task
    def engineer_task(cleaned_path: str, run_id: str = None) -> str:
        """Run the feature-engineering stage; persist the enriched DataFrame."""
        cleaned_df = pd.read_parquet(cleaned_path)  # Reload the cleaned DataFrame
        enriched_df, metadata = engineer(cleaned_df)  # Add derived analytical columns (unchanged from src/)
        out_path = _tmp_dir(run_id) / "enriched.parquet"  # This task's hand-off file
        enriched_df.to_parquet(out_path, index=False)  # Persist for load/drift/profile tasks to read
        return str(out_path)  # Hand the path forward

    @task
    def load_task(enriched_path: str, run_id: str = None, params: dict = None) -> str:
        """Run the load stage: write Silver/Gold Parquet, populate DuckDB, advance the watermark."""
        full_refresh = bool((params or {}).get("full_refresh", False))  # Extract full_refresh flag from DAG params
        enriched_df = pd.read_parquet(enriched_path)  # Reload the enriched DataFrame
        load_meta = load(enriched_df, full_refresh=full_refresh)  # Writes data/silver, data/gold, DuckDB

        # Advance the stored watermark only now that load() has returned
        # without raising — a failed task must never advance the watermark.
        if load_meta.get("watermark_candidate"):  # Check if a watermark_candidate was produced
            set_watermark(DB_PATH, load_meta["watermark_candidate"], run_id=run_id or "unknown")  # Store it

        # Return the FULL accumulated silver Parquet path (not enriched_path,
        # which on an incremental run is only the new batch) so drift/profile
        # tasks compare/profile the complete dataset, not just the new rows.
        return load_meta["silver_path"]

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_bigquery_task(loaded_path: str) -> None:
        """
        Sync the Gold Parquet files just written by load_task into BigQuery
        (v2.0 cloud analytical store, alongside the local DuckDB store).
        Runs only here — never from orchestration/pipeline.py or the GitHub
        Actions daily cron, so no GCP credential is needed in CI.
        """
        # Set (possibly to "") by docker-compose's ${GCP_PROJECT_ID:-} default; sync_to_bigquery
        # is what actually validates a non-empty value — this KeyError only guards a caller
        # that isn't the docker-compose-configured container (e.g. a bare `python -c` invocation).
        project_id = os.environ["GCP_PROJECT_ID"]
        dataset_id = os.environ.get("BQ_DATASET_ID", "superstore_analytics")  # Falls back to sync_to_bigquery's default
        sync_to_bigquery(project_id=project_id, dataset_id=dataset_id)  # Reads Gold Parquet, loads into BigQuery

    @task
    def drift_task(silver_path: str) -> None:
        """Run drift detection against the full accumulated dataset. Observability only, never fails the DAG."""
        full_df = pd.read_parquet(silver_path)  # Reload the FULL accumulated silver DataFrame
        detect_drift(full_df, threshold=DRIFT_THRESHOLD)  # Logs WARNINGs on drift beyond config's threshold

    @task
    def profile_task(silver_path: str) -> None:
        """Generate the HTML data-profiling report for the full accumulated dataset, unless disabled in config."""
        full_df = pd.read_parquet(silver_path)  # Reload the FULL accumulated silver DataFrame
        if GENERATE_PROFILE:  # Only run if profiling is enabled in config.yaml
            generate_profile(full_df)  # Writes reports/profile_<timestamp>.html (unchanged from src/)
        else:
            print("Profiling skipped (generate_profile: false in config.yaml)")  # Mirrors pipeline.py's skip log

    # -------------------------------------------------------------------
    # Wire the linear dependency chain — mirrors orchestration/pipeline.py's
    # stage order exactly; each stage is now an independently retryable,
    # independently observable Airflow task instead of one long function.
    # -------------------------------------------------------------------
    raw_path = extract_task()  # Stage 1
    validated_raw_path = quality_raw_task(raw_path)  # Stage 2 (gate)
    cleaned_path = clean_task(validated_raw_path)  # Stage 3
    validated_cleaned_path = quality_cleaned_task(cleaned_path)  # Stage 4 (gate)
    enriched_path = engineer_task(validated_cleaned_path)  # Stage 5
    loaded_path = load_task(enriched_path)  # Stage 6
    load_bigquery_task(loaded_path)  # Stage 6.5 (v2.0) — BigQuery sync, independent of drift/profile below
    drift_task(loaded_path)  # Stage 7 (observability only)
    profile_task(loaded_path)  # Stage 8 (observability only)


# Instantiate the DAG so Airflow's DAG processor discovers it at parse time.
sales_pipeline_dag()
