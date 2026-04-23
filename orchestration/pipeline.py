# =============================================================================
# orchestration/pipeline.py
# DAG-style pipeline orchestrator for the Superstore Sales Data Pipeline.
#
# This is the single entry point for a complete pipeline run.  It wires
# together all pipeline stages in the correct order, measures execution time
# at each stage, enforces quality gates, and emits a structured run report.
#
# Pipeline DAG (directed acyclic graph):
#
#   [Extract]  ──►  [Quality Check (raw)]  ──►  [Clean]
#       ──►  [Quality Check (cleaned)]  ──►  [Engineer]  ──►  [Load]
#
# Run directly:    python orchestration/pipeline.py
# Run via Make:    make run
# =============================================================================

import sys  # Used to exit with a non-zero code on failure
import time  # Measures elapsed time for each pipeline stage
from datetime import datetime, timezone  # Generates an ISO-8601 run timestamp
from pathlib import Path  # Cross-platform path resolution

import yaml  # Reads config.yaml for quality-gate settings

# ---------------------------------------------------------------------------
# Add the project root to sys.path so that 'from src...' imports work when
# this script is executed directly (python orchestration/pipeline.py).
# When invoked via pytest or as a module, Python's regular import machinery
# already knows the project root.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # orchestration → project root
sys.path.insert(0, str(PROJECT_ROOT))  # Prepend to sys.path

# Import all pipeline stages and the quality validator.
from src.extract.extractor import extract  # Bronze-layer CSV ingestion
from src.load.loader import load  # Gold-layer loading
from src.quality.validators import run_quality_checks  # Data quality validation
from src.transform.cleaner import clean  # Silver-layer data cleaning
from src.transform.feature_engineer import engineer  # Feature engineering
from src.utils.logger import get_logger  # Centralised JSON logger

# Obtain a logger named after the orchestration module.
logger = get_logger("orchestration.pipeline")

# Absolute path to the configuration file.
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def _load_config() -> dict:
    """Read and return the pipeline configuration as a Python dict."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open config.yaml
        return yaml.safe_load(fh)  # Parse YAML


# =============================================================================
# Stage runner
# A thin wrapper around each pipeline function that adds timing, logging,
# and uniform exception handling.  Using a wrapper keeps the run() function
# below readable and avoids repeating try/except blocks.
# =============================================================================


def _run_stage(stage_name: str, func, *args, **kwargs):
    """
    Execute a pipeline stage function, measure its elapsed time, and return
    its result.  Logs start, success, and failure events for every stage.

    Parameters
    ----------
    stage_name : str     Human-readable name used in log messages.
    func       : callable  The pipeline function to execute.
    *args      : positional arguments forwarded to func.
    **kwargs   : keyword arguments forwarded to func.

    Returns
    -------
    Any
        Whatever the wrapped function returns on success.

    Raises
    ------
    Exception
        Re-raises any exception thrown by func after logging it.
    """
    logger.info(f"Stage starting: {stage_name}")  # Log the stage start event
    t_start = time.perf_counter()  # High-resolution start timestamp

    try:
        result = func(*args, **kwargs)  # Execute the actual pipeline function
    except Exception as exc:
        elapsed = time.perf_counter() - t_start  # Measure time even on failure
        logger.error(  # Log the failure with the exception detail
            f"Stage failed: {stage_name}",
            exc_info=True,  # Append full traceback to the log record
            extra={"elapsed_seconds": round(elapsed, 3)},
        )
        raise  # Re-raise so the outer run() function can halt the pipeline

    elapsed = time.perf_counter() - t_start  # Measure elapsed time on success
    logger.info(  # Log the stage completion with timing
        f"Stage complete: {stage_name}",
        extra={"elapsed_seconds": round(elapsed, 3)},
    )

    return result  # Return the stage's output to the caller


# =============================================================================
# run  –  Main pipeline entry point
# =============================================================================


def run() -> dict:
    """
    Execute the full end-to-end pipeline and return a structured run report.

    Stages
    ------
    1. Extract          – Load the raw Superstore CSV into a DataFrame.
    2. Quality (raw)    – Validate schema, nulls, ranges on the raw data.
    3. Clean            – Parse dates, strip whitespace, deduplicate.
    4. Quality (clean)  – Re-validate after cleaning; run date-order checks.
    5. Engineer         – Add derived features (margins, shipping days, etc.).
    6. Load             – Write Parquet files and populate DuckDB.

    Returns
    -------
    dict
        Pipeline run report with stage timings, row counts, and quality results.
        Useful for programmatic inspection in tests or downstream alerting.

    Raises
    ------
    SystemExit
        Exits with code 1 if a quality gate fails and fail_on_quality_error
        is True in config.yaml.
    """
    config = _load_config()  # Read pipeline settings from config.yaml
    run_start = time.perf_counter()  # Overall pipeline start time
    run_ts = datetime.now(tz=timezone.utc).isoformat()  # ISO timestamp for the report

    # Determine whether a failing quality check should abort the pipeline.
    fail_on_quality_error = config.get("pipeline", {}).get("fail_on_quality_error", True)

    logger.info(  # Log the pipeline start event with version and timestamp
        "Pipeline run started",
        extra={
            "project": config["project"]["name"],
            "version": config["project"]["version"],
            "timestamp": run_ts,
        },
    )

    # Initialise the run report; stages populate it as they complete.
    report = {
        "run_timestamp": run_ts,  # When this run started (UTC ISO-8601)
        "stages": {},  # Per-stage timing and metadata
        "quality_checks": [],  # Quality check results
        "overall_status": "running",  # Updated to 'success' or 'failed' at the end
    }

    # -----------------------------------------------------------------------
    # Stage 1: Extract
    # Load the raw CSV into a DataFrame and capture ingestion metadata.
    # -----------------------------------------------------------------------
    raw_df, extract_meta = _run_stage("extract", extract)
    report["stages"]["extract"] = extract_meta  # Attach extraction metadata to the report

    # -----------------------------------------------------------------------
    # Stage 2: Quality check on raw data
    # Run structural and value-range checks before any transformation.
    # Date-ordering checks are skipped here because dates are still strings.
    # -----------------------------------------------------------------------
    raw_quality = _run_stage(
        "quality_check_raw",
        run_quality_checks,
        raw_df,
        run_date_checks=False,  # Dates are not yet parsed; skip temporal checks
    )
    report["quality_checks"].append(
        {  # Record the raw-data quality results
            "stage": "raw",
            "passed": raw_quality.overall_passed,
            "total_checks": raw_quality.total_checks,
            "failed_checks": raw_quality.failed_checks,
        }
    )

    # Enforce the quality gate: abort if any raw check failed.
    if not raw_quality.overall_passed and fail_on_quality_error:
        logger.error(  # Log the quality gate failure
            "Quality gate failed on raw data — aborting pipeline",
            extra={"failed_checks": raw_quality.failed_checks},
        )
        report["overall_status"] = "failed"  # Mark the run as failed
        sys.exit(1)  # Non-zero exit code signals failure to CI/CD

    # -----------------------------------------------------------------------
    # Stage 3: Clean
    # Apply all cleaning transformations to produce the silver DataFrame.
    # -----------------------------------------------------------------------
    cleaned_df, clean_meta = _run_stage("clean", clean, raw_df)
    report["stages"]["clean"] = clean_meta  # Attach cleaning metadata to the report

    # Check that the minimum row threshold has not been breached.
    min_rows = config.get("pipeline", {}).get("min_rows_threshold", 100)
    if len(cleaned_df) < min_rows:  # Fewer rows than expected after cleaning
        logger.warning(
            "Cleaned DataFrame has fewer rows than the configured minimum",
            extra={"rows": len(cleaned_df), "min_rows_threshold": min_rows},
        )

    # -----------------------------------------------------------------------
    # Stage 4: Quality check on cleaned data
    # Now that dates are parsed, run the full check suite including temporal
    # validation (Ship Date >= Order Date).
    # -----------------------------------------------------------------------
    clean_quality = _run_stage(
        "quality_check_cleaned",
        run_quality_checks,
        cleaned_df,
        run_date_checks=True,  # Date columns are now datetime; run temporal checks
    )
    report["quality_checks"].append(
        {  # Record the cleaned-data quality results
            "stage": "cleaned",
            "passed": clean_quality.overall_passed,
            "total_checks": clean_quality.total_checks,
            "failed_checks": clean_quality.failed_checks,
        }
    )

    # Enforce the quality gate on cleaned data.
    if not clean_quality.overall_passed and fail_on_quality_error:
        logger.error(
            "Quality gate failed on cleaned data — aborting pipeline",
            extra={"failed_checks": clean_quality.failed_checks},
        )
        report["overall_status"] = "failed"
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Stage 5: Feature engineering
    # Add derived columns (margins, shipping days, profit tiers, etc.).
    # -----------------------------------------------------------------------
    enriched_df, engineer_meta = _run_stage("engineer", engineer, cleaned_df)
    report["stages"]["engineer"] = engineer_meta  # Attach engineering metadata

    # -----------------------------------------------------------------------
    # Stage 6: Load
    # Write Parquet files (silver + gold) and populate the DuckDB database.
    # -----------------------------------------------------------------------
    load_meta = _run_stage("load", load, enriched_df)
    report["stages"]["load"] = load_meta  # Attach loading metadata

    # -----------------------------------------------------------------------
    # Pipeline complete
    # -----------------------------------------------------------------------
    total_elapsed = time.perf_counter() - run_start  # Total wall-clock time for the whole run
    report["overall_status"] = "success"  # Mark the run as successful
    report["elapsed_seconds"] = round(total_elapsed, 3)  # Total run duration in seconds

    logger.info(  # Log the final success summary
        "Pipeline run complete",
        extra={
            "overall_status": report["overall_status"],
            "elapsed_seconds": report["elapsed_seconds"],
            "rows_loaded": load_meta.get("rows_loaded"),
            "gold_tables": load_meta.get("gold_tables"),
        },
    )

    return report  # Return the full run report to the caller


# =============================================================================
# Script entry point
# Allows the pipeline to be executed directly: python orchestration/pipeline.py
# =============================================================================

if __name__ == "__main__":
    run()  # Execute the pipeline when this script is run directly
