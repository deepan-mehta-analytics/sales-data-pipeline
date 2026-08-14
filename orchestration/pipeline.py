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

import os  # Reads the FULL_REFRESH / PIPELINE_ENV env vars for the __main__ entry point and MLflow tags
import sys  # Used to exit with a non-zero code on failure
import time  # Measures elapsed time for each pipeline stage
from datetime import datetime, timezone  # Generates an ISO-8601 run timestamp
from pathlib import Path  # Cross-platform path resolution

import pandas as pd  # Reads the full accumulated silver Parquet for drift/profiling
import yaml  # Reads config.yaml for quality-gate settings

from src.extract.extractor import extract  # Bronze-layer CSV ingestion
from src.load.loader import load  # Gold-layer loading
from src.quality.drift_detector import detect_drift  # Statistical drift detection vs prior run
from src.quality.profiler import generate_profile  # HTML data-profiling report generator
from src.quality.validators import run_quality_checks  # Data quality validation
from src.transform.cleaner import clean  # Silver-layer data cleaning
from src.transform.feature_engineer import engineer  # Feature engineering
from src.utils.logger import get_logger  # Centralised JSON logger
from src.utils.mlflow_tracker import end_run, log_run_data, start_run  # v2.0: MLflow run tracking
from src.utils.pipeline_state import get_watermark, set_watermark  # Incremental-load watermark storage

# ---------------------------------------------------------------------------
# Add the project root to sys.path so that 'from src...' imports work when
# this script is executed directly (python orchestration/pipeline.py).
# When invoked via pytest or as a module, Python's regular import machinery
# already knows the project root.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # orchestration → project root
sys.path.insert(0, str(PROJECT_ROOT))  # Prepend to sys.path

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


def _run_stage(stage_name: str, func, *args, durations: dict = None, **kwargs):
    """
    Execute a pipeline stage function, measure its elapsed time, and return
    its result.  Logs start, success, and failure events for every stage.

    Parameters
    ----------
    stage_name : str     Human-readable name used in log messages.
    func       : callable  The pipeline function to execute.
    *args      : positional arguments forwarded to func.
    durations  : dict, optional  When given, this stage's elapsed seconds is
                                  recorded at durations[stage_name] — used to
                                  feed MLflow's per-stage duration metrics.
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
    except Exception:
        elapsed = time.perf_counter() - t_start  # Measure time even on failure
        if durations is not None:
            durations[stage_name] = round(elapsed, 3)  # Record even a failed stage's duration
        logger.error(  # Log the failure with the exception detail
            f"Stage failed: {stage_name}",
            exc_info=True,  # Append full traceback to the log record
            extra={"elapsed_seconds": round(elapsed, 3)},
        )
        raise  # Re-raise so the outer run() function can halt the pipeline

    elapsed = time.perf_counter() - t_start  # Measure elapsed time on success
    if durations is not None:
        durations[stage_name] = round(elapsed, 3)  # Record this stage's duration for MLflow
    logger.info(  # Log the stage completion with timing
        f"Stage complete: {stage_name}",
        extra={"elapsed_seconds": round(elapsed, 3)},
    )

    return result  # Return the stage's output to the caller


# =============================================================================
# MLflow field extraction
# Translates this module's own report dict shape into the plain
# params/metrics/artifact-path arguments src/utils/mlflow_tracker.py expects.
# Kept here (not in mlflow_tracker.py) so the tracker module stays agnostic
# to any one caller's data shape.
# =============================================================================


def _mlflow_fields_from_report(report: dict) -> tuple:
    """Extract (params, metrics, artifact_paths) from the pipeline run report for MLflow logging."""
    watermark = report.get("watermark", {})  # {"since": ..., "new_watermark": ...} or {}
    params = {
        "run_mode": "full_refresh" if watermark.get("since") is None else "incremental",
        "watermark_used": watermark.get("since"),
    }

    metrics = {}
    for stage, elapsed in report.get("stage_durations", {}).items():  # Per-stage timings from _run_stage
        metrics[f"{stage}_seconds"] = elapsed
    if "elapsed_seconds" in report:
        metrics["total_seconds"] = report["elapsed_seconds"]

    extract_meta = report.get("stages", {}).get("extract", {})
    if "row_count" in extract_meta:
        metrics["raw_row_count"] = extract_meta["row_count"]

    clean_meta = report.get("stages", {}).get("clean", {})
    if "rows_output" in clean_meta:
        metrics["cleaned_row_count"] = clean_meta["rows_output"]

    for qc in report.get("quality_checks", []):  # [{"stage": "raw"/"cleaned", "passed":..., ...}, ...]
        stage = qc.get("stage")
        if stage:
            total = qc.get("total_checks", 0)
            failed = qc.get("failed_checks", 0)
            metrics[f"quality_{stage}_passed"] = total - failed
            metrics[f"quality_{stage}_total"] = total

    drift = report.get("drift", {})
    if "findings_count" in drift:
        metrics["drift_findings_count"] = drift["findings_count"]

    artifact_paths = []
    log_path = PROJECT_ROOT / "logs" / "pipeline.log"
    if log_path.exists():
        artifact_paths.append(str(log_path))
    profile_path = report.get("stages", {}).get("profiling", {}).get("report_path")
    if profile_path:
        artifact_paths.append(profile_path)

    return params, metrics, artifact_paths


# =============================================================================
# run  –  Main pipeline entry point
# =============================================================================


def run(full_refresh: bool = False) -> dict:
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

    Parameters
    ----------
    full_refresh : bool, optional
        When True, ignores any stored watermark and reloads the entire
        bronze CSV, replacing fact_sales instead of inserting into it —
        reproduces the pipeline's pre-incremental-load behavior exactly.
        Defaults to False (incremental).

    Returns
    -------
    dict
        Pipeline run report with stage timings, row counts, quality results,
        and the MLflow run ID (or None if tracking was unavailable this run).
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

    # Resolve the DuckDB path and this run's incremental settings.
    db_path = PROJECT_ROOT / config["paths"]["database"]
    buffer_days = config.get("pipeline", {}).get("incremental_buffer_days", 3)
    watermark = None if full_refresh else get_watermark(db_path)

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
        "stage_durations": {},  # Per-stage elapsed seconds, populated by _run_stage — feeds MLflow metrics
        "quality_checks": [],  # Quality check results
        "overall_status": "running",  # Updated to 'success' or 'failed' at the end
    }

    # Start an MLflow run for this execution (v2.0). Returns None if the
    # tracking server is unavailable — the pipeline proceeds identically
    # either way, since every mlflow_tracker call is a safe no-op on None.
    mlflow_run_id = start_run(
        tags={
            "environment": os.environ.get("PIPELINE_ENV", "local"),
            "project_version": config["project"]["version"],
        },
        run_name=run_ts,
    )
    report["mlflow_run_id"] = mlflow_run_id

    try:
        # -----------------------------------------------------------------------
        # Stage 1: Extract
        # Load the raw CSV into a DataFrame and capture ingestion metadata.
        # -----------------------------------------------------------------------
        raw_df, extract_meta = _run_stage(
            "extract", extract, since=watermark, buffer_days=buffer_days, durations=report["stage_durations"]
        )
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
            durations=report["stage_durations"],
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
        cleaned_df, clean_meta = _run_stage("clean", clean, raw_df, durations=report["stage_durations"])
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
            durations=report["stage_durations"],
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
        enriched_df, engineer_meta = _run_stage("engineer", engineer, cleaned_df, durations=report["stage_durations"])
        report["stages"]["engineer"] = engineer_meta  # Attach engineering metadata

        # -----------------------------------------------------------------------
        # Stage 6: Load
        # Write Parquet files (silver + gold) and populate the DuckDB database.
        # -----------------------------------------------------------------------
        load_meta = _run_stage(
            "load", load, enriched_df, full_refresh=full_refresh, durations=report["stage_durations"]
        )

        # Advance the stored watermark only now that load() has returned
        # without raising — a failed run must never advance the watermark.
        if load_meta.get("watermark_candidate"):
            set_watermark(db_path, load_meta["watermark_candidate"], run_id=run_ts)
        report["watermark"] = {"since": watermark, "new_watermark": load_meta.get("watermark_candidate")}
        report["stages"]["load"] = load_meta  # Attach loading metadata

        # Re-read the FULL accumulated silver dataset that load() just wrote —
        # on an incremental run enriched_df is only the new batch, but drift
        # detection and profiling must always compare/profile the complete
        # dataset (matching the reference snapshot built from a full run),
        # never just the partial new-rows batch.
        full_df = pd.read_parquet(load_meta["silver_path"])  # Full accumulated silver Parquet

        # -----------------------------------------------------------------------
        # Stage 7: Statistical drift detection (v1.1)
        # Compare key metrics from this run against the stored reference snapshot.
        # Emits WARNING logs for any metric that drifted beyond the threshold.
        # Never fails the pipeline — drift is surfaced as observability signal only.
        # -----------------------------------------------------------------------
        drift_threshold = config.get("pipeline", {}).get("drift_threshold", 0.05)  # Default 5 %
        drift_report = _run_stage(
            "drift_detection", detect_drift, full_df, drift_threshold, durations=report["stage_durations"]
        )
        report["drift"] = {  # Attach drift summary to the run report
            "is_first_run": drift_report.get("is_first_run", False),
            "findings_count": len(drift_report.get("drift_findings", [])),
            "findings": drift_report.get("drift_findings", []),
        }

        # -----------------------------------------------------------------------
        # Stage 8: Data profiling (v1.1)
        # Generate an HTML profiling report for the full accumulated silver
        # DataFrame. Requires requirements-profiling.txt (ydata-profiling);
        # falls back to a lightweight pandas-describe HTML report if
        # ydata-profiling is absent. Skipped entirely when generate_profile:
        # false in config.yaml.
        # -----------------------------------------------------------------------
        generate_profile_flag = config.get("pipeline", {}).get("generate_profile", True)
        if generate_profile_flag:  # Only run if profiling is enabled in config
            profile_path = _run_stage("profiling", generate_profile, full_df, durations=report["stage_durations"])
            report["stages"]["profiling"] = {  # Attach profile path to the run report
                "report_path": str(profile_path) if profile_path else None,
            }
        else:
            logger.info("Profiling skipped (generate_profile: false in config.yaml)")  # Log the skip

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
    except Exception:
        report["overall_status"] = "failed"  # Unexpected exceptions also close the MLflow run as failed
        raise
    finally:
        # Log this run's params/metrics/artifacts and close out the MLflow run
        # regardless of how the try block above exited — success, a quality-gate
        # sys.exit(1) (report["overall_status"] was already set to "failed"
        # above, before either sys.exit(1) call), or an unexpected exception.
        #
        # tags= is passed here (a deviation from a params/metrics/artifact_paths-only
        # call — see Step 3 note in task-3-report.md) so the run's final
        # overall_status is queryable as an MLflow *tag*, not just as the run's
        # own FINISHED/FAILED lifecycle state set by end_run() below.
        params, metrics, artifact_paths = _mlflow_fields_from_report(report)
        log_run_data(
            mlflow_run_id,
            params=params,
            metrics=metrics,
            artifact_paths=artifact_paths,
            tags={"overall_status": report.get("overall_status", "failed")},
        )
        end_run(mlflow_run_id, report.get("overall_status", "failed"))

    return report  # Return the full run report to the caller


# =============================================================================
# Script entry point
# Allows the pipeline to be executed directly: python orchestration/pipeline.py
# =============================================================================

if __name__ == "__main__":
    _full_refresh = "--full-refresh" in sys.argv or os.environ.get("FULL_REFRESH", "").lower() == "true"
    run(full_refresh=_full_refresh)  # Execute the pipeline when this script is run directly
