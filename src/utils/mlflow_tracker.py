# src/utils/mlflow_tracker.py
# =============================================================================
# src/utils/mlflow_tracker.py
# MLflow run-tracking wrapper for the Superstore Sales Data Pipeline (v2.0).
#
# The only module in this codebase that imports mlflow directly. Every
# function here is best-effort: a missing or unreachable tracking server
# degrades to a logged WARNING, never a raised exception — observability
# tooling must never fail the pipeline, matching the existing
# src/quality/profiler.py and src/quality/drift_detector.py convention.
#
# This module knows nothing about any caller's data shape (no
# orchestration/pipeline.py report-dict logic, no Airflow DAG logic lives
# here) — callers extract their own params/metrics and pass plain dicts.
#
# Tracking server resolution: reads MLFLOW_TRACKING_URI from the environment
# via mlflow's own client. If unset, mlflow's built-in default (a local
# ./mlruns folder) applies automatically — no custom fallback code needed.
#
# Request timeout: set explicitly below rather than trusting whatever
# mlflow's own built-in default happens to be for the installed version — a
# tracking server that accepts a connection but never responds must fail
# fast, not hang the pipeline indefinitely. A raised/refused connection is
# already handled by the try/except in every function below; an unbounded
# hang would defeat the entire "observability never fails the pipeline"
# goal by stalling it instead. mlflow reads these env vars lazily, at
# request time rather than at import time, so setting them any time before
# the first actual call is sufficient — no import-order dependency.
# =============================================================================

import os  # Sets conservative HTTP request-timeout env vars before mlflow makes any request
from pathlib import Path  # Checks artifact paths exist before logging them
from typing import Optional  # Type hints for the "tracking unavailable" None case

import mlflow  # MLflow tracking client
from mlflow.tracking import MlflowClient  # Used to set a run's terminal status

from src.utils.logger import get_logger  # Centralised JSON logger

logger = get_logger(__name__)

os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "5")  # Seconds per HTTP request attempt
os.environ.setdefault(
    "MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1"
)  # Cap retries so failures can't compound into a long stall


def start_run(tags: dict, run_name: Optional[str] = None) -> Optional[str]:
    """
    Begin an MLflow run and return its run ID, or None if the tracking
    server is unavailable (logged as a WARNING, never raised).

    The run is started and immediately released (mlflow.end_run()) rather
    than kept open as the "active run" for the rest of the process — later
    calls resume logging into it by run ID via log_run_data()/end_run(),
    which is MLflow's documented pattern for a run that spans multiple,
    separate function calls (or, for the Airflow DAG, separate task
    processes) rather than one contiguous `with mlflow.start_run():` block.

    Parameters
    ----------
    tags     : dict           Tags to attach at run creation (e.g. environment, project_version).
    run_name : str, optional  Human-readable name shown in the MLflow UI's run list.

    Returns
    -------
    Optional[str]
        The new run's ID, or None if tracking is unavailable this run.
    """
    try:
        active_run = mlflow.start_run(tags=tags, run_name=run_name)  # Opens a new run
        run_id = active_run.info.run_id  # Capture the ID before releasing the active context
        mlflow.end_run(
            status="RUNNING"
        )  # Release the active-run context; leave run in RUNNING state until end_run() terminates it
        return run_id
    except Exception:
        logger.warning("MLflow start_run failed; run tracking disabled for this run", exc_info=True)
        return None


def log_run_data(
    run_id: Optional[str],
    params: Optional[dict] = None,
    metrics: Optional[dict] = None,
    artifact_paths: Optional[list] = None,
    tags: Optional[dict] = None,
) -> None:
    """
    Log params, metrics, tags, and/or artifacts to an existing MLflow run.
    No-op if run_id is None (tracking was unavailable at start_run time) or
    if logging itself fails for any reason.

    Parameters
    ----------
    run_id         : str, optional  The run ID returned by start_run(). None disables logging.
    params         : dict, optional Key/value pairs logged as MLflow params.
    metrics        : dict, optional Key/value pairs logged as MLflow metrics (numeric values).
    artifact_paths : list, optional Local file paths to upload as run artifacts. Non-existent
                                     paths are silently skipped (a stage that didn't run this
                                     time, e.g. profiling disabled in config, produces no file).
    tags           : dict, optional Key/value pairs logged as MLflow tags — e.g. a caller's
                                     final run outcome, known only after start_run() already
                                     returned, so it cannot be included in start_run()'s tags.
    """
    if run_id is None:  # Tracking was never available for this run — nothing to do
        return

    try:
        with mlflow.start_run(run_id=run_id):  # Resume logging into the existing run
            if params:
                mlflow.log_params(params)
            if metrics:
                mlflow.log_metrics(metrics)
            if tags:
                mlflow.set_tags(tags)
            for path in artifact_paths or []:
                if path and Path(path).exists():  # Skip stages that didn't produce a file this run
                    mlflow.log_artifact(path)
    except Exception:
        logger.warning("MLflow log_run_data failed; continuing without run history for this call", exc_info=True)


def end_run(run_id: Optional[str], status: str) -> None:
    """
    Close out an MLflow run with a terminal status. No-op if run_id is None.

    Before terminating, checks the run's current MLflow status and skips the
    set_terminated() call (logging at INFO, not WARNING — an expected no-op,
    not a failure) if the run is already in a terminal state (FINISHED,
    FAILED, or KILLED). This protects every caller from accidentally
    downgrading an already-successful run: e.g. in the Airflow DAG, sibling
    tasks can close the same tracked run more than once (a healthy task
    closing it as "success", followed later by a different, unrelated
    task's on_failure_callback closing it again as "failed") — without this
    guard, the later call would silently overwrite a FINISHED run to FAILED
    even though the tracked pipeline actually succeeded. Both existing
    callers (orchestration/pipeline.py's `finally` block, which calls this
    exactly once per run, and the DAG's on_failure_callback, where the race
    actually occurs) share this same defensive check.

    Parameters
    ----------
    run_id : str, optional  The run ID returned by start_run(). None disables this call.
    status : str             Either "success" or "failed" — mapped to MLflow's own
                              FINISHED/FAILED terminal run states.
    """
    if run_id is None:  # Tracking was never available for this run — nothing to close
        return

    try:
        client = MlflowClient()  # Reused for both the status check and the terminate call
        current_status = client.get_run(run_id).info.status  # Current MLflow lifecycle status
        if current_status in ("FINISHED", "FAILED", "KILLED"):  # Already terminal — do not downgrade it
            logger.info(
                "MLflow end_run skipped; run %s already terminal (%s)",
                run_id,
                current_status,
            )
            return
        mlflow_status = "FINISHED" if status == "success" else "FAILED"  # Map to MLflow's vocabulary
        client.set_terminated(run_id, status=mlflow_status)
    except Exception:
        logger.warning("MLflow end_run failed", exc_info=True)
