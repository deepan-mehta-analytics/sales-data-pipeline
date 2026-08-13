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
        mlflow.end_run()  # Release the active-run context; resumed later by run_id
        return run_id
    except Exception:
        logger.warning("MLflow start_run failed; run tracking disabled for this run", exc_info=True)
        return None


def log_run_data(
    run_id: Optional[str],
    params: Optional[dict] = None,
    metrics: Optional[dict] = None,
    artifact_paths: Optional[list] = None,
) -> None:
    """
    Log params, metrics, and/or artifacts to an existing MLflow run. No-op
    if run_id is None (tracking was unavailable at start_run time) or if
    logging itself fails for any reason.

    Parameters
    ----------
    run_id         : str, optional  The run ID returned by start_run(). None disables logging.
    params         : dict, optional Key/value pairs logged as MLflow params.
    metrics        : dict, optional Key/value pairs logged as MLflow metrics (numeric values).
    artifact_paths : list, optional Local file paths to upload as run artifacts. Non-existent
                                     paths are silently skipped (a stage that didn't run this
                                     time, e.g. profiling disabled in config, produces no file).
    """
    if run_id is None:  # Tracking was never available for this run — nothing to do
        return

    try:
        with mlflow.start_run(run_id=run_id):  # Resume logging into the existing run
            if params:
                mlflow.log_params(params)
            if metrics:
                mlflow.log_metrics(metrics)
            for path in artifact_paths or []:
                if path and Path(path).exists():  # Skip stages that didn't produce a file this run
                    mlflow.log_artifact(path)
    except Exception:
        logger.warning("MLflow log_run_data failed; continuing without run history for this call", exc_info=True)


def end_run(run_id: Optional[str], status: str) -> None:
    """
    Close out an MLflow run with a terminal status. No-op if run_id is None.

    Parameters
    ----------
    run_id : str, optional  The run ID returned by start_run(). None disables this call.
    status : str             Either "success" or "failed" — mapped to MLflow's own
                              FINISHED/FAILED terminal run states.
    """
    if run_id is None:  # Tracking was never available for this run — nothing to close
        return

    try:
        mlflow_status = "FINISHED" if status == "success" else "FAILED"  # Map to MLflow's vocabulary
        MlflowClient().set_terminated(run_id, status=mlflow_status)
    except Exception:
        logger.warning("MLflow end_run failed", exc_info=True)
