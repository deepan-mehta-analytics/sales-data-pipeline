# =============================================================================
# dags/path_utils.py
# Airflow-independent path-sanitization helpers for the DAG's per-run scratch
# directory (dags/sales_pipeline_dag.py's _tmp_dir).
#
# This module deliberately imports only the standard library (re, pathlib) —
# never airflow — so the security-relevant sanitization/containment logic
# can be unit-tested under a normal `pytest` invocation. Airflow itself is
# not an installed dependency anywhere in this repo's requirements*.txt or
# .github/workflows/ci.yml; it lives only inside the separate
# airflow-runtime Docker image. Importable two ways depending on caller:
#   - `from dags.path_utils import resolve_run_dir`  (project root on
#     sys.path, e.g. pytest via pyproject.toml's pythonpath = ["."])
#   - `from path_utils import resolve_run_dir`        (Airflow adds the
#     dags_folder itself to sys.path, so sales_pipeline_dag.py imports its
#     sibling module this way at real DAG-parse/run time)
# =============================================================================

import re  # Sanitize run_id into a single safe path segment
from pathlib import Path  # Path resolution and containment checks


def resolve_run_dir(root: Path, run_id: str) -> Path:
    """
    Resolve the per-run scratch directory for `run_id` under `root`.

    `run_id` can be caller-supplied (Airflow's REST API accepts an optional
    dag_run_id override), so it cannot be trusted as a bare path segment.
    Collapse anything that isn't alphanumeric/dash/underscore/dot to '_' —
    this removes path separators and neutralizes '..' as a traversal token —
    then verify the resolved path both (a) stays inside `root` and (b) is a
    distinct subdirectory of `root`, not `root` itself. Without check (b), a
    run_id that sanitizes down to nothing meaningful (e.g. "", ".", or a
    run of only dots/dashes/underscores) would resolve to `root` unchanged,
    which trivially passes an is-relative-to containment check and would
    cause two concurrent DAG runs to silently share (and clobber) one
    scratch directory.

    Does not create the directory — callers create it once the path is
    known-safe. Raises ValueError if `run_id` cannot be resolved to a safe,
    distinct subdirectory of `root`.
    """
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)  # sanitize to a single safe path segment
    resolved_root = root.resolve()  # normalized absolute root for both checks below
    run_dir = (root / safe_run_id).resolve()  # normalized absolute candidate path
    if not run_dir.is_relative_to(resolved_root):  # reject anything that escapes the scratch root
        raise ValueError(f"Unsafe run_id resolves outside scratch root: {run_id!r}")
    if run_dir == resolved_root:  # reject run_ids with no real per-run identity (collapse onto root)
        raise ValueError(f"run_id does not resolve to a distinct scratch subdirectory: {run_id!r}")
    return run_dir  # Hand back the resolved, validated, run-scoped path
