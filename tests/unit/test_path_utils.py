# =============================================================================
# tests/unit/test_path_utils.py
# Unit tests for dags/path_utils.py's resolve_run_dir().
#
# This deliberately imports `dags.path_utils` directly rather than
# `dags.sales_pipeline_dag` — the latter imports `airflow` at module level,
# and airflow is not an installed dependency in this repo's normal
# test/CI environment (it lives only inside the separate airflow-runtime
# Docker image). dags/path_utils.py imports only the standard library
# (re, pathlib), so it can be exercised here with no airflow package
# present at all.
#
# Imports work because pyproject.toml sets pythonpath = ["."] for pytest,
# which adds the project root to sys.path automatically, and dags/ has an
# __init__.py so `dags.path_utils` resolves as a regular package import —
# same convention already used for `from src...` imports elsewhere in this
# test suite.
# =============================================================================

import pytest  # pytest testing framework

from dags.path_utils import resolve_run_dir  # Function under test: sanitize + validate a per-run scratch dir

# =============================================================================
# Tests for resolve_run_dir
# =============================================================================


class TestResolveRunDir:
    """Tests for the run_id sanitization + containment-check logic."""

    def test_normal_run_id_resolves_to_subdirectory(self, tmp_path):
        """A plain alphanumeric run_id must resolve to a distinct subdirectory of root."""
        root = tmp_path / "airflow_tmp"  # Scratch root for this test, mirrors AIRFLOW_TMP_ROOT
        run_dir = resolve_run_dir(root, "manual_20260809")  # Resolve a normal run_id

        assert run_dir == (root / "manual_20260809").resolve(), "Should resolve to root/run_id"
        assert run_dir.is_relative_to(root.resolve()), "Resolved dir must stay under root"
        assert run_dir != root.resolve(), "Resolved dir must be a distinct subdirectory, not root itself"

    def test_colon_containing_run_id_sanitizes_and_resolves(self, tmp_path):
        """
        Airflow's real auto-generated run_id format (e.g.
        'manual__2026-08-09T10:30:00+00:00') contains colons, which are unsafe
        in some filesystem paths — it must sanitize cleanly and still resolve
        to a distinct subdirectory of root.
        """
        root = tmp_path / "airflow_tmp"  # Scratch root for this test
        run_id = "manual__2026-08-09T10:30:00+00:00"  # Real Airflow-style run_id, contains colons

        run_dir = resolve_run_dir(root, run_id)  # Should sanitize ':' to '_' and resolve successfully

        assert run_dir.is_relative_to(root.resolve()), "Sanitized dir must stay under root"
        assert run_dir != root.resolve(), "Sanitized dir must be a distinct subdirectory, not root itself"
        assert ":" not in run_dir.name, "Sanitized directory name must not contain a raw colon"

    def test_traversal_run_id_is_neutralized_and_stays_contained(self, tmp_path):
        """
        A run_id containing embedded '../' traversal segments must never
        resolve outside root. In this implementation the '/' characters are
        collapsed to '_' by sanitization *before* the path join happens, so
        a value like '../../etc/passwd' never survives as multiple path
        components — it becomes a single, safe, contained subdirectory name
        rather than raising. Verified by direct execution: this is the
        actual, correct behavior (not a gap) — the real traversal vector is
        a run_id that sanitizes down to exactly '..' with no separators at
        all, covered separately by test_bare_dotdot_run_id_raises below.
        """
        root = tmp_path / "airflow_tmp"  # Scratch root for this test
        run_dir = resolve_run_dir(root, "../../etc/passwd")  # Embedded traversal attempt

        assert run_dir.is_relative_to(root.resolve()), "Must never resolve outside root, even when it doesn't raise"
        assert run_dir != root.resolve(), "Must still be a distinct subdirectory, not root itself"
        assert "/" not in run_dir.name and "\\" not in run_dir.name, "No raw separator may survive sanitization"

    def test_bare_dotdot_run_id_raises(self, tmp_path):
        """A bare '..' run_id must raise ValueError (resolves to root's parent)."""
        root = tmp_path / "airflow_tmp"  # Scratch root for this test

        with pytest.raises(ValueError):  # '..' escapes root entirely
            resolve_run_dir(root, "..")

    def test_empty_string_run_id_raises(self, tmp_path):
        """
        An empty-string run_id must raise ValueError rather than silently
        resolving to root itself (Finding 1: cross-run collision risk).
        """
        root = tmp_path / "airflow_tmp"  # Scratch root for this test

        with pytest.raises(ValueError):  # Empty run_id has no real per-run identity
            resolve_run_dir(root, "")

    def test_dot_run_id_raises(self, tmp_path):
        """
        A '.' run_id must raise ValueError rather than silently resolving to
        root itself (Finding 1: cross-run collision risk).
        """
        root = tmp_path / "airflow_tmp"  # Scratch root for this test

        with pytest.raises(ValueError):  # '.' collapses onto root, no real per-run identity
            resolve_run_dir(root, ".")

    def test_resolved_dir_does_not_exist_until_created(self, tmp_path):
        """resolve_run_dir() must not create the directory — that's the caller's job."""
        root = tmp_path / "airflow_tmp"  # Scratch root for this test
        run_dir = resolve_run_dir(root, "some_run")  # Resolve without creating

        assert not run_dir.exists(), "resolve_run_dir() must not have side effects on the filesystem"
