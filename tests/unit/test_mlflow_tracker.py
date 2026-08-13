# tests/unit/test_mlflow_tracker.py
# =============================================================================
# tests/unit/test_mlflow_tracker.py
# Unit tests for src/utils/mlflow_tracker.py.
#
# The mlflow client is mocked throughout — no real tracking server is
# required or contacted. These tests verify: (1) the happy path calls the
# mlflow client with the correct arguments, and (2) every function degrades
# gracefully (returns None / no-ops / logs a warning) when the mlflow client
# raises, instead of ever propagating an exception.
# =============================================================================

import os
from unittest.mock import MagicMock, patch

from requests.exceptions import Timeout

import src.utils.mlflow_tracker as mlflow_tracker
from src.utils.mlflow_tracker import end_run, log_run_data, start_run


class TestRequestTimeoutSafeguard:
    """
    Verifies the module sets a conservative, explicit request timeout rather
    than relying on mlflow's own built-in default — a hanging (not just a
    raising) tracking server must not stall the pipeline indefinitely.
    """

    def test_timeout_env_vars_set_on_import(self):
        assert os.environ.get("MLFLOW_HTTP_REQUEST_TIMEOUT") == "5"
        assert os.environ.get("MLFLOW_HTTP_REQUEST_MAX_RETRIES") == "1"

    def test_start_run_returns_none_on_timeout(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.side_effect = Timeout("tracking server did not respond in time")

            run_id = start_run(tags={"environment": "local"})

            assert run_id is None

    def test_log_run_data_never_raises_on_timeout(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.side_effect = Timeout("tracking server did not respond in time")

            log_run_data("run-1", params={"a": 1})  # Must not raise, must not hang

    def test_end_run_never_raises_on_timeout(self):
        with patch.object(mlflow_tracker, "MlflowClient") as mock_client_cls:
            mock_client_cls.return_value.set_terminated.side_effect = Timeout("tracking server did not respond in time")

            end_run("run-1", status="success")  # Must not raise, must not hang


class TestStartRun:
    def test_returns_run_id_on_success(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            active_run = MagicMock()
            active_run.info.run_id = "run-abc-123"
            mock_mlflow.start_run.return_value = active_run

            run_id = start_run(tags={"environment": "local"})

            assert run_id == "run-abc-123"
            mock_mlflow.start_run.assert_called_once()
            _, kwargs = mock_mlflow.start_run.call_args
            assert kwargs["tags"] == {"environment": "local"}
            mock_mlflow.end_run.assert_called_once()  # Releases the active-run context immediately

    def test_passes_run_name_when_given(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            active_run = MagicMock()
            active_run.info.run_id = "run-xyz"
            mock_mlflow.start_run.return_value = active_run

            start_run(tags={}, run_name="my-run")

            _, kwargs = mock_mlflow.start_run.call_args
            assert kwargs["run_name"] == "my-run"

    def test_returns_none_when_mlflow_raises(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.side_effect = ConnectionError("no tracking server")

            run_id = start_run(tags={"environment": "local"})

            assert run_id is None


class TestLogRunData:
    def test_noop_when_run_id_is_none(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            log_run_data(None, params={"a": 1}, metrics={"b": 2.0})

            mock_mlflow.start_run.assert_not_called()

    def test_logs_params_and_metrics_on_success(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            ctx = MagicMock()
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=ctx)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            log_run_data("run-1", params={"run_mode": "incremental"}, metrics={"total_seconds": 1.23})

            mock_mlflow.start_run.assert_called_once_with(run_id="run-1")
            mock_mlflow.log_params.assert_called_once_with({"run_mode": "incremental"})
            mock_mlflow.log_metrics.assert_called_once_with({"total_seconds": 1.23})

    def test_skips_log_params_when_none(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.return_value.__enter__ = MagicMock()
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            log_run_data("run-1", metrics={"total_seconds": 1.0})

            mock_mlflow.log_params.assert_not_called()
            mock_mlflow.log_metrics.assert_called_once_with({"total_seconds": 1.0})

    def test_logs_only_existing_artifact_paths(self, tmp_path):
        real_file = tmp_path / "pipeline.log"
        real_file.write_text("log contents")
        missing_file = str(tmp_path / "does_not_exist.log")

        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.return_value.__enter__ = MagicMock()
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            log_run_data("run-1", artifact_paths=[str(real_file), missing_file])

            mock_mlflow.log_artifact.assert_called_once_with(str(real_file))

    def test_never_raises_when_mlflow_fails(self):
        with patch.object(mlflow_tracker, "mlflow") as mock_mlflow:
            mock_mlflow.start_run.side_effect = ConnectionError("no tracking server")

            log_run_data("run-1", params={"a": 1})  # Must not raise


class TestEndRun:
    def test_noop_when_run_id_is_none(self):
        with patch.object(mlflow_tracker, "MlflowClient") as mock_client_cls:
            end_run(None, status="success")

            mock_client_cls.assert_not_called()

    def test_success_maps_to_finished(self):
        with patch.object(mlflow_tracker, "MlflowClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value = mock_client

            end_run("run-1", status="success")

            mock_client.set_terminated.assert_called_once_with("run-1", status="FINISHED")

    def test_failed_maps_to_failed(self):
        with patch.object(mlflow_tracker, "MlflowClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value = mock_client

            end_run("run-1", status="failed")

            mock_client.set_terminated.assert_called_once_with("run-1", status="FAILED")

    def test_never_raises_when_client_fails(self):
        with patch.object(mlflow_tracker, "MlflowClient") as mock_client_cls:
            mock_client_cls.return_value.set_terminated.side_effect = ConnectionError("unreachable")

            end_run("run-1", status="success")  # Must not raise
