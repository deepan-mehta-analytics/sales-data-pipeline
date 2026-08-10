# =============================================================================
# tests/unit/test_bigquery_loader.py
# Unit tests for src/load/bigquery_loader.py.
#
# The google-cloud-bigquery Client is mocked throughout — no real network
# call or live GCP project is ever touched by this test suite, per the
# approved design (docs/superpowers/specs/2026-08-09-bigquery-warehouse-design.md
# §5/§7). google-cloud-bigquery is a normal test-environment dependency here
# (installed via requirements-dev.txt in this task), unlike `airflow` itself,
# because this module never imports airflow.
# =============================================================================

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from google.cloud import bigquery

from src.load.bigquery_loader import (
    _coerce_nullable_dtypes,
    _ensure_dataset,
    _load_dataframe_to_table,
    _sanitize_columns,
    sanitize_column_name,
    sync_to_bigquery,
)


class TestSanitizeColumnName:
    """Tests for converting one column name into a BigQuery-safe identifier."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Order ID", "order_id"),
            ("Sub-Category", "sub_category"),
            ("Row ID", "row_id"),
            ("profit_margin_pct", "profit_margin_pct"),
            (" Weird  Name!! ", "weird_name"),
            ("123abc", "_123abc"),
        ],
    )
    def test_known_cases(self, raw, expected):
        assert sanitize_column_name(raw) == expected

    def test_empty_after_sanitization_raises(self):
        with pytest.raises(ValueError):
            sanitize_column_name("!!!")


class TestSanitizeColumns:
    """Tests for renaming an entire DataFrame's columns."""

    def test_renames_all_columns(self):
        df = pd.DataFrame({"Order ID": [1], "Sub-Category": ["Chairs"]})
        result = _sanitize_columns(df)
        assert list(result.columns) == ["order_id", "sub_category"]

    def test_duplicate_after_sanitization_raises(self):
        df = pd.DataFrame({"Order-ID": [1], "Order ID": [2]})
        with pytest.raises(ValueError):
            _sanitize_columns(df)


class TestCoerceNullableDtypes:
    """Tests for downcasting fully-populated pandas nullable dtypes."""

    def test_fully_populated_int64_is_downcast(self):
        df = pd.DataFrame({"order_year": pd.array([2016, 2017], dtype="Int64")})
        result = _coerce_nullable_dtypes(df)
        assert str(result["order_year"].dtype) == "int64"

    def test_column_with_real_nulls_is_left_nullable(self):
        df = pd.DataFrame({"shipping_days": pd.array([1, pd.NA], dtype="Int64")})
        result = _coerce_nullable_dtypes(df)
        assert str(result["shipping_days"].dtype) == "Int64"


class TestEnsureDataset:
    """Tests for idempotent dataset creation."""

    def test_creates_dataset_with_exists_ok(self):
        client = MagicMock()
        _ensure_dataset(client, "proj", "superstore_analytics", "US")
        client.create_dataset.assert_called_once()
        called_dataset = client.create_dataset.call_args.args[0]
        assert called_dataset.location == "US"
        assert client.create_dataset.call_args.kwargs["exists_ok"] is True


class TestLoadDataframeToTable:
    """Tests for the single-table load-job wrapper."""

    def test_builds_truncate_job_and_waits_for_result(self):
        client = MagicMock()
        job = MagicMock()
        client.load_table_from_dataframe.return_value = job

        df = pd.DataFrame({"Region": ["West"], "Sales": [100.0]})
        _load_dataframe_to_table(client, "proj", "ds", "agg_sales_by_region", df)

        client.load_table_from_dataframe.assert_called_once()
        _, kwargs = client.load_table_from_dataframe.call_args
        job_config = kwargs["job_config"]
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
        job.result.assert_called_once()

    def test_passes_partitioning_and_clustering_when_given(self):
        client = MagicMock()
        client.load_table_from_dataframe.return_value = MagicMock()
        df = pd.DataFrame({"region": ["West"]})

        _load_dataframe_to_table(
            client,
            "proj",
            "ds",
            "fact_sales",
            df,
            time_partitioning=bigquery.TimePartitioning(field="order_date"),
            clustering_fields=["region", "category"],
        )

        _, kwargs = client.load_table_from_dataframe.call_args
        job_config = kwargs["job_config"]
        assert job_config.time_partitioning.field == "order_date"
        assert job_config.clustering_fields == ["region", "category"]


class TestSyncToBigquery:
    """Tests for the public orchestration entry point."""

    def test_loads_fact_and_all_five_gold_tables(self, tmp_path, monkeypatch):
        # Build a minimal on-disk layout matching config.yaml's paths.silver / paths.gold shape.
        (tmp_path / "data" / "silver").mkdir(parents=True)
        (tmp_path / "data" / "gold").mkdir(parents=True)

        silver_df = pd.DataFrame(
            {"Region": ["West"], "Category": ["Chairs"], "Order Date": pd.to_datetime(["2016-01-01"])}
        )
        silver_df.to_parquet(tmp_path / "data" / "silver" / "cleaned_sales.parquet", index=False)

        gold_tables = [
            "sales_by_region",
            "sales_by_category",
            "customer_segments",
            "monthly_trends",
            "product_performance",
        ]
        for name in gold_tables:
            pd.DataFrame({"Region": ["West"]}).to_parquet(tmp_path / "data" / "gold" / f"{name}.parquet", index=False)

        fake_config = {
            "paths": {
                "silver": "data/silver/cleaned_sales.parquet",
                "gold": {name: f"data/gold/{name}.parquet" for name in gold_tables},
            }
        }

        import src.load.bigquery_loader as bql

        monkeypatch.setattr(bql, "PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(bql, "_load_config", lambda: fake_config)

        with patch.object(bql, "bigquery") as mock_bigquery, patch.object(bql, "_load_dataframe_to_table") as mock_load:
            mock_bigquery.Client.return_value = MagicMock()

            result = sync_to_bigquery(project_id="proj", dataset_id="superstore_analytics")

            assert mock_load.call_count == 6  # fact_sales + 5 gold tables
            loaded_table_names = [call.args[3] for call in mock_load.call_args_list]
            assert "fact_sales" in loaded_table_names
            assert "agg_sales_by_region" in loaded_table_names
            assert "agg_product_performance" in loaded_table_names
            assert result == {
                "project_id": "proj",
                "dataset_id": "superstore_analytics",
                "tables_loaded": ["fact_sales"] + [f"agg_{n}" for n in gold_tables],
            }


class TestSyncToBigqueryValidation:
    """
    Tests for the project_id guard added in this task (not in the original
    plan text). docker-compose.yml sets GCP_PROJECT_ID: ${GCP_PROJECT_ID:-},
    so the env var is always *set* inside the Airflow container — just empty
    if the operator forgot to configure it on the host. Without this guard,
    an empty string would silently flow into bigquery.Client(project=""),
    producing a confusing GCP-side error far from the real cause. This guard
    must fire before any client/network call is attempted.
    """

    def test_empty_project_id_raises_before_any_client_call(self):
        import src.load.bigquery_loader as bql

        with (
            patch.object(bql, "bigquery") as mock_bigquery,
            patch.object(bql, "_load_config") as mock_load_config,
            patch.object(bql, "_ensure_dataset") as mock_ensure_dataset,
        ):
            with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
                sync_to_bigquery(project_id="")

            # No client should ever be constructed, and no downstream helper
            # should ever be reached, when validation fails up front.
            mock_bigquery.Client.assert_not_called()
            mock_load_config.assert_not_called()
            mock_ensure_dataset.assert_not_called()

    def test_none_project_id_raises_before_any_client_call(self):
        import src.load.bigquery_loader as bql

        with (
            patch.object(bql, "bigquery") as mock_bigquery,
            patch.object(bql, "_load_config") as mock_load_config,
            patch.object(bql, "_ensure_dataset") as mock_ensure_dataset,
        ):
            with pytest.raises(ValueError, match="GCP_PROJECT_ID"):
                sync_to_bigquery(project_id=None)

            mock_bigquery.Client.assert_not_called()
            mock_load_config.assert_not_called()
            mock_ensure_dataset.assert_not_called()
