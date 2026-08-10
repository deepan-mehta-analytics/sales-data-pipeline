# =============================================================================
# src/load/bigquery_loader.py
# BigQuery sync step for the v2.0 Data Infrastructure phase.
#
# Reads the Gold-layer Parquet files already written by src/load/loader.py
# (this module never recomputes any aggregation) and loads them into a
# BigQuery dataset — a cloud analytical store alongside the existing local
# DuckDB store, not a replacement for it. Called only from the self-hosted
# Airflow DAG's load_bigquery task (dags/sales_pipeline_dag.py); never from
# orchestration/pipeline.py or the GitHub Actions daily cron.
#
# See docs/superpowers/specs/2026-08-09-bigquery-warehouse-design.md for the
# full design rationale (tool choice, cost pre-mortem, credential handling).
# =============================================================================

import re  # Column-name sanitization
from pathlib import Path  # Cross-platform path resolution
from typing import List, Optional  # Type hints

import pandas as pd  # DataFrame I/O
import yaml  # Reads config.yaml, same pattern as src/load/loader.py
from google.cloud import bigquery  # BigQuery client library

from src.utils.logger import get_logger  # Centralised JSON logger

logger = get_logger(__name__)  # Module-level logger

# ---------------------------------------------------------------------------
# Path constants — mirrors src/load/loader.py exactly
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # load → src → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Pipeline configuration

# Maps this pipeline's config.yaml gold-path keys to their BigQuery table
# names. Matches the DuckDB table names in src/load/loader.py exactly, so
# the same table name means the same thing in both stores.
_GOLD_TABLE_NAME_MAP = {
    "sales_by_region": "agg_sales_by_region",
    "sales_by_category": "agg_sales_by_category",
    "customer_segments": "agg_customer_segments",
    "monthly_trends": "agg_monthly_trends",
    "product_performance": "agg_product_performance",
}

# Pandas nullable extension dtypes that need special handling before a
# BigQuery load — see _coerce_nullable_dtypes below.
_NULLABLE_DTYPES = ("Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "boolean")


def _load_config() -> dict:
    """Read and return config.yaml as a Python dict."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def sanitize_column_name(name: str) -> str:
    """
    Convert a DataFrame column name into a BigQuery-safe identifier.

    BigQuery column names must match ^[A-Za-z_][A-Za-z0-9_]*$ — no spaces,
    hyphens, or other punctuation. This repo's DataFrames carry the original
    Kaggle column names verbatim (e.g. "Order ID", "Sub-Category"), which
    DuckDB and Parquet both tolerate but BigQuery load jobs reject outright
    with an "Invalid field name" error.
    """
    sanitized = re.sub(r"[^0-9a-zA-Z_]+", "_", name.strip()).strip("_").lower()
    if not sanitized:
        raise ValueError(f"Column name sanitizes to empty string: {name!r}")
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with every column renamed to a BigQuery-safe identifier."""
    renamed = df.rename(columns={col: sanitize_column_name(col) for col in df.columns})
    if renamed.columns.duplicated().any():
        dupes = renamed.columns[renamed.columns.duplicated()].tolist()
        raise ValueError(f"Column sanitization produced duplicate names: {dupes}")
    return renamed


def _coerce_nullable_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Downcast fully-populated pandas nullable-dtype columns (Int64, Float64,
    boolean) to their plain numpy equivalents before handing the DataFrame
    to the BigQuery client library.

    This pipeline's feature-engineering step produces several nullable-Int64
    columns (order_year, order_month, shipping_days, etc.) — the same dtype
    family that already caused two real np.where/np.select crashes under
    the airflow-runtime image's pinned numpy/pandas versions (Phase 6).
    Converting proactively here avoids a BigQuery-specific variant of the
    same bug class. Columns that actually contain nulls are left as nullable
    — BigQuery's schema autodetect handles those natively.
    """
    result = df.copy()
    for col in result.columns:
        dtype_name = str(result[col].dtype)
        if dtype_name in _NULLABLE_DTYPES and not result[col].isna().any():
            plain_dtype = dtype_name.lower().replace("boolean", "bool")
            result[col] = result[col].astype(plain_dtype)
    return result


def _ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str, location: str) -> None:
    """Create the target BigQuery dataset if it does not already exist (idempotent)."""
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)  # Fully-qualified dataset reference
    dataset = bigquery.Dataset(dataset_ref)  # Local Dataset object, not yet created remotely
    dataset.location = location  # Multi-region location, must be set before creation
    client.create_dataset(dataset, exists_ok=True)  # No-op if the dataset already exists
    logger.info(
        "BigQuery dataset ready",
        extra={"project_id": project_id, "dataset_id": dataset_id, "location": location},
    )


def _load_dataframe_to_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    df: pd.DataFrame,
    time_partitioning: Optional[bigquery.TimePartitioning] = None,
    clustering_fields: Optional[List[str]] = None,
) -> None:
    """Truncate-and-reload one BigQuery table from a DataFrame."""
    table_ref = f"{project_id}.{dataset_id}.{table_name}"  # Fully-qualified table reference
    safe_df = _sanitize_columns(_coerce_nullable_dtypes(df))  # BigQuery-safe names + dtypes; source df untouched

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Idempotent full reload, matches DuckDB
        autodetect=True,  # Infer schema from the DataFrame; only used on first creation of the table
    )
    if time_partitioning is not None:
        job_config.time_partitioning = time_partitioning  # Only fact_sales sets this — see sync_to_bigquery
    if clustering_fields is not None:
        job_config.clustering_fields = clustering_fields  # Only fact_sales sets this — see sync_to_bigquery

    job = client.load_table_from_dataframe(safe_df, table_ref, job_config=job_config)  # Submit the load job
    job.result()  # Block until the load job finishes; raises on failure

    logger.info("BigQuery table loaded", extra={"table": table_ref, "rows": len(safe_df)})


def sync_to_bigquery(project_id: str, dataset_id: str = "superstore_analytics", location: str = "US") -> dict:
    """
    Load the existing Gold-layer Parquet files into BigQuery.

    Assumes src/load/loader.py's load() step has already run and written
    data/silver and data/gold — this function only reads what's already on
    disk, it never recomputes any aggregation.

    Parameters
    ----------
    project_id : str   Target GCP project ID (e.g. "sales-data-pipeline-dm").
    dataset_id : str   Target BigQuery dataset name. Created if missing.
    location   : str   BigQuery dataset location (multi-region "US" is free-tier-eligible).

    Returns
    -------
    dict with keys: project_id, dataset_id, tables_loaded (list of table names).

    Raises
    ------
    ValueError
        If project_id is falsy (empty string or None). docker-compose.yml
        sets GCP_PROJECT_ID: ${GCP_PROJECT_ID:-}, so the env var is always
        *set* inside the Airflow container — just empty if the operator
        forgot to configure it on the host. Without this check, an empty
        string would silently reach bigquery.Client(project=""), which
        fails later with a confusing GCP-side error instead of a clear one
        at the point of use. This check must run before any client is
        constructed or any config/network call is made.
    """
    if not project_id:
        raise ValueError(
            "project_id is required (check that GCP_PROJECT_ID is set — "
            "docker-compose.yml defaults it to an empty string if unset on the host)"
        )

    logger.info("Starting BigQuery sync", extra={"project_id": project_id, "dataset_id": dataset_id})

    config = _load_config()  # Read paths from config/config.yaml
    client = bigquery.Client(project=project_id)  # Authenticates via GOOGLE_APPLICATION_CREDENTIALS
    _ensure_dataset(client, project_id, dataset_id, location)  # Idempotent dataset creation

    tables_loaded = []  # Accumulates every table name loaded this run

    # fact_sales — the full enriched silver DataFrame, partitioned + clustered
    # since it's the largest table and the one where these features matter.
    silver_path = PROJECT_ROOT / config["paths"]["silver"]  # Resolve silver Parquet path
    fact_df = pd.read_parquet(silver_path)  # Reload the enriched DataFrame already written by load()
    _load_dataframe_to_table(
        client,
        project_id,
        dataset_id,
        "fact_sales",
        fact_df,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=sanitize_column_name("Order Date"),
        ),
        clustering_fields=[sanitize_column_name("Region"), sanitize_column_name("Category")],
    )
    tables_loaded.append("fact_sales")

    # Five gold aggregation tables — small, no partitioning/clustering needed.
    gold_paths = config["paths"]["gold"]  # Dict of {config_key: relative_path}
    for config_key, table_name in _GOLD_TABLE_NAME_MAP.items():
        gold_path = PROJECT_ROOT / gold_paths[config_key]  # Resolve this aggregation's Parquet path
        gold_df = pd.read_parquet(gold_path)  # Reload the aggregation already written by load()
        _load_dataframe_to_table(client, project_id, dataset_id, table_name, gold_df)
        tables_loaded.append(table_name)

    metadata = {"project_id": project_id, "dataset_id": dataset_id, "tables_loaded": tables_loaded}
    logger.info("BigQuery sync complete", extra=metadata)
    return metadata
