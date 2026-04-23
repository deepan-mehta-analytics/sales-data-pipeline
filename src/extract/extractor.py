# =============================================================================
# src/extract/extractor.py
# Bronze-layer extraction step for the Superstore Sales Data Pipeline.
#
# Responsibilities:
#   1. Read the raw Superstore CSV from the bronze data directory.
#   2. Enforce column presence (fail fast if the file is the wrong shape).
#   3. Apply the correct dtypes from schema.yaml so downstream steps
#      receive a consistently-typed DataFrame.
#   4. Return the raw DataFrame and a metadata dict for logging / auditing.
#
# This module intentionally does NO cleaning or transformation — that is the
# responsibility of src/transform/cleaner.py.  Keeping ingestion separate
# from transformation makes the pipeline easier to debug and test.
# =============================================================================

import pandas as pd  # Core data-manipulation library
import yaml  # Reads config.yaml and schema.yaml
from pathlib import Path  # Cross-platform path handling
from typing import Tuple  # Type-hint for the (DataFrame, dict) return value

from src.utils.logger import get_logger  # Centralised JSON logger

# Obtain a logger named after this module for structured log output.
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Path constants
# Anchored at the project root so the extractor works regardless of the
# working directory from which the pipeline is launched.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Two levels up: extract → src → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Pipeline configuration file
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"  # Column schema definition file


# =============================================================================
# _load_configs
# Internal helper – loads both YAML files once so they can be reused without
# repeated disk reads inside the public extract() function.
# =============================================================================


def _load_configs() -> Tuple[dict, dict]:
    """
    Read and parse config.yaml and schema.yaml.

    Returns
    -------
    config : dict
        Pipeline configuration (paths, encoding, date format, etc.).
    schema : dict
        Column schema definition (names, dtypes, nullability, etc.).
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open the pipeline config
        config = yaml.safe_load(fh)  # Parse YAML into a dict

    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:  # Open the schema definition
        schema = yaml.safe_load(fh)  # Parse YAML into a dict

    return config, schema  # Return both dicts to the caller


def _build_dtype_map(schema: dict) -> dict:
    """
    Build a pandas dtype mapping from the schema YAML so pandas reads each
    column with the correct type on the very first CSV parse.

    Applying dtypes at read time is more efficient than casting after loading
    because pandas does not need to allocate a second copy of the column data.

    Parameters
    ----------
    schema : dict
        The parsed schema.yaml contents (top-level key: 'columns').

    Returns
    -------
    dict
        Mapping of {column_name: dtype_string} suitable for pd.read_csv's
        'dtype' parameter.
    """
    dtype_map = {}  # Initialise an empty mapping to populate in the loop below

    for col_name, col_def in schema["columns"].items():  # Iterate over every column definition
        raw_dtype = col_def.get("dtype", "object")  # Get the dtype string; default to 'object' (str)

        # Date columns are loaded as plain strings here and converted to
        # datetime objects later in cleaner.py.  This avoids pandas trying
        # (and sometimes failing) to infer date formats automatically.
        if col_name in ("Order Date", "Ship Date"):
            dtype_map[col_name] = "object"  # Force string so we control parsing ourselves

        # Postal Code must stay as a string to preserve leading zeros (e.g. "07094").
        elif col_name == "Postal Code":
            dtype_map[col_name] = "object"  # str prevents "07094" becoming 7094

        else:
            dtype_map[col_name] = raw_dtype  # Use the dtype declared in schema.yaml

    return dtype_map  # Return the completed {column: dtype} mapping


def _validate_columns(df: pd.DataFrame, schema: dict) -> None:
    """
    Check that the loaded DataFrame contains every column declared in the
    schema.  Raises ValueError immediately if any expected column is absent.

    This is a structural validation — it answers "is this the right file?"
    before any expensive processing begins.

    Parameters
    ----------
    df     : pd.DataFrame  The raw DataFrame loaded from CSV.
    schema : dict          The parsed schema.yaml contents.

    Raises
    ------
    ValueError
        If one or more expected columns are missing from the DataFrame.
    """
    expected_columns = set(schema["columns"].keys())  # All column names declared in schema.yaml
    actual_columns = set(df.columns)  # Columns actually present in the CSV

    missing = expected_columns - actual_columns  # Columns in schema but not in the file

    if missing:  # If any columns are absent, the file is not the expected dataset
        raise ValueError(f"Schema validation failed — missing columns: {sorted(missing)}")

    logger.info(  # Log success so the pipeline audit trail shows this step passed
        "Column validation passed",
        extra={"expected": len(expected_columns), "found": len(actual_columns)},
    )


# =============================================================================
# extract  –  Public entry point
# Called by orchestration/pipeline.py to perform the bronze-layer ingestion.
# =============================================================================


def extract() -> Tuple[pd.DataFrame, dict]:
    """
    Read the raw Superstore CSV, apply correct dtypes, and validate the schema.

    Steps
    -----
    1. Load config.yaml to get the bronze file path and CSV properties.
    2. Load schema.yaml to get expected column names and dtypes.
    3. Read the CSV with the correct encoding, separator, and dtype map.
    4. Validate that all expected columns are present.
    5. Return the raw DataFrame plus a metadata dict for the pipeline log.

    Returns
    -------
    df : pd.DataFrame
        Raw DataFrame containing every row and column from the CSV.
        No cleaning or transformation has been applied at this stage.
    metadata : dict
        Audit information: row count, column count, source path, etc.

    Raises
    ------
    FileNotFoundError
        If the bronze CSV does not exist at the configured path.
    ValueError
        If required columns are missing from the loaded file.
    """
    logger.info("Starting extraction step")  # Mark the beginning of this pipeline stage

    config, schema = _load_configs()  # Load both YAML config files

    # Resolve the bronze file path relative to the project root.
    bronze_path = PROJECT_ROOT / config["paths"]["bronze"]

    # Fail fast with a clear message if the file has not been placed yet.
    if not bronze_path.exists():
        raise FileNotFoundError(
            f"Bronze CSV not found at: {bronze_path}\n"
            "Download the dataset from Kaggle and place it at the path above."
        )

    logger.info("Reading CSV", extra={"path": str(bronze_path)})  # Log the file being loaded

    # Build the dtype mapping so pandas uses the right types from the start.
    dtype_map = _build_dtype_map(schema)

    # -------------------------------------------------------------------
    # Read the CSV with all required options.
    # encoding  : latin-1 handles the special characters in product names.
    # sep       : comma delimiter as specified in config.yaml.
    # dtype     : apply the schema dtypes at parse time.
    # low_memory: False forces pandas to read the whole file before
    #             inferring types, preventing mixed-type warnings.
    # -------------------------------------------------------------------
    df = pd.read_csv(
        bronze_path,  # Absolute path to the source file
        encoding=config["source"]["encoding"],  # latin-1 for Windows-1252 characters
        sep=config["source"]["separator"],  # Comma delimiter
        dtype=dtype_map,  # Pre-defined type map from schema
        low_memory=False,  # Prevent mixed-type dtype inference
    )

    # Validate that all expected columns are present before proceeding.
    _validate_columns(df, schema)

    # Build a metadata dict that the orchestrator will include in the run report.
    metadata = {
        "source_path": str(bronze_path),  # Absolute path to the source file
        "row_count": len(df),  # Total rows ingested from the CSV
        "column_count": len(df.columns),  # Number of columns in the raw file
        "columns": list(df.columns),  # Ordered list of column names
    }

    logger.info(  # Log the extraction summary for the pipeline audit trail
        "Extraction complete",
        extra={"rows": metadata["row_count"], "columns": metadata["column_count"]},
    )

    return df, metadata  # Return the raw DataFrame and the audit metadata
