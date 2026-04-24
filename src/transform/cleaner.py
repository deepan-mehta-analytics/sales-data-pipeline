# =============================================================================
# src/transform/cleaner.py
# Silver-layer cleaning step for the Superstore Sales Data Pipeline.
#
# Responsibilities:
#   1. Parse Order Date and Ship Date columns from strings to datetime objects.
#   2. Strip leading/trailing whitespace from all string columns.
#   3. Normalise categorical columns to consistent title-case values.
#   4. Ensure Postal Code is a zero-padded 5-character string.
#   5. Remove any fully-duplicated rows.
#   6. Cast numeric columns to their correct dtypes.
#   7. Return the cleaned DataFrame and a cleaning metadata dict.
#
# This module does NOT add new columns — feature engineering lives in
# src/transform/feature_engineer.py.  Keeping cleaning and engineering
# separate makes it easy to unit-test each concern independently.
# =============================================================================

from pathlib import Path  # Cross-platform path resolution
from typing import Tuple  # Type hint for the (DataFrame, dict) return value

import pandas as pd  # Core data-manipulation library
import yaml  # Reads config.yaml for date format and encoding settings

from src.utils.logger import get_logger  # Centralised JSON logger

# Obtain a module-level logger.
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # transform → src → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Pipeline configuration file


def _load_config() -> dict:
    """Read and return the pipeline configuration as a dict."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open config in read-only mode
        return yaml.safe_load(fh)  # Parse YAML safely


# =============================================================================
# Individual cleaning functions
# Each function has a single responsibility and can be tested in isolation.
# =============================================================================


def parse_dates(df: pd.DataFrame, date_format: str) -> pd.DataFrame:
    """
    Convert the 'Order Date' and 'Ship Date' columns from strings to
    pandas datetime objects using the format declared in config.yaml.

    Using an explicit format string (e.g. '%m/%d/%Y') is faster and more
    predictable than pandas' automatic date inference, which can misparse
    ambiguous dates like '01/02/2016' (January 2nd vs February 1st).

    Parameters
    ----------
    df          : pd.DataFrame  The DataFrame to modify (not modified in-place).
    date_format : str           strptime format string, e.g. '%m/%d/%Y'.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with date columns as datetime64[ns].
    """
    df = df.copy()  # Work on a copy to avoid mutating the caller's DataFrame

    # Parse 'Order Date' from string to datetime using the explicit format.
    # errors='raise' ensures a bad date string throws immediately rather than
    # silently becoming NaT and corrupting downstream feature calculations.
    df["Order Date"] = pd.to_datetime(
        df["Order Date"],  # Column containing raw date strings
        format=date_format,  # Explicit format declared in config.yaml
        errors="raise",  # Raise on any unparseable date string
    )

    # Parse 'Ship Date' from string to datetime using the same format.
    df["Ship Date"] = pd.to_datetime(
        df["Ship Date"],  # Column containing raw ship date strings
        format=date_format,  # Same explicit format as Order Date
        errors="raise",  # Raise on any unparseable ship date string
    )

    logger.info(  # Log that date columns were successfully parsed
        "Date columns parsed",
        extra={"order_date_dtype": str(df["Order Date"].dtype), "ship_date_dtype": str(df["Ship Date"].dtype)},
    )

    return df  # Return the copy with parsed datetime columns


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove leading and trailing whitespace from every string (object-dtype)
    column in the DataFrame.

    Trailing spaces are a common source of phantom duplicate categories —
    for example 'Furniture ' and 'Furniture' would be treated as different
    values if not stripped before groupby or value-count operations.

    Parameters
    ----------
    df : pd.DataFrame  The DataFrame to clean.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with all string values stripped.
    """
    df = df.copy()  # Work on a copy to avoid mutating the caller's DataFrame

    # Include both 'object' and 'str' dtypes explicitly for forward-compatibility
    # with pandas 3.x, which will separate these two dtype categories.
    string_columns = df.select_dtypes(include=["object", "string"]).columns

    for col in string_columns:  # Iterate over every string column
        # Apply str.strip() to remove whitespace; na_action='ignore' leaves
        # NaN values unchanged instead of converting them to the string 'nan'.
        df[col] = df[col].str.strip()

    logger.info(  # Log how many columns were stripped
        "Whitespace stripped",
        extra={"string_columns_processed": len(string_columns)},
    )

    return df  # Return the cleaned copy


def normalise_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply title-case normalisation to categorical string columns.

    This prevents category mismatches caused by inconsistent casing in the
    source data — e.g. 'consumer', 'Consumer', and 'CONSUMER' should all
    resolve to 'Consumer'.

    The columns normalised here are the ones with an allowed_values list
    in schema.yaml: Segment, Category, Sub-Category, Region, Ship Mode.

    Parameters
    ----------
    df : pd.DataFrame  The DataFrame to normalise.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with categorical columns in title case.
    """
    df = df.copy()  # Work on a copy to avoid mutating the caller's DataFrame

    # List of columns that should always be in consistent title case.
    categorical_cols = [
        "Ship Mode",  # e.g. 'second class' → 'Second Class'
        "Segment",  # e.g. 'corporate' → 'Corporate'
        "Region",  # e.g. 'east' → 'East'
        "Category",  # e.g. 'furniture' → 'Furniture'
        "Sub-Category",  # e.g. 'bookcases' → 'Bookcases'
        "Country",  # e.g. 'united states' → 'United States'
        "State",  # e.g. 'new york' → 'New York'
    ]

    for col in categorical_cols:  # Iterate over each categorical column
        if col not in df.columns:  # Skip safely if the column is absent (caught by schema check)
            continue
        # Apply title() to convert every word's first letter to uppercase.
        # na_action='ignore' leaves NaN values unchanged.
        df[col] = df[col].str.title()

    logger.info("Categorical columns normalised to title case")  # Log completion

    return df  # Return the normalised copy


def fix_postal_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure Postal Code is a 5-character zero-padded string.

    US ZIP codes like '07094' are sometimes stored as integers (7094) when
    pandas infers the dtype, dropping the leading zero.  This function casts
    the column to string and re-adds any missing leading zeros.

    Parameters
    ----------
    df : pd.DataFrame  The DataFrame to fix.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with Postal Code formatted correctly.
    """
    df = df.copy()  # Work on a copy

    # Cast the column to string first (handles int or float representations).
    df["Postal Code"] = df["Postal Code"].astype(str)

    # Remove any '.0' suffix that arises when a float like 42420.0 is cast to string.
    df["Postal Code"] = df["Postal Code"].str.replace(r"\.0$", "", regex=True)

    # Left-pad with zeros to 5 characters; 'nan' strings are left unchanged.
    # US ZIP codes are always 5 digits; non-numeric values are kept as-is.
    df["Postal Code"] = df["Postal Code"].apply(lambda x: x.zfill(5) if x != "nan" and x.isdigit() else x)

    logger.info("Postal Code column standardised to 5-char zero-padded strings")

    return df  # Return the fixed copy


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop fully-duplicated rows from the DataFrame.

    In the Superstore dataset, each row represents a unique order line item
    identified by Row ID.  Fully-duplicated rows (identical across every
    column) indicate a data-loading error and should be removed.

    Parameters
    ----------
    df : pd.DataFrame  The DataFrame to deduplicate.

    Returns
    -------
    pd.DataFrame
        A copy with all fully-duplicated rows removed.
    """
    df = df.copy()  # Work on a copy
    before = len(df)  # Row count before deduplication
    df = df.drop_duplicates()  # Remove rows that are 100 % identical to a previous row
    removed = before - len(df)  # Number of rows that were dropped

    if removed > 0:  # Log a warning only when rows were actually removed
        logger.warning(
            "Duplicate rows removed",
            extra={"rows_removed": removed, "rows_remaining": len(df)},
        )
    else:
        logger.info("No duplicate rows found")  # Log a clean result

    return df  # Return the deduplicated copy


def cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explicitly cast numeric columns to their expected dtypes.

    Although dtype enforcement happens at CSV read time in extractor.py,
    this function acts as a safety net to catch any columns that arrive
    with an unexpected type (e.g. 'Sales' as object due to a comma separator
    in the source file).

    Parameters
    ----------
    df : pd.DataFrame  The DataFrame to cast.

    Returns
    -------
    pd.DataFrame
        A copy of the DataFrame with numeric columns correctly typed.
    """
    df = df.copy()  # Work on a copy

    # Cast Sales, Discount, and Profit to 64-bit floats.
    for col in ["Sales", "Discount", "Profit"]:
        if col in df.columns:  # Only process columns that exist
            df[col] = pd.to_numeric(df[col], errors="coerce")  # coerce turns unparseable values to NaN
            df[col] = df[col].astype("float64")  # Ensure consistent float64 dtype

    # Cast Row ID and Quantity to 64-bit integers.
    for col in ["Row ID", "Quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # coerce turns unparseable values to NaN
            df[col] = df[col].astype("Int64")  # Use nullable Int64 (capital I) to handle NaN

    logger.info("Numeric columns cast to correct dtypes")  # Log completion

    return df  # Return the re-typed copy


# =============================================================================
# clean  –  Public entry point
# Calls each cleaning function in the correct sequence.
# =============================================================================


def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Apply the full cleaning pipeline to the raw DataFrame and return a
    cleaned DataFrame ready for feature engineering.

    Cleaning sequence:
        1. strip_whitespace      – remove leading/trailing spaces
        2. normalise_categoricals – title-case categorical columns
        3. fix_postal_codes      – standardise ZIP codes
        4. parse_dates           – convert date strings to datetime
        5. remove_duplicates     – drop fully-identical rows
        6. cast_numerics         – ensure correct numeric dtypes

    Parameters
    ----------
    df : pd.DataFrame  Raw DataFrame from the extraction step.

    Returns
    -------
    cleaned_df : pd.DataFrame
        Fully cleaned DataFrame ready for feature engineering.
    metadata : dict
        Cleaning statistics for the pipeline audit report.
    """
    logger.info("Starting cleaning step", extra={"input_rows": len(df)})  # Log start with row count

    config = _load_config()  # Read the date format and other settings from config.yaml

    rows_before = len(df)  # Record input row count for the metadata report

    df = strip_whitespace(df)  # Step 1: strip whitespace from string columns
    df = normalise_categoricals(df)  # Step 2: title-case categorical columns
    df = fix_postal_codes(df)  # Step 3: standardise ZIP code format
    df = parse_dates(df, config["source"]["date_format"])  # Step 4: parse date strings
    df = remove_duplicates(df)  # Step 5: drop fully-duplicated rows
    df = cast_numerics(df)  # Step 6: ensure numeric column dtypes

    rows_after = len(df)  # Record output row count for the metadata report

    # Build a metadata dict summarising what the cleaning step did.
    metadata = {
        "rows_input": rows_before,  # Row count received from extraction
        "rows_output": rows_after,  # Row count after cleaning
        "rows_dropped": rows_before - rows_after,  # Rows removed during deduplication
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},  # Final column types
    }

    logger.info(  # Log the cleaning summary
        "Cleaning complete",
        extra={
            "rows_input": metadata["rows_input"],
            "rows_output": metadata["rows_output"],
            "rows_dropped": metadata["rows_dropped"],
        },
    )

    return df, metadata  # Return the cleaned DataFrame and the audit metadata
