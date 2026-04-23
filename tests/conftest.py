# =============================================================================
# tests/conftest.py
# Shared pytest fixtures for the Superstore Sales Data Pipeline test suite.
#
# Fixtures defined here are automatically available to every test module in
# the tests/ directory without needing an explicit import.  pytest discovers
# conftest.py files by convention.
#
# Design choices:
#   - All fixtures use scope="function" (default) so each test gets a fresh
#     copy of the data and cannot inadvertently affect other tests.
#   - Sample DataFrames contain enough variety to exercise edge cases:
#       • A profitable row                (Row 1)
#       • A loss-making row               (Row 4, with heavy discount)
#       • A break-even row                (Row 7)
#       • A Home Office segment row       (Row 6)
#       • A Technology category row       (Row 8)
# =============================================================================

import pytest  # pytest test framework
import pandas as pd  # DataFrame creation for fixtures
import numpy as np  # Not used directly but available if tests need it
from pathlib import Path  # Path manipulation for tmp_path-based file fixtures
import yaml  # Used to build config fixtures

# ---------------------------------------------------------------------------
# Path constants available to tests that need to load real project files.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # tests/ → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"


# =============================================================================
# Config and schema fixtures
# =============================================================================


@pytest.fixture
def config() -> dict:
    """
    Return the parsed pipeline config.yaml as a dict.
    Tests that need config settings (date format, paths, etc.) use this fixture.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open the real config file
        return yaml.safe_load(fh)  # Parse and return as dict


@pytest.fixture
def schema() -> dict:
    """
    Return the parsed schema.yaml as a dict.
    Tests that inspect column definitions or allowed values use this fixture.
    """
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:  # Open the real schema file
        return yaml.safe_load(fh)  # Parse and return as dict


# =============================================================================
# Raw DataFrame fixture (mimics CSV after extraction, before cleaning)
# =============================================================================


@pytest.fixture
def raw_df() -> pd.DataFrame:
    """
    Return a small raw DataFrame that mirrors what extractor.extract() produces.
    Date columns are plain strings; Postal Code may have missing leading zeros.
    """
    data = {
        # String date columns — not yet parsed to datetime
        "Row ID": [1, 2, 3, 4, 5],
        "Order ID": ["CA-2016-001", "CA-2016-001", "US-2015-002", "CA-2017-003", "CA-2014-004"],
        "Order Date": ["11/8/2016", "11/8/2016", "6/12/2016", "9/17/2017", "2/27/2017"],
        "Ship Date": ["11/11/2016", "11/11/2016", "6/16/2016", "9/21/2017", "3/3/2017"],
        "Ship Mode": ["Second Class", "Second Class", "Standard Class", "First Class", "Second Class"],
        "Customer ID": ["CG-001", "CG-001", "DV-002", "AH-003", "IH-004"],
        "Customer Name": ["Alice Smith", "Alice Smith", "Bob Jones", "Carol White", "Dave Brown"],
        "Segment": ["Consumer", "Consumer", "Corporate", "Corporate", "Home Office"],
        "Postal Code": ["42420", "42420", "90036", "10024", "90049"],
        "City": ["Henderson", "Henderson", "Los Angeles", "New York City", "Los Angeles"],
        "State": ["Kentucky", "Kentucky", "California", "New York", "California"],
        "Country": ["United States", "United States", "United States", "United States", "United States"],
        "Region": ["South", "South", "West", "East", "West"],
        "Product ID": ["FUR-001", "OFF-002", "OFF-003", "OFF-004", "OFF-005"],
        "Category": ["Furniture", "Office Supplies", "Office Supplies", "Office Supplies", "Office Supplies"],
        "Sub-Category": ["Bookcases", "Labels", "Labels", "Envelopes", "Binders"],
        "Product Name": ["Bookcase A", "Label B", "Label C", "Envelope D", "Binder E"],
        "Sales": [261.96, 14.62, 22.37, 3.26, 4.98],
        "Quantity": [2, 2, 2, 2, 2],
        "Discount": [0.0, 0.0, 0.2, 0.2, 0.0],
        "Profit": [41.91, 6.87, 2.52, 1.10, 2.44],
    }
    return pd.DataFrame(data)  # Create and return the raw DataFrame


# =============================================================================
# Cleaned DataFrame fixture (mimics output of cleaner.clean())
# =============================================================================


@pytest.fixture
def cleaned_df(raw_df) -> pd.DataFrame:
    """
    Return a cleaned version of raw_df with date columns parsed to datetime
    and numeric columns in the correct dtypes.

    Uses the actual cleaner module so this fixture also serves as an
    integration check that the cleaner handles the sample data correctly.
    """
    from src.transform.cleaner import clean  # Import the actual cleaner function

    cleaned, _ = clean(raw_df)  # Run the cleaner; discard the metadata dict
    return cleaned  # Return only the cleaned DataFrame


# =============================================================================
# Enriched DataFrame fixture (mimics output of feature_engineer.engineer())
# =============================================================================


@pytest.fixture
def enriched_df(cleaned_df) -> pd.DataFrame:
    """
    Return an enriched version of cleaned_df with all derived feature columns
    added by feature_engineer.engineer().

    Depends on cleaned_df so it inherits the same data and type guarantees.
    """
    from src.transform.feature_engineer import engineer  # Import the engineer function

    enriched, _ = engineer(cleaned_df)  # Run feature engineering; discard metadata
    return enriched  # Return only the enriched DataFrame


# =============================================================================
# Invalid DataFrame fixtures
# Used by quality validator tests to verify that bad data is detected.
# =============================================================================


@pytest.fixture
def df_with_nulls(raw_df) -> pd.DataFrame:
    """
    Return raw_df with deliberate null values in mandatory columns.
    Used to verify that check_nulls() catches missing mandatory data.
    """
    df = raw_df.copy()  # Copy to avoid mutating the shared fixture
    df.loc[0, "Order ID"] = None  # Introduce a null in a mandatory column
    df.loc[1, "Sales"] = None  # Introduce a null in the Sales column
    return df


@pytest.fixture
def df_with_bad_ranges(raw_df) -> pd.DataFrame:
    """
    Return raw_df with out-of-range values in numeric columns.
    Used to verify that check_value_ranges() catches invalid numeric data.
    """
    df = raw_df.copy()
    df.loc[0, "Sales"] = -50.0  # Negative sales — not allowed per schema
    df.loc[1, "Discount"] = 1.5  # Discount > 1 (150 %) — not allowed per schema
    df.loc[2, "Quantity"] = 0  # Zero quantity — not allowed per schema
    return df


@pytest.fixture
def df_with_bad_dates(cleaned_df) -> pd.DataFrame:
    """
    Return cleaned_df with Ship Date set before Order Date for one row.
    Used to verify that check_ship_after_order() catches temporal violations.
    """
    df = cleaned_df.copy()
    # Move Ship Date three days before Order Date for the first row.
    df.loc[0, "Ship Date"] = df.loc[0, "Order Date"] - pd.Timedelta(days=3)
    return df


# =============================================================================
# Temporary file fixtures
# =============================================================================


@pytest.fixture
def tmp_csv(tmp_path, raw_df) -> Path:
    """
    Write raw_df to a temporary CSV file and return its Path.
    Used by extractor tests that need to read from a real file path.
    """
    csv_path = tmp_path / "test_sales.csv"  # Define a path inside pytest's tmp_path
    raw_df.to_csv(csv_path, index=False)  # Write the raw DataFrame to CSV
    return csv_path  # Return the path so tests can read from it


@pytest.fixture
def tmp_duckdb(tmp_path) -> Path:
    """
    Return a Path inside tmp_path for a temporary DuckDB database file.
    The file does not exist yet; it will be created by the loader when called.
    """
    return tmp_path / "test_superstore.duckdb"  # Path to a non-existent DB file
