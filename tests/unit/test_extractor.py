# =============================================================================
# tests/unit/test_extractor.py
# Unit tests for src/extract/extractor.py.
#
# Imports work because pyproject.toml sets pythonpath = ["."] for pytest,
# which adds the project root to sys.path automatically.
# =============================================================================

from pathlib import Path  # Path manipulation for tests

import pandas as pd  # DataFrame assertions
import pytest  # pytest testing framework

from src.extract.extractor import _build_dtype_map  # Internal helper: builds dtype mapping from schema
from src.extract.extractor import _validate_columns  # Internal helper: checks column presence
from src.extract.extractor import extract  # Public entry point being tested

# =============================================================================
# Tests for _build_dtype_map
# =============================================================================


class TestBuildDtypeMap:
    """Tests for the _build_dtype_map internal helper function."""

    def test_date_columns_are_object(self, schema):
        """
        Order Date and Ship Date must be 'object' (string) in the dtype map
        so the extractor does not attempt automatic date parsing.
        Actual date parsing is handled explicitly in cleaner.py.
        """
        dtype_map = _build_dtype_map(schema)  # Build the map from the schema fixture

        # Both date columns must be mapped to 'object' (string dtype).
        assert dtype_map["Order Date"] == "object", "Order Date should be loaded as string"
        assert dtype_map["Ship Date"] == "object", "Ship Date should be loaded as string"

    def test_postal_code_is_object(self, schema):
        """
        Postal Code must be 'object' (string) to preserve leading zeros
        (e.g. '07094' would become 7094 if loaded as int64).
        """
        dtype_map = _build_dtype_map(schema)  # Build the map

        assert dtype_map["Postal Code"] == "object", "Postal Code must be loaded as string"

    def test_sales_is_float64(self, schema):
        """Sales must be mapped to float64 as declared in schema.yaml."""
        dtype_map = _build_dtype_map(schema)

        assert dtype_map["Sales"] == "float64", "Sales should be float64"

    def test_quantity_is_int64(self, schema):
        """Quantity must be mapped to int64 as declared in schema.yaml."""
        dtype_map = _build_dtype_map(schema)

        assert dtype_map["Quantity"] == "int64", "Quantity should be int64"

    def test_all_schema_columns_present(self, schema):
        """Every column declared in schema.yaml must appear in the dtype map."""
        dtype_map = _build_dtype_map(schema)
        schema_cols = set(schema["columns"].keys())  # All declared column names
        dtype_map_cols = set(dtype_map.keys())  # All columns in the map

        assert schema_cols == dtype_map_cols, f"Dtype map is missing columns: {schema_cols - dtype_map_cols}"


# =============================================================================
# Tests for _validate_columns
# =============================================================================


class TestValidateColumns:
    """Tests for the _validate_columns internal helper function."""

    def test_valid_df_passes(self, raw_df, schema):
        """
        A DataFrame with all expected columns must pass validation without
        raising any exception.
        """
        # Should not raise; returns None on success.
        _validate_columns(raw_df, schema)  # If this raises, the test fails

    def test_missing_column_raises(self, raw_df, schema):
        """
        Dropping a required column must cause _validate_columns to raise
        a ValueError naming the missing column.
        """
        # Remove the 'Sales' column to simulate a malformed source file.
        df_missing_col = raw_df.drop(columns=["Sales"])

        with pytest.raises(ValueError, match="missing columns"):  # Expect ValueError
            _validate_columns(df_missing_col, schema)

    def test_multiple_missing_columns_raises(self, raw_df, schema):
        """ValueError must be raised even when multiple columns are absent."""
        df_missing_many = raw_df.drop(columns=["Sales", "Profit", "Quantity"])

        with pytest.raises(ValueError, match="missing columns"):
            _validate_columns(df_missing_many, schema)


# =============================================================================
# Tests for the public extract() function
# =============================================================================


class TestExtract:
    """Integration-style unit tests for the public extract() entry point."""

    def test_extract_returns_dataframe_and_metadata(self):
        """
        extract() must return a tuple of (pd.DataFrame, dict) when the
        bronze CSV exists at the configured path.
        """
        df, meta = extract()  # Call the real extract function

        assert isinstance(df, pd.DataFrame), "First return value must be a DataFrame"
        assert isinstance(meta, dict), "Second return value must be a dict"

    def test_extract_metadata_keys(self):
        """
        The metadata dict must contain the required audit keys:
        source_path, row_count, column_count, and columns.
        """
        _, meta = extract()

        required_keys = {"source_path", "row_count", "column_count", "columns"}
        assert required_keys.issubset(meta.keys()), f"Metadata is missing keys: {required_keys - meta.keys()}"

    def test_extract_row_count_positive(self):
        """The extracted DataFrame must contain at least one row."""
        df, _ = extract()

        assert len(df) > 0, "Extracted DataFrame must not be empty"

    def test_extract_column_count(self, schema):
        """
        The extracted DataFrame must have the same number of columns as
        declared in schema.yaml.
        """
        df, _ = extract()

        expected_cols = len(schema["columns"])  # Number of columns in schema
        assert len(df.columns) == expected_cols, f"Expected {expected_cols} columns, got {len(df.columns)}"

    def test_postal_code_is_string(self):
        """
        Postal Code must be loaded as object (string) dtype to prevent
        leading zeros from being silently dropped.
        """
        df, _ = extract()

        assert df["Postal Code"].dtype == object, "Postal Code must be dtype object (string), not numeric"

    def test_order_date_is_string(self):
        """
        Order Date must be loaded as object (string) dtype so that
        cleaner.py controls date parsing explicitly.
        """
        df, _ = extract()

        assert df["Order Date"].dtype == object, "Order Date must be dtype object (string) after extraction"

    def test_sales_is_numeric(self):
        """Sales must be loaded as a numeric (float64) dtype."""
        df, _ = extract()

        assert pd.api.types.is_float_dtype(df["Sales"]), "Sales must be a float dtype after extraction"

    def test_file_not_found_raises(self, monkeypatch):
        """
        extract() must raise FileNotFoundError with a helpful message when
        the bronze CSV does not exist at the configured path.
        """
        # Patch Path.exists to always return False for this test.
        monkeypatch.setattr(Path, "exists", lambda self: False)

        with pytest.raises(FileNotFoundError, match="Bronze CSV not found"):
            extract()


# =============================================================================
# Tests for watermark-based incremental filtering
# =============================================================================


class TestExtractWatermarkFiltering:
    """Tests for extract()'s optional since/buffer_days incremental filtering."""

    def test_since_none_returns_full_dataset(self):
        """Explicitly passing since=None must match calling extract() with no args."""
        df_default, _ = extract()  # Call with no arguments (the default)
        df_explicit_none, _ = extract(since=None)  # Explicitly pass since=None

        assert len(df_default) == len(df_explicit_none)  # Both must return the same number of rows

    def test_since_filters_to_fewer_rows(self):
        """A since date well within the dataset's range must return fewer rows than a full load."""
        df_full, _ = extract()  # Get all rows for comparison
        df_filtered, meta = extract(since="2017-06-01", buffer_days=0)  # Filter to 2017-06-01 onward

        assert len(df_filtered) > 0, "Expected at least some orders on/after 2017-06-01"  # Must have rows
        assert len(df_filtered) < len(df_full), "Filtered result must be smaller than the full dataset"  # Fewer rows
        assert meta["since"] == "2017-06-01"  # Metadata tracks the since value
        assert meta["rows_before_filter"] == len(df_full)  # Metadata records rows before filtering

    def test_since_far_future_returns_empty(self):
        """A since date after every real order date must return zero rows, not an error."""
        df_filtered, meta = extract(since="2099-01-01", buffer_days=0)  # Far future date

        assert len(df_filtered) == 0  # No rows after 2099
        assert meta["row_count"] == 0  # Metadata confirms zero rows

    def test_buffer_days_widens_the_window(self):
        """A larger buffer_days must never return fewer rows than a smaller one for the same since date."""
        df_no_buffer, _ = extract(since="2017-12-01", buffer_days=0)  # No buffer
        df_with_buffer, _ = extract(since="2017-12-01", buffer_days=7)  # 7-day buffer widens the window

        assert len(df_with_buffer) >= len(df_no_buffer)  # More buffer means >= rows

    def test_filtered_rows_still_have_string_order_date(self):
        """Filtering must not change Order Date's dtype — it stays a string, parsed later by cleaner.py."""
        df_filtered, _ = extract(since="2017-06-01", buffer_days=0)  # Filtered result

        assert df_filtered["Order Date"].dtype == object  # Still string/object dtype after filtering
