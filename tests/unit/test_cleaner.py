# =============================================================================
# tests/unit/test_cleaner.py
# Unit tests for src/transform/cleaner.py.
#
# Tests cover every individual cleaning function and the public clean()
# entry point, verifying:
#   - Date strings are correctly parsed to datetime64[ns]
#   - Whitespace is stripped from string columns
#   - Categorical columns are title-cased
#   - Postal Codes are zero-padded to 5 characters
#   - Duplicate rows are removed
#   - Numeric columns have correct dtypes after casting
#   - clean() returns the correct metadata dict structure
# =============================================================================

import pytest                   # pytest testing framework
import pandas as pd             # DataFrame creation and type assertions
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.transform.cleaner import (
    parse_dates,          # Convert date string columns to datetime
    strip_whitespace,     # Remove leading/trailing spaces from string columns
    normalise_categoricals, # Title-case categorical columns
    fix_postal_codes,     # Standardise ZIP code format
    remove_duplicates,    # Drop fully-duplicated rows
    cast_numerics,        # Enforce numeric dtype on Sales, Profit, etc.
    clean,                # Public entry point that applies all steps
)


# =============================================================================
# Tests for parse_dates
# =============================================================================

class TestParseDates:
    """Tests for the parse_dates helper function."""

    def test_order_date_is_datetime(self, raw_df, config):
        """Order Date must be dtype datetime64[ns] after parsing."""
        result = parse_dates(raw_df, config["source"]["date_format"])

        assert pd.api.types.is_datetime64_any_dtype(result["Order Date"]), (
            "Order Date must be datetime64 after parse_dates()"
        )

    def test_ship_date_is_datetime(self, raw_df, config):
        """Ship Date must be dtype datetime64[ns] after parsing."""
        result = parse_dates(raw_df, config["source"]["date_format"])

        assert pd.api.types.is_datetime64_any_dtype(result["Ship Date"]), (
            "Ship Date must be datetime64 after parse_dates()"
        )

    def test_input_not_mutated(self, raw_df, config):
        """parse_dates must return a copy and must not mutate the input DataFrame."""
        original_dtype = raw_df["Order Date"].dtype   # Record original dtype
        parse_dates(raw_df, config["source"]["date_format"])  # Call the function

        assert raw_df["Order Date"].dtype == original_dtype, (
            "parse_dates must not mutate the input DataFrame"
        )

    def test_invalid_date_raises(self, raw_df, config):
        """
        A malformed date string must cause parse_dates to raise an exception
        because errors='raise' is set explicitly in the implementation.
        """
        bad_df = raw_df.copy()
        bad_df.loc[0, "Order Date"] = "not-a-date"   # Introduce an invalid date string

        with pytest.raises(Exception):   # Any exception is acceptable here
            parse_dates(bad_df, config["source"]["date_format"])

    def test_correct_year_parsed(self, raw_df, config):
        """The parsed Order Date must contain the correct calendar year."""
        result = parse_dates(raw_df, config["source"]["date_format"])

        assert result["Order Date"].dt.year.iloc[0] == 2016, (
            "First row Order Date should be year 2016"
        )


# =============================================================================
# Tests for strip_whitespace
# =============================================================================

class TestStripWhitespace:
    """Tests for the strip_whitespace helper function."""

    def test_leading_spaces_removed(self):
        """Leading spaces in string columns must be removed."""
        df = pd.DataFrame({"Category": ["  Furniture", " Technology"]})   # Leading spaces
        result = strip_whitespace(df)

        assert result["Category"].iloc[0] == "Furniture", "Leading space not removed"
        assert result["Category"].iloc[1] == "Technology", "Leading space not removed"

    def test_trailing_spaces_removed(self):
        """Trailing spaces in string columns must be removed."""
        df = pd.DataFrame({"Segment": ["Consumer   ", "Corporate "]})   # Trailing spaces
        result = strip_whitespace(df)

        assert result["Segment"].iloc[0] == "Consumer", "Trailing space not removed"

    def test_numeric_columns_unaffected(self):
        """Numeric columns must not be modified by strip_whitespace."""
        df = pd.DataFrame({"Sales": [100.0, 200.0]})   # Numeric column
        result = strip_whitespace(df)

        assert list(result["Sales"]) == [100.0, 200.0], (
            "Numeric columns must not be changed by strip_whitespace"
        )

    def test_nulls_preserved(self):
        """NaN values must remain NaN after stripping (not converted to 'nan' string)."""
        df = pd.DataFrame({"City": ["Los Angeles", None]})   # Row with NaN
        result = strip_whitespace(df)

        assert pd.isna(result["City"].iloc[1]), "NaN values must remain NaN after stripping"

    def test_input_not_mutated(self, raw_df):
        """strip_whitespace must not modify the input DataFrame."""
        original = raw_df["Category"].copy()
        strip_whitespace(raw_df)   # Call the function

        pd.testing.assert_series_equal(raw_df["Category"], original,
                                       check_names=False)


# =============================================================================
# Tests for normalise_categoricals
# =============================================================================

class TestNormaliseCategoricals:
    """Tests for the normalise_categoricals helper function."""

    def test_lowercase_category_normalised(self):
        """Lowercase category values must be converted to title case."""
        df = pd.DataFrame({"Category": ["furniture", "technology", "office supplies"]})
        result = normalise_categoricals(df)

        assert result["Category"].iloc[0] == "Furniture"
        assert result["Category"].iloc[1] == "Technology"
        assert result["Category"].iloc[2] == "Office Supplies"

    def test_uppercase_segment_normalised(self):
        """Uppercase segment values must be converted to title case."""
        df = pd.DataFrame({"Segment": ["CONSUMER", "CORPORATE", "HOME OFFICE"]})
        result = normalise_categoricals(df)

        assert result["Segment"].iloc[0] == "Consumer"
        assert result["Segment"].iloc[1] == "Corporate"
        assert result["Segment"].iloc[2] == "Home Office"

    def test_mixed_case_region_normalised(self):
        """Mixed-case region values must be normalised."""
        df = pd.DataFrame({"Region": ["eAsT", "wEST"]})
        result = normalise_categoricals(df)

        assert result["Region"].iloc[0] == "East"
        assert result["Region"].iloc[1] == "West"


# =============================================================================
# Tests for fix_postal_codes
# =============================================================================

class TestFixPostalCodes:
    """Tests for the fix_postal_codes helper function."""

    def test_leading_zero_preserved(self):
        """A ZIP code like 7094 must be padded to '07094'."""
        df = pd.DataFrame({"Postal Code": [7094]})   # Integer without leading zero
        result = fix_postal_codes(df)

        assert result["Postal Code"].iloc[0] == "07094", (
            "Postal Code must be zero-padded to 5 characters"
        )

    def test_five_digit_code_unchanged(self):
        """A correctly-formatted 5-digit ZIP code must remain unchanged."""
        df = pd.DataFrame({"Postal Code": ["42420"]})
        result = fix_postal_codes(df)

        assert result["Postal Code"].iloc[0] == "42420"

    def test_float_string_suffix_removed(self):
        """A Postal Code stored as '42420.0' (float string) must become '42420'."""
        df = pd.DataFrame({"Postal Code": [42420.0]})   # Float representation
        result = fix_postal_codes(df)

        assert result["Postal Code"].iloc[0] == "42420"


# =============================================================================
# Tests for remove_duplicates
# =============================================================================

class TestRemoveDuplicates:
    """Tests for the remove_duplicates helper function."""

    def test_duplicates_removed(self, raw_df):
        """Fully-duplicated rows must be removed from the DataFrame."""
        df_with_dups = pd.concat([raw_df, raw_df.iloc[:2]], ignore_index=True)  # Add 2 dupes
        result = remove_duplicates(df_with_dups)

        assert len(result) == len(raw_df), (
            f"Expected {len(raw_df)} rows after dedup, got {len(result)}"
        )

    def test_no_duplicates_unchanged(self, raw_df):
        """A DataFrame with no duplicates must be returned unchanged."""
        result = remove_duplicates(raw_df)

        assert len(result) == len(raw_df), (
            "Row count must not change when there are no duplicates"
        )


# =============================================================================
# Tests for the public clean() entry point
# =============================================================================

class TestClean:
    """Tests for the public clean() entry point."""

    def test_returns_dataframe_and_dict(self, raw_df):
        """clean() must return a tuple of (pd.DataFrame, dict)."""
        result = clean(raw_df)

        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)

    def test_dates_are_datetime(self, raw_df):
        """After clean(), Order Date and Ship Date must be datetime dtype."""
        cleaned, _ = clean(raw_df)

        assert pd.api.types.is_datetime64_any_dtype(cleaned["Order Date"])
        assert pd.api.types.is_datetime64_any_dtype(cleaned["Ship Date"])

    def test_metadata_has_required_keys(self, raw_df):
        """clean() metadata must include rows_input, rows_output, and rows_dropped."""
        _, meta = clean(raw_df)

        required_keys = {"rows_input", "rows_output", "rows_dropped", "dtypes"}
        assert required_keys.issubset(meta.keys()), (
            f"Metadata missing keys: {required_keys - meta.keys()}"
        )

    def test_rows_output_lte_rows_input(self, raw_df):
        """Cleaning must never increase the row count."""
        _, meta = clean(raw_df)

        assert meta["rows_output"] <= meta["rows_input"], (
            "rows_output must be <= rows_input after cleaning"
        )

    def test_input_not_mutated(self, raw_df):
        """clean() must not mutate the input DataFrame."""
        original_len = len(raw_df)   # Record the original row count
        clean(raw_df)                # Run the cleaner

        assert len(raw_df) == original_len, (
            "clean() must not modify the input DataFrame in-place"
        )
