# =============================================================================
# tests/unit/test_validators.py
# Unit tests for src/quality/validators.py.
# =============================================================================

import pandas as pd  # DataFrame creation for tests

from src.quality.validators import QualityReport  # Aggregated report container
from src.quality.validators import check_allowed_values  # Validates categorical value sets
from src.quality.validators import check_duplicate_rows  # Detects fully-duplicated rows
from src.quality.validators import check_nulls  # Validates mandatory columns have no nulls
from src.quality.validators import check_schema  # Validates column presence
from src.quality.validators import check_ship_after_order  # Validates Ship Date >= Order Date
from src.quality.validators import check_value_ranges  # Validates numeric bounds
from src.quality.validators import run_quality_checks  # Runs all checks and returns a QualityReport


class TestCheckSchema:
    """Tests for check_schema()."""

    def test_valid_df_passes(self, raw_df, schema):
        """A DataFrame with all expected columns must pass."""
        result = check_schema(raw_df, schema)

        assert result.passed is True, "Schema check should pass for a complete DataFrame"
        assert result.failures == 0

    def test_missing_column_fails(self, raw_df, schema):
        """Dropping a required column must cause the check to fail."""
        df_missing = raw_df.drop(columns=["Sales"])

        result = check_schema(df_missing, schema)

        assert result.passed is False
        assert result.failures > 0
        assert "Sales" in result.details


class TestCheckNulls:
    """Tests for check_nulls()."""

    def test_no_nulls_passes(self, raw_df, schema):
        """A DataFrame with no nulls in mandatory columns must pass."""
        result = check_nulls(raw_df, schema)

        assert result.passed is True

    def test_null_in_mandatory_column_fails(self, df_with_nulls, schema):
        """A null in a mandatory column must cause the check to fail."""
        result = check_nulls(df_with_nulls, schema)

        assert result.passed is False
        assert result.failures > 0


class TestCheckValueRanges:
    """Tests for check_value_ranges()."""

    def test_valid_ranges_pass(self, raw_df, schema):
        """A DataFrame where all values are within declared bounds must pass."""
        result = check_value_ranges(raw_df, schema)

        assert result.passed is True

    def test_negative_sales_fails(self, df_with_bad_ranges, schema):
        """A negative Sales value must cause the range check to fail."""
        result = check_value_ranges(df_with_bad_ranges, schema)

        assert result.passed is False
        assert "Sales" in result.details

    def test_discount_above_one_fails(self, df_with_bad_ranges, schema):
        """A Discount > 1.0 must cause the range check to fail."""
        result = check_value_ranges(df_with_bad_ranges, schema)

        assert result.passed is False
        assert "Discount" in result.details


class TestCheckAllowedValues:
    """Tests for check_allowed_values()."""

    def test_valid_categories_pass(self, raw_df, schema):
        """Standard category values from the schema must all pass."""
        result = check_allowed_values(raw_df, schema)

        assert result.passed is True

    def test_unknown_category_fails(self, raw_df, schema):
        """An unrecognised Category value must cause the check to fail."""
        df_bad_cat = raw_df.copy()
        df_bad_cat.loc[0, "Category"] = "Unknown Category"  # Not in allowed_values

        result = check_allowed_values(df_bad_cat, schema)

        assert result.passed is False
        assert "Category" in result.details

    def test_unknown_region_fails(self, raw_df, schema):
        """An unrecognised Region must cause the check to fail."""
        df_bad_region = raw_df.copy()
        df_bad_region.loc[0, "Region"] = "Narnia"  # Not a real US region

        result = check_allowed_values(df_bad_region, schema)

        assert result.passed is False


class TestCheckShipAfterOrder:
    """Tests for check_ship_after_order()."""

    def test_valid_dates_pass(self, cleaned_df):
        """All rows where Ship Date >= Order Date must pass."""
        result = check_ship_after_order(cleaned_df)

        assert result.passed is True

    def test_ship_before_order_fails(self, df_with_bad_dates):
        """A row where Ship Date < Order Date must cause the check to fail."""
        result = check_ship_after_order(df_with_bad_dates)

        assert result.passed is False
        assert result.failures >= 1

    def test_string_dates_fail_with_message(self, raw_df):
        """
        Running this check on a DataFrame where dates are still strings
        (not yet parsed) must return a failed result with an explanatory message.
        """
        result = check_ship_after_order(raw_df)  # raw_df has string date columns

        assert result.passed is False
        assert "datetime" in result.details.lower()


class TestCheckDuplicateRows:
    """Tests for check_duplicate_rows()."""

    def test_no_duplicates_passes(self, raw_df):
        """A DataFrame with unique rows must pass the duplicate check."""
        result = check_duplicate_rows(raw_df)

        assert result.passed is True

    def test_duplicates_detected(self, raw_df):
        """Adding duplicate rows must cause the check to fail."""
        df_with_dups = pd.concat([raw_df, raw_df.iloc[:2]], ignore_index=True)

        result = check_duplicate_rows(df_with_dups)

        assert result.passed is False
        assert result.failures == 2  # Two rows were duplicated


class TestRunQualityChecks:
    """Tests for the public run_quality_checks() entry point."""

    def test_clean_df_passes_all_checks(self, cleaned_df):
        """A fully-cleaned DataFrame must pass every quality check."""
        report = run_quality_checks(cleaned_df, run_date_checks=True)

        assert report.overall_passed is True
        assert report.failed_checks == 0

    def test_report_has_correct_structure(self, cleaned_df):
        """The QualityReport must have the expected attributes."""
        report = run_quality_checks(cleaned_df)

        assert isinstance(report, QualityReport)
        assert hasattr(report, "total_checks")
        assert hasattr(report, "passed_checks")
        assert hasattr(report, "failed_checks")
        assert hasattr(report, "results")
        assert hasattr(report, "overall_passed")

    def test_total_equals_passed_plus_failed(self, cleaned_df):
        """total_checks must equal passed_checks + failed_checks."""
        report = run_quality_checks(cleaned_df, run_date_checks=True)

        assert report.total_checks == report.passed_checks + report.failed_checks

    def test_bad_df_sets_overall_failed(self, df_with_nulls):
        """A DataFrame with quality issues must set overall_passed=False."""
        report = run_quality_checks(df_with_nulls)

        assert report.overall_passed is False

    def test_date_check_included_when_requested(self, cleaned_df):
        """When run_date_checks=True, total_checks must be greater than without it."""
        report_without = run_quality_checks(cleaned_df, run_date_checks=False)
        report_with = run_quality_checks(cleaned_df, run_date_checks=True)

        assert (
            report_with.total_checks > report_without.total_checks
        ), "run_date_checks=True should add at least one extra check"
