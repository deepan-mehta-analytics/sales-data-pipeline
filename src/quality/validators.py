# =============================================================================
# src/quality/validators.py
# Data-quality validation layer for the Superstore Sales Data Pipeline.
#
# Validators are run on the raw DataFrame (immediately after extraction) and
# again on the cleaned DataFrame (after transformation) to catch both source
# data issues and any bugs introduced by the cleaning logic.
#
# Each validator returns a ValidationResult dataclass so the orchestrator can
# decide whether to halt the pipeline (hard failures) or continue with a
# warning (soft failures), according to config.yaml:fail_on_quality_error.
#
# Design principle: validators never modify the DataFrame — they only inspect
# it.  All fixes belong in src/transform/cleaner.py.
# =============================================================================

import pandas as pd  # DataFrame type used in all validators
import yaml  # Reads schema.yaml for expected column defs
from dataclasses import dataclass, field  # Lightweight result container
from pathlib import Path  # Cross-platform path resolution
from typing import List  # Type hints

from src.utils.logger import get_logger  # Centralised JSON logger

# Obtain a module-level logger for structured output.
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Path constants – resolved relative to this file so they work everywhere
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # extract → src → project root
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"  # Column schema definition


# =============================================================================
# ValidationResult
# A lightweight dataclass that every validator returns so callers can
# inspect failures without parsing log strings.
# =============================================================================


@dataclass
class ValidationResult:
    """Container for the outcome of a single validation check."""

    check_name: str  # Human-readable name of the check that produced this result
    passed: bool  # True when the check found no issues; False otherwise
    failures: int = 0  # Number of rows (or columns) that failed the check
    details: str = ""  # Free-text description of what failed and why


@dataclass
class QualityReport:
    """Aggregated quality report for an entire DataFrame validation pass."""

    total_checks: int = 0  # Total number of individual checks run
    passed_checks: int = 0  # Checks that found no issues
    failed_checks: int = 0  # Checks that found at least one issue
    results: List[ValidationResult] = field(default_factory=list)  # Per-check detail
    overall_passed: bool = True  # False if any check failed


# =============================================================================
# Individual validator functions
# Each function accepts a DataFrame, performs exactly one type of check,
# and returns a ValidationResult.
# =============================================================================


def check_schema(df: pd.DataFrame, schema: dict) -> ValidationResult:
    """
    Verify that all columns declared in schema.yaml are present in the DataFrame.

    Parameters
    ----------
    df     : pd.DataFrame  DataFrame to inspect.
    schema : dict          Parsed schema.yaml contents.

    Returns
    -------
    ValidationResult
        passed=True if every expected column exists; False otherwise.
    """
    expected = set(schema["columns"].keys())  # Column names declared in the schema
    actual = set(df.columns)  # Column names actually present
    missing = sorted(expected - actual)  # Columns that are expected but absent

    if missing:  # At least one column is missing — this is a hard structural failure
        return ValidationResult(
            check_name="schema_columns",
            passed=False,
            failures=len(missing),
            details=f"Missing columns: {missing}",
        )

    # All expected columns are present — schema check passes.
    return ValidationResult(check_name="schema_columns", passed=True)


def check_nulls(df: pd.DataFrame, schema: dict) -> ValidationResult:
    """
    Check that columns marked nullable=false in the schema contain no null values.

    Null values in mandatory columns (e.g. Order ID, Sales) indicate a corrupted
    source file and should halt the pipeline before bad data propagates.

    Parameters
    ----------
    df     : pd.DataFrame  DataFrame to inspect.
    schema : dict          Parsed schema.yaml contents.

    Returns
    -------
    ValidationResult
        passed=True if all non-nullable columns are fully populated.
    """
    violations = []  # Accumulate (column, null_count) pairs for the report

    for col_name, col_def in schema["columns"].items():  # Iterate over every column definition
        if col_def.get("nullable", True):  # Skip columns that are allowed to be null
            continue

        if col_name not in df.columns:  # Skip columns that are absent (caught by schema check)
            continue

        null_count = int(df[col_name].isna().sum())  # Count null / NaN values in this column

        if null_count > 0:  # This mandatory column has at least one null — record the violation
            violations.append(f"{col_name}: {null_count} nulls")

    if violations:  # One or more mandatory columns contain nulls
        return ValidationResult(
            check_name="null_check",
            passed=False,
            failures=len(violations),
            details="; ".join(violations),
        )

    # No nulls found in any mandatory column — check passes.
    return ValidationResult(check_name="null_check", passed=True)


def check_value_ranges(df: pd.DataFrame, schema: dict) -> ValidationResult:
    """
    Verify that numeric columns stay within the min/max bounds declared in the schema.

    Examples from schema.yaml:
      - Sales must be >= 0.0
      - Quantity must be >= 1
      - Discount must be between 0.0 and 1.0

    Parameters
    ----------
    df     : pd.DataFrame  DataFrame to inspect.
    schema : dict          Parsed schema.yaml contents.

    Returns
    -------
    ValidationResult
        passed=True if all columns respect their declared bounds.
    """
    violations = []  # Accumulate (column, violation_count) pairs for the report

    for col_name, col_def in schema["columns"].items():  # Iterate over every column definition
        if col_name not in df.columns:  # Skip columns that are absent
            continue

        # Check the lower bound (min_value) if one is declared.
        if "min_value" in col_def:
            min_val = col_def["min_value"]  # Declared lower bound
            below_min = int((df[col_name] < min_val).sum())  # Rows below the minimum
            if below_min > 0:
                violations.append(f"{col_name}: {below_min} rows below min {min_val}")

        # Check the upper bound (max_value) if one is declared.
        if "max_value" in col_def:
            max_val = col_def["max_value"]  # Declared upper bound
            above_max = int((df[col_name] > max_val).sum())  # Rows above the maximum
            if above_max > 0:
                violations.append(f"{col_name}: {above_max} rows above max {max_val}")

    if violations:  # At least one column has out-of-range values
        return ValidationResult(
            check_name="value_ranges",
            passed=False,
            failures=len(violations),
            details="; ".join(violations),
        )

    # All numeric columns are within their declared bounds — check passes.
    return ValidationResult(check_name="value_ranges", passed=True)


def check_allowed_values(df: pd.DataFrame, schema: dict) -> ValidationResult:
    """
    Check that categorical columns only contain the values declared in
    schema.yaml:allowed_values.

    Categories with unexpected values can break downstream aggregations
    (e.g. a typo like "Fueniture" creating a phantom Category group).

    Parameters
    ----------
    df     : pd.DataFrame  DataFrame to inspect.
    schema : dict          Parsed schema.yaml contents.

    Returns
    -------
    ValidationResult
        passed=True if all categorical columns contain only allowed values.
    """
    violations = []  # Accumulate (column, rogue_values) pairs for the report

    for col_name, col_def in schema["columns"].items():  # Iterate over every column definition
        if "allowed_values" not in col_def:  # Skip columns without a restricted value set
            continue

        if col_name not in df.columns:  # Skip absent columns
            continue

        allowed = set(col_def["allowed_values"])  # Set of permissible values
        actual_vals = set(df[col_name].dropna().unique())  # Unique values in the column
        rogue = sorted(actual_vals - allowed)  # Values not in the allowed set

        if rogue:  # At least one unexpected value is present
            violations.append(f"{col_name}: unexpected values {rogue}")

    if violations:  # One or more categorical columns contain unexpected values
        return ValidationResult(
            check_name="allowed_values",
            passed=False,
            failures=len(violations),
            details="; ".join(violations),
        )

    # All categorical columns contain only allowed values — check passes.
    return ValidationResult(check_name="allowed_values", passed=True)


def check_ship_after_order(df: pd.DataFrame) -> ValidationResult:
    """
    Business-rule check: every Ship Date must be on or after its Order Date.

    A row where Ship Date < Order Date indicates a data-entry error and would
    produce negative shipping_days values in the feature-engineering step.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame that must already have Order Date and Ship Date columns
        parsed as datetime (run this check after cleaner.py has run).

    Returns
    -------
    ValidationResult
        passed=True if no ship dates precede their corresponding order dates.
    """
    # Guard: if the columns are not datetime yet, we cannot compare them.
    if df["Order Date"].dtype == object or df["Ship Date"].dtype == object:
        return ValidationResult(
            check_name="ship_after_order",
            passed=False,
            failures=0,
            details="Date columns must be parsed to datetime before this check can run.",
        )

    # Count rows where Ship Date is strictly before Order Date.
    bad_rows = int((df["Ship Date"] < df["Order Date"]).sum())

    if bad_rows > 0:  # At least one order has a ship date before its order date
        return ValidationResult(
            check_name="ship_after_order",
            passed=False,
            failures=bad_rows,
            details=f"{bad_rows} rows where Ship Date < Order Date",
        )

    # All rows have Ship Date >= Order Date — check passes.
    return ValidationResult(check_name="ship_after_order", passed=True)


def check_duplicate_rows(df: pd.DataFrame) -> ValidationResult:
    """
    Detect fully-duplicated rows in the DataFrame.

    A fully-duplicated row means two line items are identical across every
    single column, which should not happen in a transactional dataset.

    Parameters
    ----------
    df : pd.DataFrame  DataFrame to inspect.

    Returns
    -------
    ValidationResult
        passed=True if no fully-duplicated rows are found.
    """
    duplicate_count = int(df.duplicated().sum())  # Count rows that are 100 % identical to a previous row

    if duplicate_count > 0:  # Duplicate rows found
        return ValidationResult(
            check_name="duplicate_rows",
            passed=False,
            failures=duplicate_count,
            details=f"{duplicate_count} fully-duplicated rows detected",
        )

    # No duplicates — check passes.
    return ValidationResult(check_name="duplicate_rows", passed=True)


# =============================================================================
# run_quality_checks  –  Public entry point
# Called by orchestration/pipeline.py to validate a DataFrame.
# =============================================================================


def run_quality_checks(df: pd.DataFrame, run_date_checks: bool = False) -> QualityReport:
    """
    Execute all quality checks against a DataFrame and return a consolidated
    QualityReport.

    Parameters
    ----------
    df              : pd.DataFrame
        The DataFrame to validate (raw or cleaned, depending on pipeline stage).
    run_date_checks : bool, optional
        Set to True only after cleaner.py has parsed date columns to datetime.
        The ship_after_order check requires datetime-typed date columns.

    Returns
    -------
    QualityReport
        Contains per-check ValidationResults and an overall pass/fail flag.
    """
    logger.info("Running data quality checks")  # Log the start of the validation pass

    # Load the schema so validators know the expected column definitions.
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        schema = yaml.safe_load(fh)

    report = QualityReport()  # Initialise an empty report to populate below
    checks = [  # Ordered list of checks to run on every call
        check_schema(df, schema),  # 1. All expected columns present?
        check_nulls(df, schema),  # 2. Mandatory columns free of nulls?
        check_value_ranges(df, schema),  # 3. Numeric columns within declared bounds?
        check_allowed_values(df, schema),  # 4. Categorical columns contain only allowed values?
        check_duplicate_rows(df),  # 5. No fully-duplicated rows?
    ]

    # Conditionally add the date-ordering check only after dates are parsed.
    if run_date_checks:
        checks.append(check_ship_after_order(df))  # 6. Ship Date >= Order Date?

    for result in checks:  # Tally results into the consolidated report
        report.total_checks += 1  # Increment total check counter

        if result.passed:
            report.passed_checks += 1  # Increment pass counter
            logger.info(f"Check passed: {result.check_name}")  # Log each passing check
        else:
            report.failed_checks += 1  # Increment fail counter
            report.overall_passed = False  # Mark the report as failed overall
            logger.warning(  # Log each failing check with detail
                f"Check failed: {result.check_name}",
                extra={"failures": result.failures, "details": result.details},
            )

        report.results.append(result)  # Store the full result for the pipeline report

    # Log the overall quality outcome.
    logger.info(
        "Quality check complete",
        extra={
            "total": report.total_checks,
            "passed": report.passed_checks,
            "failed": report.failed_checks,
            "overall_passed": report.overall_passed,
        },
    )

    return report  # Return the full report to the orchestrator
