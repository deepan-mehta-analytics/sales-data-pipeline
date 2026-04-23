# =============================================================================
# src/transform/feature_engineer.py
# Feature-engineering step for the Superstore Sales Data Pipeline.
#
# This module adds analytically valuable derived columns to the cleaned
# silver-layer DataFrame.  All new columns are computed from existing ones —
# no external data sources are joined here.
#
# Derived features
# ────────────────
#  Time features
#    order_year         – calendar year of the order (e.g. 2016)
#    order_month        – month number of the order (1-12)
#    order_month_name   – abbreviated month name (e.g. 'Nov')
#    order_quarter      – fiscal quarter (1-4)
#    order_day_of_week  – weekday number (Monday=0, Sunday=6)
#    shipping_days      – days elapsed between order and shipment
#
#  Financial features
#    profit_margin_pct  – profit as a percentage of sales revenue
#    discount_amount    – absolute discount value in USD
#    revenue_per_unit   – sales revenue divided by quantity
#    profit_per_unit    – net profit divided by quantity
#    is_profitable      – boolean flag: True when Profit > 0
#
#  Categorical features
#    profit_tier        – 'High' / 'Medium' / 'Low' / 'Loss' bucket
#    shipping_speed     – 'Express' (1-2 days) / 'Standard' / 'Slow'
# =============================================================================

import pandas as pd         # Core data-manipulation library
import numpy as np          # Used for np.where / np.select conditions
from pathlib import Path    # Cross-platform path resolution
from typing import Tuple    # Type hint for return value

from src.utils.logger import get_logger   # Centralised JSON logger

# Obtain a module-level logger for structured output.
logger = get_logger(__name__)


# =============================================================================
# Time-based feature functions
# =============================================================================

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract calendar and temporal features from the 'Order Date' column.

    These features are essential for time-series analysis, seasonality
    detection, and cohort-based reporting in the gold-layer aggregations.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned DataFrame with 'Order Date' already parsed to datetime.

    Returns
    -------
    pd.DataFrame
        Copy of the DataFrame with new time-based columns appended.
    """
    df = df.copy()   # Work on a copy to avoid mutating the input

    # Extract the 4-digit calendar year from Order Date (e.g. 2016).
    df["order_year"] = df["Order Date"].dt.year.astype("Int64")

    # Extract the month number (1 = January … 12 = December).
    df["order_month"] = df["Order Date"].dt.month.astype("Int64")

    # Extract the abbreviated month name for human-readable reporting (e.g. 'Jan').
    df["order_month_name"] = df["Order Date"].dt.strftime("%b")

    # Extract the calendar quarter (1 = Q1 Jan-Mar … 4 = Q4 Oct-Dec).
    df["order_quarter"] = df["Order Date"].dt.quarter.astype("Int64")

    # Extract the day of the week as an integer (0 = Monday, 6 = Sunday).
    # Useful for detecting weekday vs weekend ordering patterns.
    df["order_day_of_week"] = df["Order Date"].dt.dayofweek.astype("Int64")

    # Calculate the number of calendar days between order placement and shipment.
    # The result is a Timedelta; .dt.days extracts the integer day count.
    df["shipping_days"] = (df["Ship Date"] - df["Order Date"]).dt.days.astype("Int64")

    logger.info(   # Log the names of the new time features for the audit trail
        "Time features added",
        extra={"features": ["order_year", "order_month", "order_month_name",
                            "order_quarter", "order_day_of_week", "shipping_days"]},
    )

    return df   # Return the copy with new columns appended


# =============================================================================
# Financial feature functions
# =============================================================================

def add_financial_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute financial KPI columns derived from Sales, Profit, Quantity,
    and Discount.

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned DataFrame with numeric columns in float64 / Int64 dtype.

    Returns
    -------
    pd.DataFrame
        Copy of the DataFrame with new financial columns appended.
    """
    df = df.copy()   # Work on a copy to avoid mutating the input

    # profit_margin_pct: net profit as a percentage of revenue.
    # Formula: (Profit / Sales) * 100
    # np.where guards against division by zero when Sales == 0.
    # Rows where Sales == 0 receive a profit margin of 0.0 by convention.
    df["profit_margin_pct"] = np.where(
        df["Sales"] != 0,                      # Condition: Sales is non-zero
        (df["Profit"] / df["Sales"]) * 100,    # True branch: compute the margin
        0.0,                                   # False branch: default to 0 when Sales is zero
    ).round(2)   # Round to 2 decimal places for readability

    # discount_amount: the absolute dollar value of the discount applied.
    # Formula: Sales * Discount
    # Note: 'Sales' in this dataset is already the post-discount revenue.
    # The pre-discount price would be Sales / (1 - Discount), but the
    # absolute discount amount is still informative for margin analysis.
    df["discount_amount"] = (df["Sales"] * df["Discount"]).round(2)

    # revenue_per_unit: average revenue generated per unit sold.
    # Formula: Sales / Quantity
    # np.where guards against division by zero when Quantity == 0.
    df["revenue_per_unit"] = np.where(
        df["Quantity"] != 0,              # Condition: at least one unit sold
        df["Sales"] / df["Quantity"],     # True: divide revenue by quantity
        0.0,                              # False: default to 0 for safety
    ).round(2)

    # profit_per_unit: average net profit (or loss) per unit sold.
    # Formula: Profit / Quantity
    # A negative value means this product is sold at a loss per unit.
    df["profit_per_unit"] = np.where(
        df["Quantity"] != 0,              # Condition: at least one unit sold
        df["Profit"] / df["Quantity"],    # True: divide profit by quantity
        0.0,                              # False: default to 0 for safety
    ).round(2)

    # is_profitable: a boolean flag indicating whether this line item made money.
    # True when Profit > 0; False for break-even (Profit == 0) or losses.
    df["is_profitable"] = df["Profit"] > 0

    logger.info(   # Log the financial feature names for the audit trail
        "Financial features added",
        extra={"features": ["profit_margin_pct", "discount_amount",
                            "revenue_per_unit", "profit_per_unit", "is_profitable"]},
    )

    return df   # Return the copy with new columns appended


# =============================================================================
# Categorical feature functions
# =============================================================================

def add_categorical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bucket continuous or ordinal values into meaningful categorical labels.

    These derived categories make it easy to filter, group, and visualise
    the data without writing threshold logic in every downstream query.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame that must already have profit_margin_pct and shipping_days
        columns (i.e. run after add_time_features and add_financial_features).

    Returns
    -------
    pd.DataFrame
        Copy of the DataFrame with new categorical columns appended.
    """
    df = df.copy()   # Work on a copy

    # -----------------------------------------------------------------------
    # profit_tier: classify each line item by its profitability level.
    # Thresholds are based on common retail margin benchmarks:
    #   Loss   – negative profit margin (money-losing transaction)
    #   Low    – 0 % to 10 % margin (thin margin, often due to high discounts)
    #   Medium – 10 % to 20 % margin (average retail margin)
    #   High   – above 20 % margin (strong profitability)
    # -----------------------------------------------------------------------
    profit_conditions = [
        df["profit_margin_pct"] < 0,                                        # Loss
        (df["profit_margin_pct"] >= 0)  & (df["profit_margin_pct"] < 10),   # Low
        (df["profit_margin_pct"] >= 10) & (df["profit_margin_pct"] < 20),   # Medium
        df["profit_margin_pct"] >= 20,                                       # High
    ]
    profit_labels = ["Loss", "Low", "Medium", "High"]   # Labels matching the condition order

    # np.select applies the first matching condition's label; 'Unknown' is the default.
    df["profit_tier"] = np.select(profit_conditions, profit_labels, default="Unknown")

    # -----------------------------------------------------------------------
    # shipping_speed: classify fulfilment speed by calendar days.
    # Thresholds are approximate; Same Day = 0-1, Express = 2-3, etc.
    # -----------------------------------------------------------------------
    speed_conditions = [
        df["shipping_days"] <= 1,                                   # Same Day / Overnight
        (df["shipping_days"] >= 2) & (df["shipping_days"] <= 3),    # Express (2-3 days)
        (df["shipping_days"] >= 4) & (df["shipping_days"] <= 6),    # Standard (4-6 days)
        df["shipping_days"] >= 7,                                   # Slow (1 week or more)
    ]
    speed_labels = ["Same Day", "Express", "Standard", "Slow"]   # Labels matching conditions

    df["shipping_speed"] = np.select(speed_conditions, speed_labels, default="Unknown")

    logger.info(   # Log the categorical feature names
        "Categorical features added",
        extra={"features": ["profit_tier", "shipping_speed"]},
    )

    return df   # Return the copy with new columns appended


# =============================================================================
# engineer  –  Public entry point
# =============================================================================

def engineer(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Apply all feature-engineering transformations and return the enriched
    DataFrame alongside a metadata dict for the pipeline audit report.

    Sequence:
        1. add_time_features       – temporal KPIs from Order Date / Ship Date
        2. add_financial_features  – margin, discount, per-unit metrics
        3. add_categorical_features – bucketed profit and shipping labels

    Parameters
    ----------
    df : pd.DataFrame
        Cleaned silver-layer DataFrame (output of cleaner.clean()).

    Returns
    -------
    enriched_df : pd.DataFrame
        Silver-layer DataFrame with all derived feature columns appended.
    metadata : dict
        Feature engineering statistics for the pipeline audit report.
    """
    logger.info("Starting feature engineering step", extra={"input_rows": len(df)})

    cols_before = set(df.columns)   # Record column names before engineering starts

    df = add_time_features(df)           # Step 1: add temporal features
    df = add_financial_features(df)      # Step 2: add financial KPI features
    df = add_categorical_features(df)    # Step 3: add categorical bucket features

    cols_after   = set(df.columns)   # Record column names after all engineering
    new_features = sorted(cols_after - cols_before)   # List of newly added column names

    # Build a metadata dict summarising what was engineered.
    metadata = {
        "rows":         len(df),            # Row count (unchanged by feature engineering)
        "cols_before":  len(cols_before),   # Column count before engineering
        "cols_after":   len(cols_after),    # Column count after engineering
        "new_features": new_features,       # Names of the derived columns added
    }

    logger.info(   # Log the feature engineering summary
        "Feature engineering complete",
        extra={
            "rows":         metadata["rows"],
            "cols_added":   len(new_features),
            "new_features": new_features,
        },
    )

    return df, metadata   # Return the enriched DataFrame and audit metadata
