# =============================================================================
# src/quality/drift_detector.py
# Statistical drift detection for the Superstore Sales Data Pipeline.
#
# After each successful pipeline run this module:
#   1. Computes a lightweight set of reference statistics from the current run.
#   2. Compares those stats against the JSON snapshot saved by the previous run.
#   3. Logs a WARNING for any metric whose relative change exceeds the configured
#      drift threshold (default 5 %).
#   4. Saves the current run's stats as the new reference for the next run.
#
# This is a dependency-free implementation — it uses only pandas and the
# Python standard library.  No MLflow, Evidently, or similar frameworks needed.
#
# Reference file: reports/run_stats_reference.json
# =============================================================================

import json  # Read/write the JSON reference-stats file
from datetime import datetime  # Timestamp each reference snapshot
from pathlib import Path  # Cross-platform path resolution
from typing import Dict, Optional  # Type hints

import pandas as pd  # DataFrame type

from src.utils.logger import get_logger  # Centralised JSON logger

# ── Module setup ──────────────────────────────────────────────────────────────
logger = get_logger(__name__)  # Named logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # quality → src → project root
REFERENCE_PATH = PROJECT_ROOT / "reports" / "run_stats_reference.json"  # Stored reference file


# =============================================================================
# Internal helpers
# =============================================================================


def _compute_stats(df: pd.DataFrame) -> Dict:
    """
    Compute a lightweight summary of key metrics from the enriched DataFrame.

    These statistics form the reference snapshot compared on the next pipeline
    run.  Chosen metrics cover the most likely failure modes: row count change,
    financial-column distribution shift, and cardinality changes.

    Parameters
    ----------
    df : pd.DataFrame  The enriched silver DataFrame from the current run.

    Returns
    -------
    dict
        Flat dictionary of {metric_name: numeric_value} pairs.
    """
    stats: Dict = {}  # Initialise the stats dict

    stats["row_count"] = int(len(df))  # Total rows — most fundamental drift signal
    stats["column_count"] = int(len(df.columns))  # Total columns — catches schema drift

    # ── Financial column statistics ───────────────────────────────────────────
    for col in ["Sales", "Profit", "Discount"]:  # Core monetary / rate columns
        if col not in df.columns:  # Skip gracefully if column is absent
            continue
        stats[f"{col}_mean"] = round(float(df[col].mean()), 4)  # Column mean (4 dp)
        stats[f"{col}_std"] = round(float(df[col].std()), 4)  # Column std dev (4 dp)
        stats[f"{col}_null_rate"] = round(float(df[col].isna().mean()), 6)  # Null fraction

    # ── Cardinality checks ────────────────────────────────────────────────────
    if "Customer ID" in df.columns:
        stats["unique_customers"] = int(df["Customer ID"].nunique())  # Distinct customer count

    if "Product ID" in df.columns:
        stats["unique_products"] = int(df["Product ID"].nunique())  # Distinct product count

    if "Region" in df.columns:
        stats["unique_regions"] = int(df["Region"].nunique())  # Should always be 4

    # ── Date range ────────────────────────────────────────────────────────────
    if "Order Date" in df.columns and hasattr(df["Order Date"], "dt"):
        stats["order_date_min"] = str(df["Order Date"].min().date())  # Earliest order date
        stats["order_date_max"] = str(df["Order Date"].max().date())  # Latest order date

    stats["computed_at"] = datetime.utcnow().isoformat()  # UTC timestamp of this snapshot

    return stats  # Return the populated dict


def _load_reference() -> Optional[Dict]:
    """
    Load the reference stats snapshot from the previous pipeline run.

    Returns
    -------
    dict or None
        Parsed reference stats, or None if no snapshot exists yet (first run).
    """
    if not REFERENCE_PATH.exists():  # No reference file found — this is the first run
        return None

    with open(REFERENCE_PATH, "r", encoding="utf-8") as fh:  # Open for reading
        return json.load(fh)  # Parse and return the JSON dict


def _save_reference(stats: Dict) -> None:
    """
    Persist the current run's stats as the reference for the next run.

    Parameters
    ----------
    stats : dict  Stats dict produced by _compute_stats().
    """
    REFERENCE_PATH.parent.mkdir(parents=True, exist_ok=True)  # Ensure reports/ exists

    with open(REFERENCE_PATH, "w", encoding="utf-8") as fh:  # Open for writing
        json.dump(stats, fh, indent=2)  # Write pretty-printed JSON

    logger.info("Reference stats saved", extra={"path": str(REFERENCE_PATH)})


# =============================================================================
# detect_drift  –  Public entry point
# =============================================================================


def detect_drift(df: pd.DataFrame, threshold: float = 0.05) -> Dict:
    """
    Compare the current run's statistics against the stored reference and emit
    WARNING logs for any metrics that drift beyond the threshold.

    Parameters
    ----------
    df        : pd.DataFrame
        The enriched silver DataFrame from the current pipeline run.
    threshold : float, optional
        Relative drift threshold as a fraction (default 0.05 = 5 %).
        A metric is flagged when |current - reference| / |reference| > threshold.

    Returns
    -------
    dict
        Drift report containing current stats, reference stats, and findings.
        Returned to the orchestrator for inclusion in the run report.
    """
    current = _compute_stats(df)  # Compute stats for this run
    reference = _load_reference()  # Load the previous run's snapshot

    report = {
        "current_stats": current,  # This run's computed stats
        "reference_stats": reference,  # Previous run's stats (None on first run)
        "drift_findings": [],  # Populated below if drift is detected
        "is_first_run": reference is None,  # True when no prior reference exists
    }

    # ── First-run path: no comparison possible ────────────────────────────────
    if reference is None:
        logger.info(
            "No reference stats found — first run. Storing current stats as reference.",
            extra={"row_count": current["row_count"]},
        )
        _save_reference(current)  # Store so the next run can compare
        return report  # Return early — nothing to compare against

    # ── Compare each numeric metric against the reference ─────────────────────
    non_numeric = {"computed_at", "order_date_min", "order_date_max"}  # Keys to skip

    for key, current_val in current.items():  # Iterate over every metric in the current stats
        if key in non_numeric:  # Skip metadata / date-string fields
            continue

        if key not in reference:  # Skip new metrics added since the last run
            continue

        ref_val = reference[key]  # Reference value for this metric

        if not isinstance(current_val, (int, float)):  # Skip non-numeric values
            continue

        if ref_val == 0:  # Avoid division by zero; a reference of 0 can't be a denominator
            continue

        relative_delta = abs(current_val - ref_val) / abs(ref_val)  # Fractional change

        if relative_delta <= threshold:  # Within acceptable bounds — no finding
            continue

        # ── Drift detected ────────────────────────────────────────────────────
        finding = {
            "metric": key,  # Metric name
            "reference_value": ref_val,  # Previous value
            "current_value": current_val,  # Current value
            "relative_delta_pct": round(relative_delta * 100, 2),  # Change as a percentage
            "threshold_pct": round(threshold * 100, 1),  # Configured threshold
        }
        report["drift_findings"].append(finding)  # Record the finding

        logger.warning(
            f"Drift detected: {key}",
            extra={
                "metric": key,
                "reference": ref_val,
                "current": current_val,
                "delta_pct": finding["relative_delta_pct"],
                "threshold_pct": finding["threshold_pct"],
            },
        )

    # ── Summary log ───────────────────────────────────────────────────────────
    if not report["drift_findings"]:
        logger.info(
            "Drift check passed — no metrics drifted beyond threshold",
            extra={"metrics_checked": len(current) - len(non_numeric), "threshold_pct": threshold * 100},
        )
    else:
        logger.warning(
            "Drift check complete — findings detected",
            extra={
                "findings_count": len(report["drift_findings"]),
                "drifted_metrics": [f["metric"] for f in report["drift_findings"]],
            },
        )

    _save_reference(current)  # Always update the reference with the current run's stats

    return report  # Return the full drift report to the orchestrator
