# =============================================================================
# src/quality/profiler.py
# Optional data-profiling step for the Superstore Sales Data Pipeline.
#
# Generates a comprehensive HTML profiling report using ydata-profiling after
# each successful pipeline run.  The report surfaces distribution statistics,
# correlations, missing-value patterns, and data-quality insights in a
# shareable, self-contained HTML file.
#
# The profiler degrades gracefully: if ydata-profiling is not installed it
# falls back to a lightweight pandas-describe HTML report using no extra
# dependencies.  Set generate_profile: false in config.yaml to skip entirely.
#
# Output: reports/profile_YYYYMMDD_HHMMSS.html
# =============================================================================

from datetime import datetime  # Generates the timestamp used in the output filename
from pathlib import Path  # Cross-platform path resolution
from typing import Optional  # Type hint for a return value that may be None

import pandas as pd  # DataFrame type accepted by the profiler

from src.utils.logger import get_logger  # Centralised JSON logger

# ── Module setup ──────────────────────────────────────────────────────────────
logger = get_logger(__name__)  # Named logger for structured output

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # quality → src → project root
REPORTS_DIR = PROJECT_ROOT / "reports"  # Output directory for all reports


def _timestamp() -> str:
    """Return a compact UTC timestamp string safe for use in filenames."""
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")  # e.g. 20260509_143022


# =============================================================================
# generate_profile  –  Public entry point
# =============================================================================


def generate_profile(df: pd.DataFrame) -> Optional[Path]:
    """
    Generate a data-profiling HTML report for the enriched silver DataFrame.

    Attempts ydata-profiling first for an interactive full report.  Falls back
    to a minimal pandas-describe HTML page if ydata-profiling is not installed.

    Parameters
    ----------
    df : pd.DataFrame
        The silver-layer DataFrame to profile (output of feature_engineer).

    Returns
    -------
    Path or None
        Absolute path of the generated report, or None if an unrecoverable
        error occurs (pipeline continues either way).
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)  # Create reports/ if it does not exist

    report_path = REPORTS_DIR / f"profile_{_timestamp()}.html"  # Timestamped filename

    # ── Attempt full ydata-profiling report ───────────────────────────────────
    try:
        from ydata_profiling import ProfileReport  # Deferred import — package is optional

        logger.info(
            "Generating ydata-profiling report",
            extra={"rows": len(df), "cols": len(df.columns)},
        )

        profile = ProfileReport(
            df,  # DataFrame to analyse
            title="Superstore Sales — Data Profile",  # Title shown in the HTML header
            explorative=True,  # Enable all explorative sections
            minimal=False,  # Full report, not the minimal variant
        )

        profile.to_file(report_path)  # Write the self-contained HTML to disk

        logger.info(
            "Profiling report written",
            extra={"path": str(report_path), "size_bytes": report_path.stat().st_size},
        )

        return report_path  # Return path for the orchestrator to log

    except ImportError:
        logger.warning(
            "ydata-profiling not installed — falling back to pandas describe report; "
            "install requirements-profiling.txt for the full interactive report"
        )

    # ── Fallback: lightweight pandas describe HTML ────────────────────────────
    try:
        summary = df.describe(include="all").T  # Transpose so columns become rows

        html = (  # Build a minimal self-contained HTML page
            "<!DOCTYPE html>\n"
            "<html><head><meta charset='utf-8'>"
            "<title>Superstore Sales — Summary Statistics</title>"
            "<style>"
            "body{font-family:sans-serif;padding:2rem;}"
            "table{border-collapse:collapse;width:100%;}"
            "th,td{border:1px solid #ccc;padding:8px 12px;text-align:left;}"
            "th{background:#f4f4f4;}"
            "</style></head><body>"
            f"<h1>Superstore Sales — Summary Statistics</h1>"
            f"<p>Generated: {datetime.utcnow().isoformat()} UTC"
            f" &nbsp;|&nbsp; Rows: {len(df):,}"
            f" &nbsp;|&nbsp; Columns: {len(df.columns)}</p>"
            f"{summary.to_html()}"
            "</body></html>"
        )

        report_path.write_text(html, encoding="utf-8")  # Write fallback HTML to disk

        logger.info(
            "Fallback summary report written",
            extra={"path": str(report_path), "size_bytes": report_path.stat().st_size},
        )

        return report_path  # Return path even for the fallback report

    except Exception:
        logger.error("Profiling step failed entirely", exc_info=True)  # Log but do not re-raise
        return None  # Return None so the pipeline can continue without aborting
