# =============================================================================
# src/utils/logger.py
# Centralised structured logging for the Superstore Sales Data Pipeline.
#
# Every pipeline module calls get_logger(__name__) to obtain a logger that
# writes machine-readable JSON to both stdout and a persistent log file.
# Keeping all logging configuration here means the log level, format, and
# destination can be changed in one place (config.yaml) without touching
# any pipeline code.
# =============================================================================

import json  # Serialises log records to compact JSON strings
import logging  # Python standard-library logging framework
import os  # Creates the logs/ directory when it does not yet exist
import sys  # Provides the stdout stream for the console handler
from datetime import datetime, timezone  # Generates ISO-8601 UTC timestamps
from pathlib import Path  # Cross-platform path resolution

import yaml  # Reads logging settings from config.yaml

# ---------------------------------------------------------------------------
# Project root and config path
# Resolving paths relative to this file makes the module location-agnostic;
# it works whether executed from the project root, src/, or inside Docker.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Two levels up: utils → src → project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"  # Absolute path to the YAML config


def _load_config() -> dict:
    """
    Read and parse config.yaml, returning its contents as a Python dict.
    Called once per get_logger() invocation; result is not cached because
    the function is called infrequently and we want live config reads.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:  # Open config in read-only mode
        return yaml.safe_load(fh)  # Parse YAML safely (no arbitrary code execution)


# =============================================================================
# JsonFormatter
# A custom logging.Formatter that converts every LogRecord into a single-line
# JSON object.  JSON logs are easy to ingest into Elastic, Splunk, Datadog,
# CloudWatch, or any log aggregation platform.
# =============================================================================


class JsonFormatter(logging.Formatter):
    """Emit each log record as a compact, single-line JSON string."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Build a JSON-serialisable dict from the LogRecord fields and return
        its JSON string representation.

        Parameters
        ----------
        record : logging.LogRecord
            The log record emitted by a logger call (e.g. logger.info(...)).

        Returns
        -------
        str
            A single-line JSON string terminated without a trailing newline.
        """
        # Convert the record's Unix epoch timestamp to an ISO-8601 UTC string.
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()

        # Build the core payload; every log line contains these fields.
        payload = {
            "timestamp": ts,  # ISO-8601 datetime in UTC (e.g. "2024-01-15T10:30:00+00:00")
            "level": record.levelname,  # Severity string: DEBUG / INFO / WARNING / ERROR / CRITICAL
            "logger": record.name,  # Logger name passed to get_logger() (e.g. "src.extract.extractor")
            "module": record.module,  # Python module filename without .py extension
            "function": record.funcName,  # Name of the function that called the logger
            "line": record.lineno,  # Source line number that emitted the log
            "message": record.getMessage(),  # The formatted message string (handles % args)
        }

        # If an exception was captured (e.g. logger.exception(...)), append the traceback.
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)  # Multi-line traceback string

        # Serialise to compact JSON (no indentation) with consistent key ordering.
        return json.dumps(payload, ensure_ascii=False, default=str)


# =============================================================================
# get_logger
# The single public function exposed by this module.  All pipeline modules
# call get_logger(__name__) at module level so the logger is ready before
# any pipeline function is invoked.
# =============================================================================


def get_logger(name: str) -> logging.Logger:
    """
    Create and configure a named logger with a console handler (stdout) and
    a file handler (logs/pipeline.log), both using JSON format.

    If a logger with the same name was already created in this Python process
    (e.g. because the module was imported twice), the existing logger is
    returned unchanged to avoid attaching duplicate handlers.

    Parameters
    ----------
    name : str
        Logical identifier for this logger.  Pass __name__ from the calling
        module so the logger name reflects the module path automatically.

    Returns
    -------
    logging.Logger
        A fully configured logger instance ready to emit JSON log lines.
    """
    config = _load_config()  # Read current config.yaml values

    # Extract the desired log level string; fall back to INFO if not set.
    level_str = config.get("logging", {}).get("level", "INFO").upper()

    # Convert the level string to the corresponding integer constant.
    # getattr returns logging.INFO if level_str is unrecognised.
    level = getattr(logging, level_str, logging.INFO)

    # Fetch (or create) the logger object identified by 'name'.
    logger = logging.getLogger(name)

    # Guard: if this logger already has handlers, it was configured before.
    # Return it immediately to prevent duplicate log lines.
    if logger.handlers:
        return logger

    # Set the minimum severity this logger will process.
    logger.setLevel(level)

    # -----------------------------------------------------------------------
    # Console handler
    # Writes JSON log lines to stdout so Docker / GitHub Actions can capture
    # them without reading a file.
    # -----------------------------------------------------------------------
    console_handler = logging.StreamHandler(sys.stdout)  # Attach the handler to stdout
    console_handler.setLevel(level)  # Apply the configured severity threshold
    console_handler.setFormatter(JsonFormatter())  # Format records as JSON

    # -----------------------------------------------------------------------
    # File handler
    # Appends JSON log lines to logs/pipeline.log so every pipeline run is
    # permanently recorded for auditing and debugging.
    # -----------------------------------------------------------------------
    # Resolve the log file path from config, anchored at the project root.
    log_file = PROJECT_ROOT / config.get("paths", {}).get("logs", "logs/pipeline.log")

    # Create the logs/ directory if it does not already exist.
    # exist_ok=True prevents an error when the directory is already there.
    os.makedirs(log_file.parent, exist_ok=True)

    # Open the log file in append mode so previous runs are never overwritten.
    file_handler = logging.FileHandler(str(log_file), mode="a", encoding="utf-8")
    file_handler.setLevel(level)  # Same severity threshold as console
    file_handler.setFormatter(JsonFormatter())  # Same JSON format as console

    # Register both handlers with the logger.
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent log records from bubbling up to the root logger.
    # Without this, records would be printed twice if the root logger also
    # has a handler (e.g. basicConfig was called elsewhere).
    logger.propagate = False

    return logger  # Return the fully configured logger to the caller
