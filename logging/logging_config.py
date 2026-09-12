"""
Standard Python logging configuration for the Healthcare MDM pipeline.

Databricks notebooks/jobs can import and call `configure_logging()` once
at the top of a run to get consistent, structured log output (job/run ID,
stage name, level) in the driver logs, in addition to the structured
pipeline log table written by src/core/logging_utils.py. This module is
deliberately independent of Spark so it can also be used by the plain-
Python real-time API layer (src/api/, api/).
"""

import logging
import sys

LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | "
    "batch=%(batch_id)s stage=%(stage)s | %(message)s"
)


class _DefaultContextFilter(logging.Filter):
    """Fill in batch_id/stage with '-' when a log call doesn't supply them,
    so LOG_FORMAT never raises a KeyError for missing fields."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "batch_id"):
            record.batch_id = "-"
        if not hasattr(record, "stage"):
            record.stage = "-"
        return True


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configure the root "hmdm" logger with a single stdout handler and the
    standard HMDM log format.

    Args:
        level: Logging level for the "hmdm" logger (default INFO).

    Returns:
        logging.Logger: the configured "hmdm" logger - call
        logger.getChild("<module_name>") from each module for a
        per-module logger that still inherits this configuration.
    """
    logger = logging.getLogger("hmdm")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        handler.addFilter(_DefaultContextFilter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger
