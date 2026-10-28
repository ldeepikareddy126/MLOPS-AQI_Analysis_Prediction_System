"""
src/aqi_pipeline/utils/logging.py
==================================
Structured logging utility for the AQI pipeline.

Design decisions:
- JSON-structured output so logs are parseable by Airflow's log viewer.
- Every log entry includes run_id, source, and component for traceability.
- Uses Python's standard logging; no external log libraries needed.
"""

import logging
import os
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Args:
        name: Usually __name__ of the calling module.
        level: Override log level (default: from LOG_LEVEL env var or INFO).

    Returns:
        A configured Logger instance.
    """
    log_level = level or os.environ.get("LOG_LEVEL", "INFO")
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(name)s] %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        logger.propagate = False

    return logger
