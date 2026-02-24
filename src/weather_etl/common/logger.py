"""Logging utilities for structured pipeline logs."""

from __future__ import annotations

import logging


def configure_console_logging() -> None:
    """Configure console logging for the weather_etl logger."""
    logger = logging.getLogger("weather_etl")
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s: %(message)s")
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
