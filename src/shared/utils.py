"""Shared utility functions for FX-AlphaLab."""

import logging
from datetime import datetime
from pathlib import Path

import pytz


def setup_logger(
    name: str, log_file: Path | None = None, level: int | str = logging.INFO
) -> logging.Logger:
    """Set up logger with console and file handlers.

    Args:
        name: Logger name
        log_file: Optional path to log file
        level: Logging level (int constant or string name like 'DEBUG', 'INFO')

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Convert string level to int if needed
    if isinstance(level, str):
        numeric_level = getattr(logging, level.upper(), logging.INFO)
        logger.setLevel(numeric_level)
    else:
        logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def to_utc(dt: datetime, from_tz: str = "US/Eastern") -> datetime:
    """Convert datetime to UTC."""
    if dt.tzinfo is None:
        dt = pytz.timezone(from_tz).localize(dt)
    return dt.astimezone(pytz.UTC)


def is_forex_trading_time(dt: datetime) -> bool:
    """Check if datetime falls within FX trading hours.

    FX market opens Sunday 22:00 UTC and closes Friday 22:00 UTC.
    Saturday is always closed.
    """
    utc_dt = to_utc(dt) if dt.tzinfo is None else dt.astimezone(pytz.UTC)
    weekday = utc_dt.weekday()
    hour = utc_dt.hour

    if weekday == 4:  # Friday
        return hour < 22
    if weekday == 5:  # Saturday
        return False
    if weekday == 6:  # Sunday
        return hour >= 22
    return True  # Mon-Thu
