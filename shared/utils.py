"""Shared utility functions for FX-AlphaLab."""
import logging
from datetime import datetime
from pathlib import Path

import pytz


def setup_logger(name: str, log_file: Path | None = None, level=logging.INFO) -> logging.Logger:
    """Set up logger with console and file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

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
    """Check if datetime falls within FX trading hours (Mon-Fri)."""
    utc_dt = to_utc(dt) if dt.tzinfo is None else dt.astimezone(pytz.UTC)
    # FX market: Sunday 22:00 UTC - Friday 22:00 UTC
    weekday = utc_dt.weekday()
    hour = utc_dt.hour

    if weekday == 6:  # Sunday
        return hour >= 22
    elif weekday == 5:  # Saturday
        return hour < 22
    else:  # Mon-Fri
        return True
