"""Tests for utility functions."""

import logging
from datetime import datetime

import pytz

from src.shared.utils import is_forex_trading_time, setup_logger, to_utc


def test_setup_logger_basic():
    """Test basic logger setup."""
    logger = setup_logger("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO


def test_setup_logger_with_file(tmp_path):
    """Test logger setup with file handler."""
    log_file = tmp_path / "test.log"
    logger = setup_logger("test_file_logger", log_file=log_file)

    assert log_file.exists()
    logger.info("Test message")

    assert log_file.read_text()


def test_setup_logger_custom_level():
    """Test logger with custom level."""
    logger = setup_logger("test_debug_logger", level=logging.DEBUG)
    assert logger.level == logging.DEBUG


def test_to_utc_with_naive_datetime():
    """Test converting naive datetime to UTC."""
    dt = datetime(2026, 2, 8, 12, 30, 45)
    utc_dt = to_utc(dt, from_tz="US/Eastern")
    assert utc_dt.tzinfo == pytz.UTC


def test_to_utc_with_aware_datetime():
    """Test converting aware datetime to UTC."""
    eastern = pytz.timezone("US/Eastern")
    dt = eastern.localize(datetime(2026, 2, 8, 12, 30, 45))
    utc_dt = to_utc(dt)
    assert utc_dt.tzinfo == pytz.UTC


def test_is_forex_trading_time_weekday():
    """Wednesday noon UTC — market open."""
    dt = datetime(2026, 2, 11, 12, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is True


def test_is_forex_trading_time_sunday_open():
    """Sunday 23:00 UTC — market open (opens at 22:00)."""
    dt = datetime(2026, 2, 8, 23, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is True


def test_is_forex_trading_time_sunday_closed():
    """Sunday 20:00 UTC — market closed (opens at 22:00)."""
    dt = datetime(2026, 2, 8, 20, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is False


def test_is_forex_trading_time_friday_before_close():
    """Friday 21:00 UTC — market still open (closes at 22:00)."""
    dt = datetime(2026, 2, 13, 21, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is True


def test_is_forex_trading_time_friday_after_close():
    """Friday 22:00 UTC — market closed."""
    dt = datetime(2026, 2, 13, 22, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is False


def test_is_forex_trading_time_saturday():
    """Saturday — always closed."""
    dt = datetime(2026, 2, 14, 12, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is False
