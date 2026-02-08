"""Tests for utility functions."""
import logging
from datetime import datetime
from pathlib import Path

import pytest
import pytz

from shared.utils import setup_logger, to_utc, is_forex_trading_time


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
    """Test FX trading time check for weekday."""
    # Wednesday at noon UTC - should be trading
    dt = datetime(2026, 2, 11, 12, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is True


def test_is_forex_trading_time_sunday_open():
    """Test FX trading time on Sunday after 22:00 UTC."""
    # Sunday at 23:00 UTC - market open
    dt = datetime(2026, 2, 8, 23, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is True


def test_is_forex_trading_time_sunday_closed():
    """Test FX trading time on Sunday before 22:00 UTC."""
    # Sunday at 20:00 UTC - market closed
    dt = datetime(2026, 2, 8, 20, 0, 0, tzinfo=pytz.UTC)
    assert is_forex_trading_time(dt) is False

